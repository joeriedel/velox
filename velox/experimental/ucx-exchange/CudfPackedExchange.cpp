/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/experimental/ucx-exchange/CudfPackedExchange.h"

#include <atomic>

#include <cudf/contiguous_split.hpp>
#include "velox/experimental/cudf/CudfNoDefaults.h"
#include "velox/experimental/cudf/exec/GpuResources.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/VectorStream.h"

#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/OperatorAdapters.h"

#include "velox/common/base/Exceptions.h"
#include "velox/exec/Driver.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

namespace facebook::velox::ucx_exchange {
namespace {

constexpr const char* kAdapterLabel = "cudf-packed-exchange";

std::shared_ptr<const core::ExchangeNode> findExchangeNode(
    const exec::DriverFactory& factory,
    const core::PlanNodeId& planNodeId) {
  for (const auto& node : factory.planNodes) {
    if (node->id() == planNodeId) {
      return std::dynamic_pointer_cast<const core::ExchangeNode>(node);
    }
  }
  if (factory.consumerNode && factory.consumerNode->id() == planNodeId) {
    return std::dynamic_pointer_cast<const core::ExchangeNode>(
        factory.consumerNode);
  }
  return nullptr;
}

bool adaptDriver(const exec::DriverFactory& factory, exec::Driver& driver) {
  auto* ctx = driver.driverCtx();

  // Only for cuDF queries. Without this every exchange in the process would
  // become one that unpacks device pages, including plain CPU ones whose
  // producers send Presto-serialized bytes.
  if (!ctx->queryConfig().get<bool>(
          cudf_velox::CudfConfig::kCudfEnabled,
          cudf_velox::CudfConfig::getInstance().enabled)) {
    return false;
  }

  auto operators = driver.operators();

  // Reverse order so replaceOperators() index arithmetic does not shift the
  // operators still to be visited.
  for (int32_t i = static_cast<int32_t>(operators.size()) - 1; i >= 0; --i) {
    auto* op = operators[i];
    auto* exchange = dynamic_cast<exec::Exchange*>(op);
    if (exchange == nullptr) {
      continue;
    }
    // Only the standard exchange. Another transport's operator is already
    // whatever it needs to be.
    if (op->operatorType() != "Exchange") {
      continue;
    }

    auto exchangeNode = findExchangeNode(factory, op->planNodeId());
    if (exchangeNode == nullptr) {
      continue;
    }

    // Take the client off the operator being replaced rather than asking the
    // task for it. ~Exchange() calls close(), which closes the client, and the
    // replaced operator is destroyed as soon as this returns -- so leaving the
    // client on it would close the task's one client out from under the
    // replacement, and every exchange source created afterwards would be
    // discarded as soon as it was made. Releasing it leaves the old operator
    // with nothing to close and hands the live client to the new one.
    std::vector<std::unique_ptr<exec::Operator>> replacement;
    replacement.push_back(
        std::make_unique<CudfPackedExchange>(
            op->operatorId(),
            ctx,
            exchangeNode,
            exchange->releaseExchangeClient()));
    [[maybe_unused]] auto replaced =
        factory.replaceOperators(driver, i, i + 1, std::move(replacement));
  }

  // Always false, even after replacing something. DriverFactory stops at the
  // first adapter that returns true, and ToCudf still has to run after this
  // one to compile the rest of the driver. UcxCudfDriverAdapter does the same.
  return false;
}

/// Tells the cuDF compiler what CudfPackedExchange is, so ToCudf keeps it and
/// knows its output is already on the device.
///
/// Without this ToCudf sees an operator it does not recognise ahead of its own
/// operators and inserts a CudfFromVelox to upload the rows, which would try to
/// read a device pointer as host memory.
class CudfPackedExchangeAdapter : public cudf_velox::OperatorAdapter {
 public:
  CudfPackedExchangeAdapter() : OperatorAdapter("CudfPackedExchange") {}

  bool canHandle(const exec::Operator* op) const override {
    return op->operatorType() == "CudfPackedExchange";
  }

  bool canRunOnGPU(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/) const override {
    return true;
  }

  // A source: it has no input to accept.
  bool acceptsGpuInput() const override {
    return false;
  }

  bool producesGpuOutput() const override {
    return true;
  }

  std::vector<std::unique_ptr<exec::Operator>> createReplacements(
      const exec::Operator* /*op*/,
      const core::PlanNodePtr& /*planNode*/,
      exec::DriverCtx* /*ctx*/,
      int32_t /*operatorId*/) const override {
    return {};
  }

  bool keepOperator() const override {
    return true;
  }
};

} // namespace

CudfPackedExchange::CudfPackedExchange(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    const std::shared_ptr<const core::ExchangeNode>& exchangeNode,
    std::shared_ptr<exec::ExchangeClient> exchangeClient)
    : exec::Exchange(
          operatorId,
          driverCtx,
          exchangeNode,
          std::move(exchangeClient),
          "CudfPackedExchange") {}

namespace {

// Whether this page's bytes are on the device.
//
// The producer does not label its pages, and it should not have to: a cuDF
// query can contain a stage cuDF could not compile, whose ordinary
// PartitionedOutput sends Presto bytes to a consumer whose own plan is a cuDF
// plan. Asking what arrived is more reliable than assuming what was sent, and
// CUDA can answer it directly.
bool isPackedDevicePage(const folly::IOBuf& page) {
  if (page.countChainElements() != 2) {
    return false;
  }
  const auto* data = page.next()->data();
  cudaPointerAttributes attributes{};
  if (cudaPointerGetAttributes(&attributes, data) != cudaSuccess) {
    // Host memory CUDA has never seen. Clear the error it recorded.
    cudaGetLastError();
    return false;
  }
  return attributes.type == cudaMemoryTypeDevice;
}

} // namespace

RowVectorPtr CudfPackedExchange::getOutput() {
  if (currentPages_.empty()) {
    return nullptr;
  }

  // From the front: pages are ordered, and the producer's sequence is what
  // orders them.
  auto page = std::move(currentPages_.front());
  currentPages_.erase(currentPages_.begin());
  if (page == nullptr) {
    return nullptr;
  }

  std::unique_ptr<folly::IOBuf> buffer{page->getIOBuf()};
  VELOX_CHECK_NOT_NULL(buffer, "Packed exchange page has no buffer");

  if (!isPackedDevicePage(*buffer)) {
    return uploadHostPage(std::move(buffer));
  }

  // The chain the producer built: host metadata, then the device buffer.
  // Cloning shares the underlying buffers rather than copying them, so this is
  // the device pointer the producer packed.
  std::shared_ptr<folly::IOBuf> owner{std::move(buffer)};
  auto tableView = cudf::unpack(
      static_cast<uint8_t const*>(owner->data()),
      static_cast<uint8_t const*>(owner->next()->data()));

  const auto numRows = tableView.num_rows();
  const auto flatSize = static_cast<uint64_t>(owner->computeChainDataLength());

  // The view points into the page, so the page is what keeps it alive.
  return std::make_shared<cudf_velox::CudfVector>(
      pool(),
      outputType_,
      numRows,
      tableView,
      cudf_velox::CudfVector::ViewOwner{std::move(owner)},
      cudf_velox::cudfGlobalStreamPool().get_stream(),
      flatSize);
}

RowVectorPtr CudfPackedExchange::uploadHostPage(
    std::unique_ptr<folly::IOBuf> buffer) {
  // A Presto page from a producer that did not pack. It still has to leave
  // here as a cuDF vector: this operator told ToCudf it produces GPU output,
  // and the operators downstream were compiled against that.
  auto hostPage =
      std::make_unique<exec::PrestoSerializedPage>(std::move(buffer));
  auto stream = hostPage->prepareStreamForDeserialize();

  auto* serde =
      getNamedVectorSerde(VectorSerde::kindName(VectorSerde::Kind::kPresto));
  RowVectorPtr rows;
  while (!stream->atEnd()) {
    VectorStreamGroup::read(
        stream.get(), pool(), outputType_, serde, &rows, serdeOptions_.get());
  }
  if (rows == nullptr || rows->size() == 0) {
    return nullptr;
  }

  auto cudaStream = cudf_velox::cudfGlobalStreamPool().get_stream();
  auto table = cudf_velox::with_arrow::toCudfTable(
      rows, pool(), cudaStream, cudf_velox::get_temp_mr());
  const auto numRows = static_cast<vector_size_t>(table->num_rows());
  auto vector = std::make_shared<cudf_velox::CudfVector>(
      pool(), outputType_, numRows, std::move(table), cudaStream);
  cudaStream.synchronize();
  return vector;
}

namespace {
std::atomic<bool> gReadsDevicePages{false};
} // namespace

bool readsDevicePages() {
  return gReadsDevicePages.load(std::memory_order_acquire);
}

void registerCudfPackedExchange() {
  static std::atomic<bool> registered{false};
  bool expected = false;
  if (!registered.compare_exchange_strong(expected, true)) {
    return;
  }

  gReadsDevicePages.store(true, std::memory_order_release);

  cudf_velox::OperatorAdapterRegistry::getInstance().registerAdapter(
      std::make_unique<CudfPackedExchangeAdapter>());

  // Inserted at the front rather than appended. Adapters run in order and
  // ToCudf must see the exchange already replaced, so that its own
  // OperatorAdapter describes CudfPackedExchange rather than the plain
  // exchange it started as. Appending would leave that to the order
  // registerCudf() happened to be called in.
  exec::DriverAdapter adapter{
      std::string(kAdapterLabel), /*inspect=*/{}, &adaptDriver};
  exec::DriverFactory::adapters.insert(
      exec::DriverFactory::adapters.begin(), std::move(adapter));
}

} // namespace facebook::velox::ucx_exchange
