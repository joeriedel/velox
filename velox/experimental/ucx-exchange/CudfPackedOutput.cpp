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

#include "velox/experimental/ucx-exchange/CudfPackedOutput.h"

#include <atomic>
#include <cstdlib>

#include "velox/exec/Driver.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/ucx-exchange/UcxOutputQueueManager.h"
#include "velox/experimental/ucx-exchange/UcxPartitionedOutput.h"

namespace facebook::velox::ucx_exchange {
namespace {

constexpr const char* kAdapterLabel = "cudf-packed-output";

// Mirrors the rule in LocalPlanner: a partial limit upstream means flush early
// so the final limit is reached with as little work as possible. Duplicated
// because the original is file-local to LocalPlanner.cpp.
bool eagerFlush(const core::PlanNode& node) {
  if (const auto* limit = dynamic_cast<const core::LimitNode*>(&node)) {
    return limit->isPartial() && limit->offset() + limit->count() < 10'000;
  }
  if (node.sources().empty()) {
    return false;
  }
  return eagerFlush(*node.sources()[0]);
}

std::shared_ptr<const core::PartitionedOutputNode> findOutputNode(
    const exec::DriverFactory& factory,
    const core::PlanNodeId& planNodeId) {
  for (const auto& node : factory.planNodes) {
    if (node->id() == planNodeId) {
      return std::dynamic_pointer_cast<const core::PartitionedOutputNode>(node);
    }
  }
  if (factory.consumerNode && factory.consumerNode->id() == planNodeId) {
    return std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
        factory.consumerNode);
  }
  return nullptr;
}

bool adaptDriver(const exec::DriverFactory& factory, exec::Driver& driver) {
  auto* ctx = driver.driverCtx();

  if (std::getenv("VELOX_UCX_STOCK_OUTPUT_BUFFER") == nullptr) {
    return false;
  }
  if (!ctx->queryConfig().get<bool>(
          cudf_velox::CudfConfig::kCudfEnabled,
          cudf_velox::CudfConfig::getInstance().enabled)) {
    return false;
  }

  auto operators = driver.operators();
  for (int32_t i = static_cast<int32_t>(operators.size()) - 1; i >= 0; --i) {
    auto* op = operators[i];
    if (op == nullptr ||
        dynamic_cast<exec::PartitionedOutput*>(op) == nullptr) {
      continue;
    }
    // Only the standard operator. Another transport's is already its own.
    if (op->operatorType() != "PartitionedOutput") {
      continue;
    }

    auto outputNode = findOutputNode(factory, op->planNodeId());
    if (outputNode == nullptr) {
      continue;
    }

    std::vector<std::unique_ptr<exec::Operator>> replacement;
    replacement.push_back(
        std::make_unique<UcxPartitionedOutput>(
            op->operatorId(),
            ctx,
            outputNode,
            eagerFlush(*outputNode),
            // Held but never consulted in stock mode: the operator's flow
            // control and enqueue both go to the output buffer instead.
            UcxOutputQueueManager::getInstanceRef()));
    [[maybe_unused]] auto replaced =
        factory.replaceOperators(driver, i, i + 1, std::move(replacement));
  }

  // Always false, for the same reason as the exchange adapter: DriverFactory
  // stops at the first adapter returning true, and ToCudf must still run.
  return false;
}

} // namespace

void registerCudfPackedOutput() {
  static std::atomic<bool> registered{false};
  bool expected = false;
  if (!registered.compare_exchange_strong(expected, true)) {
    return;
  }

  // Front of the list so ToCudf sees the operator already substituted.
  //
  // ToCudf() currently only adapts transportKind::kUtx nodes, but our
  // proof-of-concept here discovers the transport dynamically.
  //
  // No OperatorAdapter accompanies it: ToCudf keys its host/device boundary
  // rules on operator type, and cuDF registers one for "cudfPartitionedOutput"
  // already. Removing that entry as dead code, once the transport registry it
  // names is gone, would break this path -- loudly, at addInput's "Input must
  // be a CudfVector" check.
  exec::DriverAdapter adapter{
      std::string(kAdapterLabel), /*inspect=*/{}, &adaptDriver};
  exec::DriverFactory::adapters.insert(
      exec::DriverFactory::adapters.begin(), std::move(adapter));
}

} // namespace facebook::velox::ucx_exchange
