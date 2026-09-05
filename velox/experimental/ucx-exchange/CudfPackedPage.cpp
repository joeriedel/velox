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

#include "velox/experimental/ucx-exchange/CudfPackedPage.h"

#include "velox/experimental/cudf/exec/GpuResources.h"

#include "velox/common/memory/ByteStream.h"
#include "velox/experimental/cudf/CudfNoDefaults.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"
#include "velox/experimental/ucx-exchange/DevicePageReaders.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::ucx_exchange {
namespace {

using PackedHolder = std::shared_ptr<cudf::packed_columns>;

void releasePackedHolder(void* /*buf*/, void* userData) {
  delete static_cast<PackedHolder*>(userData);
}

// An IOBuf over memory the packed table owns. The pointer is described, never
// dereferenced, so this is how a device buffer travels through host code.
std::unique_ptr<folly::IOBuf>
wrapRegion(void* data, size_t size, const PackedHolder& holder) {
  return folly::IOBuf::takeOwnership(
      data, size, releasePackedHolder, new PackedHolder{holder});
}

} // namespace

CudfPackedPage::CudfPackedPage(
    std::unique_ptr<cudf::packed_columns> packed,
    std::string taskId,
    int destination,
    int64_t numRows,
    RowTypePtr rowType,
    memory::MemoryPool* pool)
    : packed_(std::move(packed)),
      taskId_(std::move(taskId)),
      destination_(destination),
      numRows_(numRows),
      rowType_(std::move(rowType)),
      pool_(pool),
      packedSize_(
          (packed_->metadata ? packed_->metadata->size() : 0) +
          (packed_->gpu_data ? packed_->gpu_data->size() : 0)) {}

uint64_t CudfPackedPage::size() const {
  return packedSize_;
}

std::optional<int64_t> CudfPackedPage::numRows() const {
  return numRows_;
}

std::unique_ptr<folly::IOBuf> CudfPackedPage::getIOBuf() const {
  if (DevicePageReaders::instance().accepts(taskId_, destination_)) {
    const size_t metadataBytes =
        packed_->metadata ? packed_->metadata->size() : 0;
    const size_t deviceBytes =
        packed_->gpu_data ? packed_->gpu_data->size() : 0;

    auto chain = wrapRegion(packed_->metadata->data(), metadataBytes, packed_);
    if (deviceBytes > 0) {
      chain->appendToChain(
          wrapRegion(packed_->gpu_data->data(), deviceBytes, packed_));
    }
    return chain;
  }
  return hostBytes()->clone();
}

const std::unique_ptr<folly::IOBuf>& CudfPackedPage::hostBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (hostBytes_ != nullptr) {
    return hostBytes_;
  }

  // Nobody asked for device memory, so the table comes back to the host here.
  // This is the conversion CudfToVelox would have done had the plan decided
  // the format up front; doing it now means it happens only for a reader that
  // actually needs it.
  auto stream = cudf_velox::cudfGlobalStreamPool().get_stream();
  auto table = cudf::unpack(
      static_cast<uint8_t const*>(packed_->metadata->data()),
      static_cast<uint8_t const*>(packed_->gpu_data->data()));
  auto rowVector = cudf_velox::with_arrow::toVeloxColumn(
      table,
      pool_,
      rowType_,
      /*namePrefix=*/"",
      stream,
      cudf_velox::get_temp_mr());
  stream.synchronize();
  rowVector->setType(rowType_);

  auto* serde = getNamedVectorSerde(
      VectorSerde::kindName(VectorSerde::Kind::kPresto));
  VectorStreamGroup group(pool_, serde);
  group.createStreamTree(rowType_, static_cast<int32_t>(numRows_));
  const IndexRange range{0, static_cast<vector_size_t>(rowVector->size())};
  group.append(rowVector, folly::Range<const IndexRange*>(&range, 1));

  IOBufOutputStream out(*pool_, nullptr, static_cast<int32_t>(group.size()));
  group.flush(&out);
  hostBytes_ = out.getIOBuf();
  return hostBytes_;
}

std::unique_ptr<ByteInputStream> CudfPackedPage::prepareStreamForDeserialize() {
  // Only a host reader deserializes, and it reads what hostBytes() produced.
  VELOX_NYI(
      "CudfPackedPage is read through getIOBuf(), not deserialized in place");
}

} // namespace facebook::velox::ucx_exchange
