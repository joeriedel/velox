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

#pragma once

#include <memory>
#include <mutex>
#include <string>

#include <cudf/contiguous_split.hpp>

#include "velox/exec/SerializedPage.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::ucx_exchange {

/// An output page holding a packed cuDF table, rendered when it is read.
///
/// The producer does not decide whether its output leaves as device memory or
/// as host bytes. It cannot: the reader is another task's exchange or the
/// coordinator over HTTP, and nothing in the producer's own plan distinguishes
/// them. So the page keeps the packed table and decides at getIOBuf(), by
/// which time the reader has asked and identified itself in
/// DevicePageReaders.
///
/// - A destination that reads device memory gets the chain the pack produced,
///   [host metadata][device data], with nothing copied.
/// - Any other destination gets Presto-serialized host bytes, produced here by
///   unpacking, converting to a RowVector and serializing.
///
/// This is what keeps CudfToVelox to the cases that genuinely need host
/// memory. A final stage feeding another worker over a device-capable
/// transport stays on the device; a final stage feeding the coordinator does
/// not, without either being named in the plan.
class CudfPackedPage : public exec::SerializedPageBase {
 public:
  CudfPackedPage(
      std::unique_ptr<cudf::packed_columns> packed,
      std::string taskId,
      int destination,
      int64_t numRows,
      RowTypePtr rowType,
      memory::MemoryPool* pool);

  /// The packed size, which is what the device reader will take. A host reader
  /// gets a different number of bytes; the buffer uses this for accounting
  /// only, so the estimate is allowed to be wrong in that direction.
  uint64_t size() const override;

  std::optional<int64_t> numRows() const override;

  std::unique_ptr<folly::IOBuf> getIOBuf() const override;

  std::unique_ptr<ByteInputStream> prepareStreamForDeserialize() override;

 private:
  // Serializes the packed table as Presto host bytes. Cached, since the buffer
  // may hand the same page out more than once.
  const std::unique_ptr<folly::IOBuf>& hostBytes() const;

  const std::shared_ptr<cudf::packed_columns> packed_;
  const std::string taskId_;
  const int destination_;
  const int64_t numRows_;
  const RowTypePtr rowType_;
  memory::MemoryPool* const pool_;
  const uint64_t packedSize_;

  mutable std::mutex mutex_;
  mutable std::unique_ptr<folly::IOBuf> hostBytes_;
};

} // namespace facebook::velox::ucx_exchange
