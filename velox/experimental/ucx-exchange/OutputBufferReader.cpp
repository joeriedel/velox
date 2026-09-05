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

#include "velox/experimental/ucx-exchange/OutputBufferReader.h"

#include <glog/logging.h>

#include "velox/common/base/Exceptions.h"
#include "velox/exec/DefaultOutputBufferManager.h"

namespace facebook::velox::ucx_exchange {

OutputBufferReader::OutputBufferReader(
    std::shared_ptr<exec::DefaultOutputBufferManager> manager,
    std::string taskId,
    int destination,
    uint64_t maxBytes,
    int64_t startSequence)
    : manager_(std::move(manager)),
      taskId_(std::move(taskId)),
      destination_(destination),
      maxBytes_(maxBytes),
      sequence_(startSequence) {
  VELOX_CHECK_NOT_NULL(manager_, "Output buffer manager is null");
  VELOX_CHECK_GE(destination_, 0);
  VELOX_CHECK_GT(maxBytes_, 0);
}

OutputBufferReader::~OutputBufferReader() {
  close();
}

void OutputBufferReader::request(FrameCallback callback) {
  requestInternal(maxBytes_, std::move(callback));
}

void OutputBufferReader::request(uint64_t maxBytes, FrameCallback callback) {
  requestInternal(maxBytes, std::move(callback));
}

void OutputBufferReader::requestSizes(FrameCallback callback) {
  requestInternal(/*maxBytes=*/0, std::move(callback));
}

void OutputBufferReader::requestInternal(
    uint64_t maxBytes,
    FrameCallback callback) {
  VELOX_CHECK(callback != nullptr, "Frame callback is null");

  if (closed_.load(std::memory_order_acquire) ||
      atEnd_.load(std::memory_order_acquire)) {
    callback(Frame{{}, sequence_.load(std::memory_order_acquire), {}, true});
    return;
  }

  const int64_t requested = sequence_.load(std::memory_order_acquire);

  // getData() delivers at most one callback per call, so the cycle is re-armed
  // by the caller requesting again. Requesting sequence N is what releases
  // everything before N at the producer, which is why no explicit acknowledge
  // is needed here.
  manager_->getData(
      taskId_,
      destination_,
      maxBytes,
      requested,
      [this, callback = std::move(callback)](
          std::vector<std::unique_ptr<folly::IOBuf>> pages,
          int64_t sequence,
          std::vector<int64_t> remainingBytes) mutable {
        Frame frame;
        frame.sequence = sequence;
        frame.remainingBytes = std::move(remainingBytes);

        // A null entry is the producer's end marker. It is only ever the last
        // element, and it is not a page to ship.
        for (auto& page : pages) {
          if (page == nullptr) {
            frame.atEnd = true;
            break;
          }
          frame.pages.push_back(std::move(page));
        }

        // Advance past the pages actually delivered so the next request both
        // asks for new data and releases these.
        sequence_.store(
            sequence + static_cast<int64_t>(frame.pages.size()),
            std::memory_order_release);
        if (frame.atEnd) {
          atEnd_.store(true, std::memory_order_release);
        }

        callback(std::move(frame));
      });
}

void OutputBufferReader::close() {
  closed_.store(true, std::memory_order_release);
}

void OutputBufferReader::deleteResults() {
  // Skipping this leaks the buffer, and the task it pins, until the manager is
  // destroyed. It must not run on every close(), because several readers may
  // be serving one task and each request may create its own.
  try {
    manager_->deleteResults(taskId_, destination_);
  } catch (const std::exception& e) {
    // Reached from teardown paths, where throwing would mask the original
    // failure.
    LOG(WARNING) << "Failed to delete results for task " << taskId_
                 << " destination " << destination_ << ": " << e.what();
  }
}

} // namespace facebook::velox::ucx_exchange
