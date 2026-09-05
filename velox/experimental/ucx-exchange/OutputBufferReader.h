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

#include <folly/io/IOBuf.h>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace facebook::velox::exec {
class DefaultOutputBufferManager;
}

namespace facebook::velox::ucx_exchange {

/// Drains one destination of a task's output buffer, page by page, on behalf of
/// a transport that wants to ship those pages elsewhere.
///
/// The buffer manager's data plane is already payload-agnostic -- pages are
/// carried as `folly::IOBuf`, which describes memory without dereferencing it,
/// so the same path carries host pages and device-resident pages alike. A
/// transport can therefore read from whatever buffer the ordinary
/// `PartitionedOutput` operator wrote to, rather than requiring the operator
/// and the buffer to be replaced together.
///
/// Usage is a request/acknowledge cycle: call request() with a callback,
/// receive one frame, ship it, then call request() again. Acknowledgement is
/// implicit -- requesting sequence N acknowledges everything before it,
/// matching the buffer manager's own contract.
///
/// Not thread-safe against concurrent request() calls for the same reader. One
/// request may be outstanding at a time; the callback may run on any thread.
class OutputBufferReader {
 public:
  /// One batch of pages, as handed over by the buffer manager.
  struct Frame {
    /// Pages in sequence order. Empty when the producer had nothing ready.
    std::vector<std::unique_ptr<folly::IOBuf>> pages;

    /// Sequence number of the first page in 'pages'.
    int64_t sequence{0};

    /// Sizes of pages still buffered at the producer, for flow control.
    std::vector<int64_t> remainingBytes;

    /// True once the producer has signalled that no more data will follow.
    bool atEnd{false};
  };

  using FrameCallback = std::function<void(Frame)>;

  /// Reads 'destination' of 'taskId' from 'manager', fetching at most
  /// 'maxBytes' per frame.
  /// Reads from 'startSequence' onwards. A reader that serves one request and
  /// is then discarded must be told where to resume, since the position lives
  /// with whoever is driving the exchange rather than with the reader.
  OutputBufferReader(
      std::shared_ptr<exec::DefaultOutputBufferManager> manager,
      std::string taskId,
      int destination,
      uint64_t maxBytes,
      int64_t startSequence);

  ~OutputBufferReader();

  /// Requests the next frame. 'callback' runs once, when the producer has data
  /// or reaches the end. Requesting also acknowledges every page delivered by
  /// the previous frame, so the producer can release them.
  ///
  /// Does nothing if the reader has already seen the end or been closed.
  void request(FrameCallback callback);

  /// Same, but caps this frame at 'maxBytes' instead of the configured cap, so
  /// a caller can size a fetch to the space it currently has. A 'maxBytes' of
  /// zero reports sizes without handing over any pages.
  void request(uint64_t maxBytes, FrameCallback callback);

  /// Requests only the sizes of what the producer has buffered, without
  /// consuming any of it. Used to answer the exchange's data-size probes.
  /// The delivered frame carries no pages, only 'remainingBytes' and the
  /// end-of-stream flag.
  void requestSizes(FrameCallback callback);

  /// Returns true once a frame carrying the end marker has been delivered.
  bool atEnd() const {
    return atEnd_.load(std::memory_order_acquire);
  }

  /// Sequence number this reader will request next.
  int64_t sequence() const {
    return sequence_.load(std::memory_order_acquire);
  }

  /// Stops reading. Idempotent.
  ///
  /// Deliberately does not release the producer's results: a reader may be one
  /// of several serving the same task, and short-lived readers are created per
  /// request. Releasing here would destroy the buffer other readers and later
  /// requests still need. Call deleteResults() once the stream is finished.
  void close();

  /// Releases the producer's buffered results for this destination. Call once,
  /// after end-of-stream has been consumed.
  void deleteResults();

 private:
  // The stock buffer manager. Named DefaultOutputBufferManager on this
  // branch, where OutputBufferManager was made abstract and the data plane
  // stayed on the concrete managers; upstream this type is simply
  // OutputBufferManager.
  const std::shared_ptr<exec::DefaultOutputBufferManager> manager_;
  const std::string taskId_;
  const int destination_;
  const uint64_t maxBytes_;

  // Shared implementation of request() and requestSizes(). A 'maxBytes' of
  // zero makes the buffer manager report sizes without handing over pages.
  void requestInternal(uint64_t maxBytes, FrameCallback callback);

  std::atomic<int64_t> sequence_;
  std::atomic<bool> atEnd_{false};
  std::atomic<bool> closed_{false};
};

} // namespace facebook::velox::ucx_exchange
