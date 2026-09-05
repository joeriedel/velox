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

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "velox/experimental/ucx-exchange/AutoWireProtocol.h"
#include "velox/experimental/ucx-exchange/CommElement.h"
#include "velox/experimental/ucx-exchange/OutputBufferReader.h"

namespace facebook::velox::ucx_exchange {

class Communicator;

/// Fetches pages from a remote producer's ordinary output buffer over UCX.
///
/// The consumer side of the `auto` transport, and the counterpart to
/// OutputBufferReader: both hand back an OutputBufferReader::Frame, so the
/// exchange source above them does not care whether the producer is in this
/// process or on another host. That symmetry is the point -- one source, two
/// ways of reaching the same ordinary output buffer.
///
/// One request is in flight at a time, matching the exchange's own
/// request/acknowledge cycle. A request runs as three receives: a fixed-size
/// header, then the page layout, then one message per segment. Segments are
/// received separately so a page can be rebuilt as the chain it was sent as,
/// and so device-resident segments never have to be coalesced with host ones.
class AutoUcxClient : public CommElement,
                      public std::enable_shared_from_this<AutoUcxClient> {
 public:
  using FrameCallback = OutputBufferReader::FrameCallback;

  /// Connects to the producer's UCX listener. Returns nullptr when no
  /// connection is possible, which the caller should treat as "this transport
  /// cannot serve the task" rather than as a failure.
  static std::shared_ptr<AutoUcxClient> create(
      std::shared_ptr<Communicator> communicator,
      const std::string& host,
      uint16_t port,
      std::string taskId,
      int destination);

  /// Asks the producer for up to 'maxBytes' of pages. Zero asks for sizes
  /// only. 'callback' runs once, on the progress thread.
  void request(uint64_t maxBytes, FrameCallback callback);

  /// True once the producer has answered that it will not serve this task.
  /// The consumer uses this to fall back to another transport instead of
  /// failing the query.
  bool declined() const {
    return declined_.load(std::memory_order_acquire);
  }

  /// True if the wire failed rather than the producer refusing.
  bool failed() const {
    return failed_.load(std::memory_order_acquire);
  }

  /// True once the peer has answered this transport's protocol, whatever it
  /// answered. A peer that refuses a task still speaks the transport, so this
  /// is what capability probing cares about -- unlike declined(), which is
  /// about one task.
  bool respondedToProtocol() const {
    return respondedToProtocol_.load(std::memory_order_acquire);
  }

  void process() override;

  /// Participates in the communicator's shutdown drain.
  ///
  /// A send posted to a peer that never connected stays in ucxx's delayed
  /// submission queue, where the progress thread retries protocol
  /// reconfiguration for it indefinitely. Communicator::run() then blocks
  /// joining that thread and shutdown never completes. Handing the request to
  /// the communicator's cleanup lets the drain retire it.
  bool supportsCommunicatorShutdownDrain() const override {
    return true;
  }

  void forceCloseForShutdown() override;

  /// Asks for this client to be closed on the communicator's own thread.
  ///
  /// Closing touches the communicator's element and cancellation state, which
  /// the dispatch loop already holds while it is running process(). Calling
  /// close() from another thread -- the exchange, or a factory abandoning a
  /// probe -- races that ordering. CommElement is built around a single-owner
  /// process() path, so the close is queued onto it instead.
  void requestClose();

  void close() override;

 private:
  enum class ClientState : uint32_t {
    Idle,
    WaitingHeader,
    WaitingLayout,
    WaitingSegments,
    Done,
  };

  AutoUcxClient(
      std::shared_ptr<Communicator> communicator,
      std::string taskId,
      int destination);

  std::shared_ptr<AutoUcxClient> getSelfPtr();

  void postHeaderReceive();
  void sendRequest(uint64_t maxBytes);
  void onHeader(ucs_status_t status);
  void postLayoutReceive();
  void onLayout(ucs_status_t status);
  void postSegmentReceives();
  void onSegment(ucs_status_t status);

  // Rebuilds pages from the received segments and hands the frame back.
  void deliverFrame();

  // Ends the current request without pages, recording why.
  void finishEmpty(bool declined, bool failed);

  const std::string taskId_;
  const int destination_;

  std::atomic<ClientState> state_{ClientState::Idle};
  std::atomic<bool> closed_{false};
  std::atomic<bool> closeRequested_{false};
  std::atomic<bool> declined_{false};
  std::atomic<bool> failed_{false};
  std::atomic<bool> respondedToProtocol_{false};

  // Per-request state. Only one request is outstanding, so these are reset at
  // the start of each.
  FrameCallback callback_;
  uint64_t replyTag_{0};

  // Position in the producer's output buffer. The producer answers each
  // request from a fresh short-lived server, so the consumer is what carries
  // the position between requests.
  std::atomic<int64_t> sequence_{0};
  AutoResponseHeader header_{};
  std::vector<uint8_t> headerBuffer_;
  std::vector<uint8_t> layoutBuffer_;
  std::vector<uint32_t> segmentsPerPage_;
  std::vector<uint64_t> segmentSizes_;

  // Sizes of pages the producer still holds, relayed so the exchange can
  // decide whether another fetch is worth making.
  std::vector<uint64_t> remainingBytes_;
  std::vector<std::unique_ptr<folly::IOBuf>> segmentBuffers_;
  std::atomic<uint32_t> segmentsOutstanding_{0};

  // Sends kept alive while UCX owns them.
  std::vector<std::shared_ptr<ucxx::Request>> pendingRequests_;

  // Receives kept separately, because they must be cancelled rather than
  // merely released: a posted tagRecv that nobody will ever satisfy keeps the
  // communicator's shutdown drain waiting forever.
  std::vector<std::shared_ptr<ucxx::Request>> pendingReceives_;
};

} // namespace facebook::velox::ucx_exchange
