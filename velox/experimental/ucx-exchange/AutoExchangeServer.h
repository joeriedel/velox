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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "velox/experimental/ucx-exchange/AutoWireProtocol.h"
#include "velox/experimental/ucx-exchange/CommElement.h"
#include "velox/experimental/ucx-exchange/OutputBufferReader.h"

namespace facebook::velox::ucx_exchange {

class Communicator;
class EndpointRef;

/// Serves one request for pages from a producer's ordinary output buffer.
///
/// Modelled on UcxExchangeServer, and deliberately narrower. That server owns
/// a long-lived conversation with one consumer, holding a cuDF-specific queue,
/// an intra-node fast path and an abort protocol. This one answers a single
/// request and finishes, because the consumer drives the exchange and issues a
/// new request whenever it wants more.
///
/// Two things distinguish it from both existing servers, and they are the
/// point of the exercise:
///
/// - It reads through OutputBufferReader, so its pages come from the buffer
///   the *standard* PartitionedOutput operator wrote to. No transport-specific
///   output operator or buffer manager is involved.
///
/// - It never names a payload type. Pages arrive as folly::IOBuf chains and
///   every segment is sent as its own message, so UCX resolves the memory type
///   of each pointer. A host page is a chain of one; a cuDF page is
///   [host metadata][device data]. The server does not need to know which it
///   has, which is what lets one transport serve both.
class AutoExchangeServer
    : public CommElement,
      public std::enable_shared_from_this<AutoExchangeServer> {
 public:
  /// Where a request has got to. Requests are answered in one pass, so the
  /// machine only has to survive the two sends and their completions.
  enum class ServerState : uint32_t {
    /// Registered, not yet asked the buffer for anything.
    Created,
    /// Waiting for the output buffer to hand over a frame.
    WaitingForData,
    /// Response header in flight.
    SendingHeader,
    /// Segment payloads in flight.
    SendingSegments,
    /// Finished, successfully or otherwise.
    Done,
  };

  /// Builds a server for one request and registers it with 'communicator'.
  /// Returns nullptr if the communicator is shutting down.
  ///
  /// @param endpointRef Endpoint back to the consumer, opened from the worker
  /// address it supplied in its request.
  /// @param taskId Producer task whose output buffer is being read.
  /// @param destination Output buffer destination being read.
  /// @param replyTag Tag the consumer is listening on; segment tags derive
  /// from it.
  /// @param maxBytes Cap the consumer placed on this fetch. Zero asks for
  /// sizes only.
  static std::shared_ptr<AutoExchangeServer> create(
      std::shared_ptr<Communicator> communicator,
      std::shared_ptr<EndpointRef> endpointRef,
      std::string taskId,
      int destination,
      uint64_t replyTag,
      uint64_t maxBytes,
      int64_t sequence);

  void process() override;

  void close() override;

  bool isClosed() const {
    return closed_.load(std::memory_order_acquire);
  }

 private:
  AutoExchangeServer(
      std::shared_ptr<Communicator> communicator,
      std::shared_ptr<EndpointRef> endpointRef,
      std::string taskId,
      int destination,
      uint64_t replyTag,
      uint64_t maxBytes,
      int64_t sequence);

  std::shared_ptr<AutoExchangeServer> getSelfPtr();

  // Asks the output buffer for the next frame. The callback can land on any
  // thread, so it hands the result back through a state event.
  void requestData();

  // Builds the response header and starts sending it.
  void onFrame(OutputBufferReader::Frame frame);

  // Answers a request this process cannot serve, so the consumer can fall
  // back to another transport rather than treating it as a failure.
  void sendRefusal(AutoResponseStatus status);

  void headerSendComplete(ucs_status_t status);

  // Starts one tagSend and routes its completion back into the state machine.
  void sendBuffer(
      void* data,
      size_t size,
      uint64_t tag,
      std::function<void(AutoExchangeServer*, ucs_status_t)> onComplete);

  void sendSegments();

  void segmentSendComplete(ucs_status_t status);

  // Releases the producer's results once end-of-stream has been sent.
  // Without this nothing ever frees them: each request is served by a
  // short-lived server, and the consumer has no way to say it is finished.
  void maybeReleaseResults();

  void setState(ServerState newState);

  const std::string taskId_;
  const int destination_;
  const uint64_t replyTag_;
  const uint64_t maxBytes_;

  const std::shared_ptr<OutputBufferReader> reader_;

  std::atomic<ServerState> state_{ServerState::Created};
  std::atomic<bool> closed_{false};

  // Fixed-size response header, kept alive for the duration of its send.
  std::vector<uint8_t> headerBuffer_;

  // Per-page segment counts followed by segment sizes. Describes how to put
  // the segments back into chains.
  std::vector<uint8_t> layoutBuffer_;

  // Pages being sent. Holds the IOBufs alive until every segment completes,
  // which is what keeps the producer's buffer from releasing them early.
  OutputBufferReader::Frame frame_;

  // Flattened view of the segments in 'frame_', in send order.
  std::vector<const folly::IOBuf*> segments_;

  // Segments whose sends have not yet completed.
  std::atomic<uint32_t> segmentsInFlight_{0};

  // Requests kept alive while UCX owns them.
  std::vector<std::shared_ptr<ucxx::Request>> pendingRequests_;
};

} // namespace facebook::velox::ucx_exchange
