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

#include <functional>
#include <memory>
#include <string>

#include "velox/exec/ExchangeSource.h"
#include "velox/experimental/ucx-exchange/OutputBufferReader.h"

namespace facebook::velox::ucx_exchange {

/// Exchange source for the `auto` transport.
///
/// Unlike the UCX exchange, this does not replace the exchange operator and
/// does not read a transport out of the plan. It is selected the way Velox
/// already selects exchange sources: `ExchangeSource::create()` walks the
/// registered factories and takes the first that claims the remote task, and a
/// factory that cannot serve a producer declines by returning nullptr so the
/// next one -- HTTP, in a Prestissimo deployment -- gets it instead.
///
/// The source does not know how pages reach it. It is given a fetcher, and the
/// two that exist hand back the same OutputBufferReader::Frame:
///
/// - OutputBufferReader, when the producer's output buffer is in this process.
/// - AutoUcxClient, when it is on another host, over UCX.
///
/// Both read the buffer that the *standard* PartitionedOutput operator wrote
/// to, so neither needs a transport-specific output operator or buffer
/// manager, and neither names a payload type.
class AutoExchangeSource : public exec::ExchangeSource {
 public:
  /// Asks the producer for up to 'maxBytes' of pages, zero meaning sizes only,
  /// and delivers one frame to the callback.
  using Fetcher =
      std::function<void(uint64_t maxBytes, OutputBufferReader::FrameCallback)>;

  AutoExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue,
      memory::MemoryPool* pool,
      Fetcher fetcher,
      std::function<void()> closer);

  bool shouldRequestLocked() override;

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) override;

  /// Reports what the producer has buffered without consuming it. Routed
  /// through a zero-byte request so it shares request()'s bookkeeping --
  /// notably clearing the pending flag, which ExchangeClient asserts on before
  /// issuing its next request.
  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override;

  void close() override;

  folly::F14FastMap<std::string, int64_t> stats() const override;

  std::string toString() override;

 private:
  const Fetcher fetcher_;
  const std::function<void()> closer_;

  std::atomic<int64_t> numPages_{0};
  std::atomic<int64_t> numBytes_{0};
  std::atomic<bool> requestPending_{false};

  // Guarded by queue_->mutex().
  bool atEnd_{false};
  VeloxPromise<Response> promise_{VeloxPromise<Response>::makeEmpty()};
};

/// Serves producers whose output buffer lives in this process, and declines
/// the rest so another factory can claim them.
std::shared_ptr<exec::ExchangeSource> createAutoExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool);

/// Builds a factory that carries remote tasks over UCX, handing a peer that
/// does not answer to 'fallback'.
///
/// Nothing tells it which peers speak UCX. It claims every remote peer and
/// settles the question on the source's first request by asking the peer, so a
/// peer that does speak UCX is carried by it from its first exchange.
///
/// The fallback is composed in rather than found in the factory list, because
/// a source cannot name the factory after itself: Factory is a std::function,
/// which is not equality-comparable, and wrapping this factory for metrics
/// would defeat matching on identity anyway.
exec::ExchangeSource::Factory makeAutoUcxFactory(
    exec::ExchangeSource::Factory fallback);

/// Enables the `auto` transport. Idempotent.
///
/// Two registrations, neither of which modifies Velox core. Nothing is
/// registered for the send side: the producer runs the ordinary
/// PartitionedOutput into the stock output buffer and the transport reads it,
/// so there is no transport for a plan to name.
///
/// - The acceptor is registered on the shared UCX listener under its own
///   active-message id, alongside the cuDF and CPU-row acceptors.
///
/// - The UCX ExchangeSource factory is registered ahead of 'fallback', so the
///   receive side is chosen by asking the peer rather than declared in the
///   plan.
///
/// @param enableUcx Registers the UCX acceptor and factory. When false only
/// the in-process path is offered, which is useful for isolating behaviour in
/// tests.
void registerAutoTransport(
    bool enableUcx,
    exec::ExchangeSource::Factory fallback);

} // namespace facebook::velox::ucx_exchange
