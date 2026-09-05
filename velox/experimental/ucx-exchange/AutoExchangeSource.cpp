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

#include "velox/experimental/ucx-exchange/AutoExchangeSource.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <folly/Uri.h>
#include <glog/logging.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>

#include "velox/exec/DefaultOutputBufferManager.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/exec/OutputTransportRegistry.h"
#include "velox/exec/SerializedPage.h"
#include "velox/experimental/ucx-exchange/AutoAcceptor.h"
#include "velox/experimental/ucx-exchange/AutoUcxClient.h"
#include "velox/experimental/ucx-exchange/AutoWireProtocol.h"
#include "velox/experimental/ucx-exchange/Communicator.h"
#include "velox/experimental/ucx-exchange/CudfPackedExchange.h"
#include "velox/experimental/ucx-exchange/DevicePageReaders.h"

namespace facebook::velox::ucx_exchange {
namespace {

// Cap on a fetch when the exchange does not ask for a specific size.
constexpr uint64_t kDefaultFetchBytes{1ULL << 20};

// Port every worker's auto-transport listener runs on. Deliberately one
// configured port rather than one derived from the HTTP port: deriving it
// bakes a deployment convention into library code, which is among the things
// this transport is trying to avoid.
uint16_t autoListenerPort() {
  // Read per call rather than cached, so a process can be pointed at a
  // different peer between queries -- and so tests can make the producer
  // unreachable to exercise the fallback.
  if (const char* value = std::getenv("VELOX_UCX_AUTO_PORT")) {
    return static_cast<uint16_t>(std::stoi(value));
  }
  return static_cast<uint16_t>(30260);
}

// Returns whether anything is accepting connections at 'host:port'.
//
// Asked before UCX is involved at all, because a UCX endpoint cannot answer
// it: createEndpointFromHostname() returns an optimistic handle and connects
// in the background, so it succeeds against a dead port and the failure only
// surfaces later through a request callback. That path does not always resolve
// cleanly -- a send to an endpoint that never connects can wedge the progress
// thread inside ucp_am_send_nbx retrying protocol reconfiguration, which then
// blocks Communicator::run() from joining it at shutdown.
//
// A plain TCP connect answers the same question immediately and with no UCX
// involvement, so an unreachable peer is never handed to UCX in the first
// place. It does not fix that ucxx behaviour, it avoids provoking it.
bool peerAcceptsConnections(
    const std::string& host,
    uint16_t port,
    std::chrono::milliseconds timeout) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return false;
  }

  // Closed on every path out, including the early returns below.
  struct FdGuard {
    int fd;
    ~FdGuard() {
      ::close(fd);
    }
  } guard{fd};

  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return false;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
    // Not a dotted-quad address. Leave name resolution to the transport
    // rather than guessing here.
    return true;
  }

  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) ==
      0) {
    return true;
  }
  if (errno != EINPROGRESS) {
    return false;
  }

  pollfd descriptor{fd, POLLOUT, 0};
  const int ready = ::poll(&descriptor, 1, static_cast<int>(timeout.count()));
  if (ready <= 0) {
    // Timed out or failed; treat an unresponsive peer as absent.
    return false;
  }

  int error{0};
  socklen_t length = sizeof(error);
  if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0) {
    return false;
  }
  return error == 0;
}

// How long to wait for a peer to accept a connection before concluding
// nothing is there.
constexpr std::chrono::milliseconds kReachabilityTimeout{300};

// Asks a peer whether it speaks this transport, and records the answer.
//
// Fire and forget: nothing waits for it. The task that triggered this probe
// has already been handed to another transport, and the answer is for the
// tasks that follow.
} // namespace

AutoExchangeSource::AutoExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool,
    Fetcher fetcher,
    std::function<void()> closer)
    : ExchangeSource(taskId, destination, queue, pool),
      fetcher_(std::move(fetcher)),
      closer_(std::move(closer)) {}

bool AutoExchangeSource::shouldRequestLocked() {
  if (atEnd_) {
    return false;
  }
  return !requestPending_.exchange(true);
}

folly::SemiFuture<exec::ExchangeSource::Response> AutoExchangeSource::request(
    uint32_t maxBytes,
    std::chrono::microseconds /*maxWait*/) {
  auto promise = VeloxPromise<Response>("AutoExchangeSource::request");
  auto future = promise.getSemiFuture();
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    promise_ = std::move(promise);
  }

  // The callback can outlive the caller's reference to this source.
  auto self = shared_from_this();
  fetcher_(maxBytes, [self, this](OutputBufferReader::Frame frame) {
    int64_t frameBytes{0};
    VeloxPromise<Response> requestPromise;
    std::vector<ContinuePromise> queuePromises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      requestPending_ = false;
      requestPromise = std::move(promise_);
      for (auto& iobuf : frame.pages) {
        frameBytes += iobuf->computeChainDataLength();
        queue_->enqueueLocked(
            std::make_unique<exec::PrestoSerializedPage>(std::move(iobuf)),
            queuePromises);
      }
      if (frame.atEnd) {
        queue_->enqueueLocked(nullptr, queuePromises);
        atEnd_ = true;
      }
    }

    numPages_.fetch_add(
        static_cast<int64_t>(frame.pages.size()), std::memory_order_relaxed);
    numBytes_.fetch_add(frameBytes, std::memory_order_relaxed);

    for (auto& queuePromise : queuePromises) {
      queuePromise.setValue();
    }
    if (!requestPromise.isFulfilled()) {
      requestPromise.setValue(
          Response{frameBytes, atEnd_, std::move(frame.remainingBytes)});
    }
  });

  return future;
}

folly::SemiFuture<exec::ExchangeSource::Response>
AutoExchangeSource::requestDataSizes(std::chrono::microseconds maxWait) {
  return request(0, maxWait);
}

void AutoExchangeSource::close() {
  if (closer_ != nullptr) {
    closer_();
  }
}

folly::F14FastMap<std::string, int64_t> AutoExchangeSource::stats() const {
  return {
      {"numPages", numPages_.load(std::memory_order_relaxed)},
      {"numBytes", numBytes_.load(std::memory_order_relaxed)},
  };
}

std::string AutoExchangeSource::toString() {
  return fmt::format("AutoExchangeSource: {}", remoteTaskId_);
}

std::shared_ptr<exec::ExchangeSource> createAutoExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool) {
  auto manager = exec::DefaultOutputBufferManager::getInstanceRef();
  if (manager == nullptr || !manager->stats(taskId).has_value()) {
    // Not held here. Decline so the next factory gets the task.
    return nullptr;
  }

  // Same process, so a device pointer handed over is still valid here. Whether
  // the consumer can read one is decided by which exchange operator the plan
  // compiled to, and that is what readsDevicePages() reports. Saying so here
  // is the in-process equivalent of the wire handshake.
  if (readsDevicePages()) {
    DevicePageReaders::instance().record(taskId, destination);
  }

  auto reader = std::make_shared<OutputBufferReader>(
      manager, taskId, destination, kDefaultFetchBytes, /*startSequence=*/0);
  return std::make_shared<AutoExchangeSource>(
      taskId,
      destination,
      std::move(queue),
      pool,
      [reader](uint64_t maxBytes, OutputBufferReader::FrameCallback callback) {
        if (maxBytes == 0) {
          reader->requestSizes(std::move(callback));
        } else {
          reader->request(maxBytes, std::move(callback));
        }
      },
      [reader]() {
        reader->close();
        // The in-process reader is the only one serving this task, so it is
        // also the one that releases the producer's results.
        reader->deleteResults();
      });
}

namespace {

// Longest a peer gets to answer before the task goes to the fallback. Only
// paid once per source, and only by a peer that does not answer.
constexpr std::chrono::milliseconds kProtocolTimeout{2000};

// Wraps the UCX client as an ordinary exchange source, once the peer is known
// to answer.
std::shared_ptr<exec::ExchangeSource> makeUcxBackedSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool,
    std::shared_ptr<AutoUcxClient> client) {
  return std::make_shared<AutoExchangeSource>(
      taskId,
      destination,
      std::move(queue),
      pool,
      [client](uint64_t maxBytes, OutputBufferReader::FrameCallback callback) {
        client->request(maxBytes, std::move(callback));
      },
      [client]() { client->requestClose(); });
}

/// Claims a peer without knowing whether it speaks UCX, and finds out by
/// asking it.
///
/// The transport is settled on the first request and never revisited: UCX if
/// the peer answers, the fallback if it does not. The question is a zero-byte
/// request, so a peer that turns out not to speak UCX leaves the queue as it
/// found it and the fallback starts from a clean stream.
class AutoTransportSource : public exec::ExchangeSource {
 public:
  AutoTransportSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<exec::ExchangeQueue> queue,
      memory::MemoryPool* pool,
      std::shared_ptr<Communicator> communicator,
      std::string host,
      uint16_t port,
      exec::ExchangeSource::Factory fallback)
      : ExchangeSource(taskId, destination, queue, pool),
        communicator_(std::move(communicator)),
        host_(std::move(host)),
        port_(port),
        fallback_(std::move(fallback)) {}

  bool shouldRequestLocked() override {
    if (auto delegate = delegateLocked()) {
      return delegate->shouldRequestLocked();
    }
    return !requestPending_.exchange(true);
  }

  folly::SemiFuture<Response> request(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) override {
    if (auto delegate = delegateLocked()) {
      return delegate->request(maxBytes, maxWait);
    }
    // Already settled: do not ask the peer about UCX again.
    if (choseFallback_.load(std::memory_order_acquire)) {
      return requestViaFallback(maxBytes, maxWait);
    }
    return settleTransport(maxBytes, maxWait);
  }

  folly::SemiFuture<Response> requestDataSizes(
      std::chrono::microseconds maxWait) override {
    return request(0, maxWait);
  }

  void close() override {
    if (auto delegate = delegateLocked()) {
      delegate->close();
    }
  }

  folly::F14FastMap<std::string, int64_t> stats() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return delegate_ != nullptr ? delegate_->stats()
                                : folly::F14FastMap<std::string, int64_t>{};
  }

  std::string toString() override {
    return fmt::format("AutoTransportSource: {}", remoteTaskId_);
  }

 private:
  std::shared_ptr<exec::ExchangeSource> delegateLocked() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return delegate_;
  }

  std::shared_ptr<exec::ExchangeSource> adopt(
      std::shared_ptr<exec::ExchangeSource> source) {
    std::lock_guard<std::mutex> lock(mutex_);
    delegate_ = std::move(source);
    return delegate_;
  }

  // Null if the fallback will not claim the task yet, which is normal: a
  // producer's output buffer does not exist until its task is created.
  std::shared_ptr<exec::ExchangeSource> useFallback() {
    choseFallback_.store(true, std::memory_order_release);
    auto source = fallback_(remoteTaskId_, destination_, queue_, pool_.get());
    if (source == nullptr) {
      return nullptr;
    }
    return adopt(std::move(source));
  }

  // Not at end, so ExchangeClient asks again. Waiting out a producer that has
  // not been created yet, rather than failing or reporting an empty stream.
  folly::SemiFuture<Response> nothingYet() {
    requestPending_.store(false);
    return folly::makeSemiFuture(Response{0, false, {}});
  }

  folly::SemiFuture<Response> requestViaFallback(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) {
    auto source = useFallback();
    if (source == nullptr) {
      return nothingYet();
    }
    return source->request(maxBytes, maxWait);
  }

  // Asks the peer once, then commits to whichever transport answered.
  folly::SemiFuture<Response> settleTransport(
      uint32_t maxBytes,
      std::chrono::microseconds maxWait) {
    requestPending_.store(false);

    // Not a capability question. A UCX endpoint is created optimistically, so
    // sending to a port with nothing behind it can leave the progress thread
    // retrying inside ucp_am_send_nbx and never return.
    if (!peerAcceptsConnections(host_, port_, kReachabilityTimeout)) {
      return requestViaFallback(maxBytes, maxWait);
    }

    auto client = AutoUcxClient::create(
        communicator_, host_, port_, remoteTaskId_, destination_);
    if (client == nullptr) {
      return requestViaFallback(maxBytes, maxWait);
    }

    auto promise = std::make_shared<folly::Promise<bool>>();
    auto future = promise->getSemiFuture();
    auto answered = std::make_shared<std::atomic<bool>>(false);

    // Zero bytes: this only asks whether anyone is home.
    client->request(
        0, [client, promise, answered](OutputBufferReader::Frame /*frame*/) {
          if (!answered->exchange(true)) {
            promise->setValue(
                client->respondedToProtocol() && !client->failed());
          }
        });

    auto self =
        std::static_pointer_cast<AutoTransportSource>(shared_from_this());
    return std::move(future)
        .within(kProtocolTimeout)
        .deferValue([self, client, maxBytes, maxWait](bool speaksUcx) {
          if (!speaksUcx) {
            client->requestClose();
            return self->requestViaFallback(maxBytes, maxWait);
          }
          return self
              ->adopt(makeUcxBackedSource(
                  self->remoteTaskId_,
                  self->destination_,
                  self->queue_,
                  self->pool_.get(),
                  client))
              ->request(maxBytes, maxWait);
        })
        .deferError([self, client, answered, maxBytes, maxWait](
                        folly::exception_wrapper&& /*error*/) {
          // Claim the answer so a late reply cannot fulfil the promise twice.
          answered->store(true);
          client->requestClose();
          return self->requestViaFallback(maxBytes, maxWait);
        });
  }

  const std::shared_ptr<Communicator> communicator_;
  const std::string host_;
  const uint16_t port_;
  const exec::ExchangeSource::Factory fallback_;

  std::atomic<bool> requestPending_{false};
  std::atomic<bool> choseFallback_{false};

  mutable std::mutex mutex_;
  std::shared_ptr<exec::ExchangeSource> delegate_;
};

} // namespace

exec::ExchangeSource::Factory makeAutoUcxFactory(
    exec::ExchangeSource::Factory fallback) {
  VELOX_CHECK(fallback != nullptr, "Auto UCX transport needs a fallback");
  return
      [fallback = std::move(fallback)](
          const std::string& taskId,
          int destination,
          std::shared_ptr<exec::ExchangeQueue> queue,
          memory::MemoryPool* pool) -> std::shared_ptr<exec::ExchangeSource> {
        std::string host;
        try {
          folly::Uri uri(taskId);
          host = uri.host();
        } catch (const std::exception&) {
          // Not a URL this transport understands. Decline so the next factory
          // gets it.
          return nullptr;
        }
        if (host.empty()) {
          return nullptr;
        }

        auto communicator = Communicator::getInstance();
        if (communicator == nullptr) {
          return nullptr;
        }

        return std::make_shared<AutoTransportSource>(
            taskId,
            destination,
            std::move(queue),
            pool,
            std::move(communicator),
            std::move(host),
            autoListenerPort(),
            fallback);
      };
}

void registerAutoTransport(
    bool enableUcx,
    exec::ExchangeSource::Factory fallback) {
  VELOX_CHECK(fallback != nullptr, "Auto transport needs a fallback");
  static std::atomic<bool> registered{false};
  bool expected = false;
  if (!registered.compare_exchange_strong(expected, true)) {
    return;
  }

  // Nothing is registered for the send side. The producer runs the ordinary
  // PartitionedOutput into the stock output buffer, as any Velox producer
  // does, and the transport reads that buffer rather than replacing it. There
  // is no transport for a plan to name.
  //
  // 'fallback' is registered twice because it answers two different
  // questions.
  //
  // Inside the UCX factory it is the transport for a peer that was claimed but
  // did not answer. Registered again behind it, it is the factory for a task
  // the UCX factory could not claim at all -- one whose id is not a URL, so
  // there is no peer to ask.
  if (enableUcx) {
    auto communicator = Communicator::getInstance();
    VELOX_CHECK_NOT_NULL(
        communicator, "Auto transport needs a Communicator to serve UCX");
    communicator->registerAmCallback(
        kAmAutoCallbackId, &AutoAcceptor::cStyleAMCallback);
    exec::ExchangeSource::registerFactory(makeAutoUcxFactory(fallback));
  }

  exec::ExchangeSource::registerFactory(std::move(fallback));
}

} // namespace facebook::velox::ucx_exchange
