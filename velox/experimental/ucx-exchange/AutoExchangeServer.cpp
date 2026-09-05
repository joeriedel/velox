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

#include "velox/experimental/ucx-exchange/AutoExchangeServer.h"

#include <glog/logging.h>
#include <cstring>

#include "velox/common/base/Exceptions.h"
#include "velox/exec/DefaultOutputBufferManager.h"
#include "velox/experimental/ucx-exchange/Communicator.h"
#include "velox/experimental/ucx-exchange/EndpointRef.h"

namespace facebook::velox::ucx_exchange {
namespace {

// Kept alive by UCX for the duration of a send so the callback has somewhere
// to land, and so a completion that arrives twice is only acted on once.
struct SendContext {
  std::atomic<bool> claimed{false};

  bool tryClaim() {
    bool expected = false;
    return claimed.compare_exchange_strong(expected, true);
  }
};

} // namespace

// static
std::shared_ptr<AutoExchangeServer> AutoExchangeServer::create(
    std::shared_ptr<Communicator> communicator,
    std::shared_ptr<EndpointRef> endpointRef,
    std::string taskId,
    int destination,
    uint64_t replyTag,
    uint64_t maxBytes,
    int64_t sequence) {
  VELOX_CHECK_NOT_NULL(communicator);
  VELOX_CHECK_NOT_NULL(endpointRef);

  std::shared_ptr<AutoExchangeServer> server{new AutoExchangeServer(
      std::move(communicator),
      std::move(endpointRef),
      std::move(taskId),
      destination,
      replyTag,
      maxBytes,
      sequence)};

  if (!server->communicator_->registerCommElement(server)) {
    // Shutting down; registerCommElement has already closed it.
    return nullptr;
  }
  server->communicator_->addToWorkQueue(server);
  return server;
}

AutoExchangeServer::AutoExchangeServer(
    std::shared_ptr<Communicator> communicator,
    std::shared_ptr<EndpointRef> endpointRef,
    std::string taskId,
    int destination,
    uint64_t replyTag,
    uint64_t maxBytes,
    int64_t sequence)
    : CommElement(std::move(communicator), std::move(endpointRef)),
      taskId_(std::move(taskId)),
      destination_(destination),
      replyTag_(replyTag),
      maxBytes_(maxBytes),
      reader_(
          std::make_shared<OutputBufferReader>(
              exec::DefaultOutputBufferManager::getInstanceRef(),
              taskId_,
              destination,
              maxBytes > 0 ? maxBytes : 1,
              sequence)) {}

std::shared_ptr<AutoExchangeServer> AutoExchangeServer::getSelfPtr() {
  return shared_from_this();
}

void AutoExchangeServer::setState(ServerState newState) {
  state_.store(newState, std::memory_order_release);
}

void AutoExchangeServer::process() {
  while (true) {
    drainStateEvents();
    if (closed_.load(std::memory_order_acquire)) {
      return;
    }

    switch (state_.load(std::memory_order_acquire)) {
      case ServerState::Created:
        requestData();
        return;
      case ServerState::WaitingForData:
      case ServerState::SendingHeader:
      case ServerState::SendingSegments:
        // Progress arrives as a state event from a UCX or buffer callback.
        return;
      case ServerState::Done:
        close();
        return;
    }
  }
}

void AutoExchangeServer::requestData() {
  auto manager = exec::DefaultOutputBufferManager::getInstanceRef();
  if (manager == nullptr || !manager->stats(taskId_).has_value()) {
    // Nothing here to serve. Refusing is a normal answer, not an error: it is
    // what lets the consumer fall back to another transport.
    sendRefusal(AutoResponseStatus::kUnknownTask);
    return;
  }

  setState(ServerState::WaitingForData);

  auto self = getSelfPtr();
  auto onFrame = [self](OutputBufferReader::Frame frame) {
    auto shared = std::make_shared<OutputBufferReader::Frame>(std::move(frame));
    self->enqueueStateEvent(
        self, [self, shared]() { self->onFrame(std::move(*shared)); });
    self->communicator_->addToWorkQueue(self);
  };

  if (maxBytes_ == 0) {
    reader_->requestSizes(std::move(onFrame));
  } else {
    reader_->request(maxBytes_, std::move(onFrame));
  }
}

void AutoExchangeServer::onFrame(OutputBufferReader::Frame frame) {
  frame_ = std::move(frame);

  // Flatten the pages into their segments, in send order, recording how many
  // segments each page contributed so the consumer can rebuild the chains. A
  // host page contributes one; a cuDF page contributes two,
  // [host metadata][device data]. Sending each separately is what keeps this
  // payload-agnostic.
  segments_.clear();
  std::vector<uint32_t> segmentsPerPage;
  std::vector<uint64_t> segmentSizes;
  for (const auto& page : frame_.pages) {
    uint32_t count{0};
    const auto* head = page.get();
    const auto* buf = head;
    do {
      segments_.push_back(buf);
      segmentSizes.push_back(buf->length());
      ++count;
      buf = buf->next();
    } while (buf != head);
    segmentsPerPage.push_back(count);
  }

  AutoResponseHeader header;
  header.status = AutoResponseStatus::kAccepted;
  header.atEnd = frame_.atEnd ? 1 : 0;
  header.numPages = static_cast<uint32_t>(frame_.pages.size());
  header.numSegments = static_cast<uint32_t>(segments_.size());
  header.numRemaining = static_cast<uint32_t>(frame_.remainingBytes.size());

  // Fixed-size header, so the consumer can post its receive without knowing
  // how much follows.
  headerBuffer_.resize(sizeof(header));
  std::memcpy(headerBuffer_.data(), &header, sizeof(header));

  // Everything variable-length travels separately: per-page segment counts,
  // then segment sizes.
  layoutBuffer_.clear();
  if (!segments_.empty() || !frame_.remainingBytes.empty()) {
    const size_t countsBytes = segmentsPerPage.size() * sizeof(uint32_t);
    const size_t sizesBytes = segmentSizes.size() * sizeof(uint64_t);
    const size_t remainingBytes =
        frame_.remainingBytes.size() * sizeof(uint64_t);
    layoutBuffer_.resize(countsBytes + sizesBytes + remainingBytes);
    if (countsBytes > 0) {
      std::memcpy(layoutBuffer_.data(), segmentsPerPage.data(), countsBytes);
    }
    if (sizesBytes > 0) {
      std::memcpy(
          layoutBuffer_.data() + countsBytes, segmentSizes.data(), sizesBytes);
    }
    if (remainingBytes > 0) {
      std::memcpy(
          layoutBuffer_.data() + countsBytes + sizesBytes,
          frame_.remainingBytes.data(),
          remainingBytes);
    }
  }

  setState(ServerState::SendingHeader);
  sendBuffer(
      headerBuffer_.data(),
      headerBuffer_.size(),
      replyTag_,
      [](AutoExchangeServer* server, ucs_status_t status) {
        server->headerSendComplete(status);
      });
}

void AutoExchangeServer::sendBuffer(
    void* data,
    size_t size,
    uint64_t tag,
    std::function<void(AutoExchangeServer*, ucs_status_t)> onComplete) {
  auto ctx = std::make_shared<SendContext>();
  std::weak_ptr<AutoExchangeServer> weakSelf = getSelfPtr();
  pendingRequests_.push_back(endpointRef_->endpoint_->tagSend(
      data,
      size,
      ucxx::Tag{tag},
      false,
      [weakSelf, onComplete](ucs_status_t status, std::shared_ptr<void> arg) {
        auto context = std::static_pointer_cast<SendContext>(arg);
        if (!context->tryClaim()) {
          return;
        }
        if (auto self = weakSelf.lock()) {
          self->enqueueStateEvent(self, [self, status, onComplete]() {
            onComplete(self.get(), status);
          });
          self->communicator_->addToWorkQueue(self);
        }
      },
      ctx));
}

void AutoExchangeServer::sendRefusal(AutoResponseStatus status) {
  AutoResponseHeader header;
  header.status = status;
  header.atEnd = 1;

  headerBuffer_.resize(sizeof(header));
  std::memcpy(headerBuffer_.data(), &header, sizeof(header));

  setState(ServerState::SendingHeader);
  sendBuffer(
      headerBuffer_.data(),
      headerBuffer_.size(),
      replyTag_,
      [](AutoExchangeServer* server, ucs_status_t /*status*/) {
        server->setState(ServerState::Done);
      });
}

void AutoExchangeServer::headerSendComplete(ucs_status_t status) {
  if (status != UCS_OK) {
    LOG(ERROR) << "auto exchange: response header send failed for task "
               << taskId_ << " destination " << destination_ << ": "
               << ucs_status_string(status);
    setState(ServerState::Done);
    return;
  }
  if (layoutBuffer_.empty()) {
    maybeReleaseResults();
    setState(ServerState::Done);
    return;
  }
  sendBuffer(
      layoutBuffer_.data(),
      layoutBuffer_.size(),
      autoLayoutTag(replyTag_),
      [](AutoExchangeServer* server, ucs_status_t layoutStatus) {
        if (layoutStatus != UCS_OK || server->segments_.empty()) {
          server->maybeReleaseResults();
          server->setState(ServerState::Done);
          return;
        }
        server->sendSegments();
      });
}

void AutoExchangeServer::sendSegments() {
  setState(ServerState::SendingSegments);
  segmentsInFlight_.store(
      static_cast<uint32_t>(segments_.size()), std::memory_order_release);

  for (uint32_t index = 0; index < segments_.size(); ++index) {
    // The pointer may be device memory; UCX resolves the memory type itself,
    // which is why nothing here needs to know what kind of page this is.
    sendBuffer(
        const_cast<uint8_t*>(segments_[index]->data()),
        segments_[index]->length(),
        autoSegmentTag(replyTag_, index),
        [](AutoExchangeServer* server, ucs_status_t status) {
          server->segmentSendComplete(status);
        });
  }
}

void AutoExchangeServer::segmentSendComplete(ucs_status_t status) {
  if (status != UCS_OK) {
    LOG(ERROR) << "auto exchange: segment send failed for task " << taskId_
               << " destination " << destination_ << ": "
               << ucs_status_string(status);
  }
  if (segmentsInFlight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    // Every segment has left, so the pages can be released back to the
    // producer's buffer.
    maybeReleaseResults();
    setState(ServerState::Done);
  }
}

void AutoExchangeServer::maybeReleaseResults() {
  if (!frame_.atEnd) {
    return;
  }
  // End-of-stream has been sent, so no later request can want these pages.
  // Releasing them also releases the task they pin; skipping it leaks both
  // for the life of the process, since the buffer manager is a singleton.
  reader_->deleteResults();
}

void AutoExchangeServer::close() {
  bool expected = false;
  if (!closed_.compare_exchange_strong(expected, true)) {
    return;
  }

  // Only releases this server's view of the results. The producer's buffer
  // outlives it, since other consumers and later requests still read it.
  segments_.clear();
  frame_ = OutputBufferReader::Frame{};
  pendingRequests_.clear();

  communicator_->unregister(getSelfPtr());
}

} // namespace facebook::velox::ucx_exchange
