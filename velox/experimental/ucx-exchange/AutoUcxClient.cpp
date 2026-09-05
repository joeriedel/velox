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

#include "velox/experimental/ucx-exchange/AutoUcxClient.h"

#include <glog/logging.h>
#include <atomic>
#include <cstring>

#include "velox/common/base/Exceptions.h"
#include "velox/experimental/ucx-exchange/Communicator.h"
#include "velox/experimental/ucx-exchange/EndpointRef.h"

namespace facebook::velox::ucx_exchange {
namespace {

// Guards against a UCX completion being delivered more than once.
struct CallbackOnce {
  std::atomic<bool> claimed{false};

  bool tryClaim() {
    bool expected = false;
    return claimed.compare_exchange_strong(expected, true);
  }
};

// Reply tags only have to be unique among this process's outstanding
// requests, since they are paired with a specific endpoint.
uint64_t nextReplyTag() {
  static std::atomic<uint64_t> counter{1};
  return counter.fetch_add(1, std::memory_order_relaxed) << 8;
}

} // namespace

// static
std::shared_ptr<AutoUcxClient> AutoUcxClient::create(
    std::shared_ptr<Communicator> communicator,
    const std::string& host,
    uint16_t port,
    std::string taskId,
    int destination) {
  VELOX_CHECK_NOT_NULL(communicator);

  std::shared_ptr<AutoUcxClient> client{
      new AutoUcxClient(communicator, std::move(taskId), destination)};

  if (!communicator->registerCommElement(client)) {
    return nullptr;
  }

  client->endpointRef_ =
      communicator->assocEndpointRef(client, HostPort{host, port});
  if (client->endpointRef_ == nullptr) {
    // No route to the producer. The caller treats this as "cannot serve",
    // not as a failure, so another transport gets a chance.
    client->close();
    return nullptr;
  }
  return client;
}

AutoUcxClient::AutoUcxClient(
    std::shared_ptr<Communicator> communicator,
    std::string taskId,
    int destination)
    : CommElement(std::move(communicator)),
      taskId_(std::move(taskId)),
      destination_(destination) {}

std::shared_ptr<AutoUcxClient> AutoUcxClient::getSelfPtr() {
  return shared_from_this();
}

void AutoUcxClient::process() {
  drainStateEvents();
  if (closeRequested_.load(std::memory_order_acquire)) {
    close();
  }
}

void AutoUcxClient::requestClose() {
  closeRequested_.store(true, std::memory_order_release);
  communicator_->addToWorkQueue(getSelfPtr());
}

void AutoUcxClient::request(uint64_t maxBytes, FrameCallback callback) {
  VELOX_CHECK(callback != nullptr);

  if (closed_.load(std::memory_order_acquire)) {
    callback(OutputBufferReader::Frame{{}, 0, {}, true});
    return;
  }

  callback_ = std::move(callback);
  replyTag_ = nextReplyTag();
  header_ = AutoResponseHeader{};
  segmentsPerPage_.clear();
  segmentSizes_.clear();
  segmentBuffers_.clear();
  pendingRequests_.clear();

  // The receive is posted before the request goes out so the response cannot
  // arrive unmatched.
  postHeaderReceive();
  sendRequest(maxBytes);
}

void AutoUcxClient::postHeaderReceive() {
  state_.store(ClientState::WaitingHeader, std::memory_order_release);
  headerBuffer_.assign(sizeof(AutoResponseHeader), 0);

  auto once = std::make_shared<CallbackOnce>();
  std::weak_ptr<AutoUcxClient> weakSelf = getSelfPtr();
  pendingReceives_.push_back(endpointRef_->endpoint_->tagRecv(
      headerBuffer_.data(),
      headerBuffer_.size(),
      ucxx::Tag{replyTag_},
      ucxx::TagMaskFull,
      false,
      [weakSelf, once](ucs_status_t status, std::shared_ptr<void> /*arg*/) {
        if (!once->tryClaim()) {
          return;
        }
        if (auto self = weakSelf.lock()) {
          self->enqueueStateEvent(
              self, [self, status]() { self->onHeader(status); });
          self->communicator_->addToWorkQueue(self);
        }
      },
      once));
}

void AutoUcxClient::sendRequest(uint64_t maxBytes) {
  auto payload = std::make_shared<std::vector<uint8_t>>();
  AutoRequestHeader header;
  header.destination = destination_;
  header.maxBytes = maxBytes;
  header.replyTag = replyTag_;
  header.sequence = sequence_.load(std::memory_order_acquire);
  header.taskIdBytes = static_cast<uint32_t>(taskId_.size());
  header.workerAddressBytes = 0;

  payload->resize(sizeof(header) + taskId_.size());
  std::memcpy(payload->data(), &header, sizeof(header));
  std::memcpy(payload->data() + sizeof(header), taskId_.data(), taskId_.size());

  ucxx::AmReceiverCallbackInfo info(
      communicator_->kAmCallbackOwner, kAmAutoCallbackId);

  auto once = std::make_shared<CallbackOnce>();
  std::weak_ptr<AutoUcxClient> weakSelf = getSelfPtr();
  pendingRequests_.push_back(endpointRef_->endpoint_->amSend(
      payload->data(),
      payload->size(),
      UCS_MEMORY_TYPE_HOST,
      info,
      false,
      [weakSelf, once, payload](
          ucs_status_t status, std::shared_ptr<void> /*arg*/) {
        if (!once->tryClaim()) {
          return;
        }
        if (status == UCS_OK) {
          return;
        }
        LOG(ERROR) << "auto exchange: request send failed: "
                   << ucs_status_string(status);
        if (auto self = weakSelf.lock()) {
          self->enqueueStateEvent(self, [self]() {
            self->finishEmpty(/*declined=*/false, /*failed=*/true);
          });
          self->communicator_->addToWorkQueue(self);
        }
      },
      payload));
}

void AutoUcxClient::onHeader(ucs_status_t status) {
  if (status != UCS_OK) {
    LOG(ERROR) << "auto exchange: response header receive failed: "
               << ucs_status_string(status);
    finishEmpty(/*declined=*/false, /*failed=*/true);
    return;
  }

  std::memcpy(&header_, headerBuffer_.data(), sizeof(header_));
  if (header_.magic != kAutoResponseMagic ||
      header_.version != kAutoProtocolVersion) {
    LOG(ERROR) << "auto exchange: malformed response header";
    finishEmpty(/*declined=*/false, /*failed=*/true);
    return;
  }

  // A well-formed header of any status means the peer speaks this protocol.
  respondedToProtocol_.store(true, std::memory_order_release);

  if (header_.status != AutoResponseStatus::kAccepted) {
    // The producer will not serve this task here. Not an error: the consumer
    // falls back to another transport.
    VLOG(2) << "auto exchange: producer declined task " << taskId_ << " status "
            << static_cast<int>(header_.status);
    finishEmpty(/*declined=*/true, /*failed=*/false);
    return;
  }

  if (header_.numSegments == 0 && header_.numRemaining == 0) {
    deliverFrame();
    return;
  }
  postLayoutReceive();
}

void AutoUcxClient::postLayoutReceive() {
  state_.store(ClientState::WaitingLayout, std::memory_order_release);
  layoutBuffer_.assign(
      header_.numPages * sizeof(uint32_t) +
          header_.numSegments * sizeof(uint64_t) +
          header_.numRemaining * sizeof(uint64_t),
      0);

  auto once = std::make_shared<CallbackOnce>();
  std::weak_ptr<AutoUcxClient> weakSelf = getSelfPtr();
  pendingReceives_.push_back(endpointRef_->endpoint_->tagRecv(
      layoutBuffer_.data(),
      layoutBuffer_.size(),
      ucxx::Tag{autoLayoutTag(replyTag_)},
      ucxx::TagMaskFull,
      false,
      [weakSelf, once](ucs_status_t status, std::shared_ptr<void> /*arg*/) {
        if (!once->tryClaim()) {
          return;
        }
        if (auto self = weakSelf.lock()) {
          self->enqueueStateEvent(
              self, [self, status]() { self->onLayout(status); });
          self->communicator_->addToWorkQueue(self);
        }
      },
      once));
}

void AutoUcxClient::onLayout(ucs_status_t status) {
  if (status != UCS_OK) {
    LOG(ERROR) << "auto exchange: layout receive failed: "
               << ucs_status_string(status);
    finishEmpty(/*declined=*/false, /*failed=*/true);
    return;
  }

  segmentsPerPage_.resize(header_.numPages);
  segmentSizes_.resize(header_.numSegments);
  remainingBytes_.resize(header_.numRemaining);

  const size_t countsBytes = header_.numPages * sizeof(uint32_t);
  const size_t sizesBytes = header_.numSegments * sizeof(uint64_t);
  if (countsBytes > 0) {
    std::memcpy(segmentsPerPage_.data(), layoutBuffer_.data(), countsBytes);
  }
  if (sizesBytes > 0) {
    std::memcpy(
        segmentSizes_.data(), layoutBuffer_.data() + countsBytes, sizesBytes);
  }
  if (header_.numRemaining > 0) {
    std::memcpy(
        remainingBytes_.data(),
        layoutBuffer_.data() + countsBytes + sizesBytes,
        header_.numRemaining * sizeof(uint64_t));
  }

  if (header_.numSegments == 0) {
    // Nothing to fetch this time, but the producer told us what it still
    // holds, which is what lets the exchange decide to ask again.
    deliverFrame();
    return;
  }
  postSegmentReceives();
}

void AutoUcxClient::postSegmentReceives() {
  state_.store(ClientState::WaitingSegments, std::memory_order_release);
  segmentsOutstanding_.store(header_.numSegments, std::memory_order_release);

  segmentBuffers_.clear();
  segmentBuffers_.reserve(header_.numSegments);
  for (uint32_t index = 0; index < header_.numSegments; ++index) {
    auto buffer = folly::IOBuf::create(segmentSizes_[index]);
    buffer->append(segmentSizes_[index]);
    segmentBuffers_.push_back(std::move(buffer));
  }

  std::weak_ptr<AutoUcxClient> weakSelf = getSelfPtr();
  for (uint32_t index = 0; index < header_.numSegments; ++index) {
    auto once = std::make_shared<CallbackOnce>();
    pendingReceives_.push_back(endpointRef_->endpoint_->tagRecv(
        segmentBuffers_[index]->writableData(),
        segmentSizes_[index],
        ucxx::Tag{autoSegmentTag(replyTag_, index)},
        ucxx::TagMaskFull,
        false,
        [weakSelf, once](ucs_status_t status, std::shared_ptr<void> /*arg*/) {
          if (!once->tryClaim()) {
            return;
          }
          if (auto self = weakSelf.lock()) {
            self->enqueueStateEvent(
                self, [self, status]() { self->onSegment(status); });
            self->communicator_->addToWorkQueue(self);
          }
        },
        once));
  }
}

void AutoUcxClient::onSegment(ucs_status_t status) {
  if (status != UCS_OK) {
    LOG(ERROR) << "auto exchange: segment receive failed: "
               << ucs_status_string(status);
    failed_.store(true, std::memory_order_release);
  }
  if (segmentsOutstanding_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    if (failed_.load(std::memory_order_acquire)) {
      finishEmpty(/*declined=*/false, /*failed=*/true);
      return;
    }
    deliverFrame();
  }
}

void AutoUcxClient::deliverFrame() {
  OutputBufferReader::Frame frame;
  frame.atEnd = header_.atEnd != 0;
  frame.remainingBytes.assign(remainingBytes_.begin(), remainingBytes_.end());

  // Rebuild each page as the chain it was sent as, using the per-page segment
  // counts. A host page has one segment; a cuDF page has two.
  size_t next{0};
  for (uint32_t page = 0; page < segmentsPerPage_.size(); ++page) {
    const uint32_t count = segmentsPerPage_[page];
    if (count == 0 || next + count > segmentBuffers_.size()) {
      LOG(ERROR) << "auto exchange: page layout does not match segments";
      finishEmpty(/*declined=*/false, /*failed=*/true);
      return;
    }
    auto chain = std::move(segmentBuffers_[next]);
    for (uint32_t i = 1; i < count; ++i) {
      chain->appendToChain(std::move(segmentBuffers_[next + i]));
    }
    next += count;
    frame.pages.push_back(std::move(chain));
  }

  // Advance past the pages delivered so the next request asks for new ones
  // and releases these at the producer.
  sequence_.fetch_add(
      static_cast<int64_t>(frame.pages.size()), std::memory_order_acq_rel);

  state_.store(ClientState::Done, std::memory_order_release);
  auto callback = std::move(callback_);
  callback_ = nullptr;
  if (callback != nullptr) {
    callback(std::move(frame));
  }
}

void AutoUcxClient::finishEmpty(bool declined, bool failed) {
  if (declined) {
    declined_.store(true, std::memory_order_release);
  }
  if (failed) {
    failed_.store(true, std::memory_order_release);
  }
  state_.store(ClientState::Done, std::memory_order_release);

  auto callback = std::move(callback_);
  callback_ = nullptr;
  if (callback != nullptr) {
    // Reported as end-of-stream so the exchange stops asking; declined() and
    // failed() tell the caller which of the two it was.
    callback(OutputBufferReader::Frame{{}, 0, {}, true});
  }
}

void AutoUcxClient::forceCloseForShutdown() {
  // Reached from the communicator's own drain, so close directly rather than
  // queueing the close back onto it.
  close();
}

void AutoUcxClient::close() {
  bool expected = false;
  if (!closed_.compare_exchange_strong(expected, true)) {
    return;
  }
  callback_ = nullptr;
  segmentBuffers_.clear();

  // Sends must be retired through the communicator, not merely released. One
  // posted to an endpoint that never connected sits in ucxx's delayed
  // submission queue with the progress thread retrying it, which blocks
  // Communicator::run() from ever joining that thread.
  for (auto& request : pendingRequests_) {
    if (request != nullptr) {
      communicator_->deferRequestCleanup(std::move(request));
    }
  }
  pendingRequests_.clear();

  // A probe that went unanswered leaves receives posted for a response that
  // will never arrive. Releasing the handles is not enough -- UCX still owns
  // them, and the communicator's shutdown drain waits on them -- so they have
  // to be cancelled.
  if (!pendingReceives_.empty()) {
    communicator_->deferTagReceiveCancellation(std::move(pendingReceives_));
    pendingReceives_.clear();
  }

  // Retiring the request is not enough when the endpoint never connected: the
  // progress thread stays inside ucp_am_send_nbx reconfiguring a protocol for
  // a peer that will never answer, and Communicator::run() blocks joining it.
  // The endpoint itself has to go.
  if (failed_.load(std::memory_order_acquire) && endpointRef_ != nullptr) {
    communicator_->deferEndpointCleanup(endpointRef_);
    endpointRef_.reset();
  }

  communicator_->unregister(getSelfPtr());
}

} // namespace facebook::velox::ucx_exchange
