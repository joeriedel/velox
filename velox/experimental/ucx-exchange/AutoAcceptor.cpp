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

#include "velox/experimental/ucx-exchange/AutoAcceptor.h"

#include "velox/experimental/ucx-exchange/DevicePageReaders.h"

#include <glog/logging.h>
#include <cstring>
#include <string>

#include "velox/experimental/ucx-exchange/AutoExchangeServer.h"
#include "velox/experimental/ucx-exchange/AutoWireProtocol.h"
#include "velox/experimental/ucx-exchange/Communicator.h"
#include "velox/experimental/ucx-exchange/EndpointRef.h"

namespace facebook::velox::ucx_exchange {
namespace {

struct ParsedRequest {
  AutoRequestHeader header;
  std::string taskId;
};

// Returns false when the buffer is not a well-formed request for this
// protocol. Callers must not throw, so this reports rather than checks.
bool parseRequest(ucxx::Buffer& buffer, ParsedRequest& parsed) {
  const auto size = buffer.getSize();
  if (size < sizeof(AutoRequestHeader)) {
    LOG(ERROR) << "auto exchange: request smaller than its header (" << size
               << " bytes)";
    return false;
  }

  std::memcpy(&parsed.header, buffer.data(), sizeof(parsed.header));
  if (parsed.header.magic != kAutoRequestMagic) {
    LOG(ERROR) << "auto exchange: bad request magic";
    return false;
  }
  if (parsed.header.version != kAutoProtocolVersion) {
    LOG(ERROR) << "auto exchange: unsupported protocol version "
               << parsed.header.version;
    return false;
  }

  const size_t taskIdBytes = parsed.header.taskIdBytes;
  if (taskIdBytes == 0 || sizeof(AutoRequestHeader) + taskIdBytes > size) {
    LOG(ERROR) << "auto exchange: request task id is missing or truncated";
    return false;
  }

  parsed.taskId.assign(
      static_cast<const char*>(buffer.data()) + sizeof(AutoRequestHeader),
      taskIdBytes);
  return true;
}

} // namespace

// static
void AutoAcceptor::cStyleAMCallback(
    std::shared_ptr<ucxx::Request> request,
    ucp_ep_h endpointHandle) {
  // Runs on the UCXX progress thread. Throwing from a UCX callback is
  // undefined behaviour, so everything here reports and returns.
  try {
    if (request == nullptr || !request->isCompleted()) {
      LOG(ERROR) << "auto exchange: active message callback without a "
                    "completed request";
      return;
    }

    auto buffer =
        std::dynamic_pointer_cast<ucxx::Buffer>(request->getRecvBuffer());
    if (buffer == nullptr) {
      LOG(ERROR) << "auto exchange: active message had no receive buffer";
      return;
    }

    ParsedRequest parsed;
    if (!parseRequest(*buffer, parsed)) {
      return;
    }

    auto communicator = Communicator::getInstance();
    if (communicator == nullptr) {
      LOG(ERROR) << "auto exchange: no communicator to answer on";
      return;
    }

    // The listener already opened an endpoint for this connection, and it is
    // the channel the consumer is listening on, so the reply goes back the way
    // the request arrived.
    auto endpointRef = communicator->findEndpointRefByHandle(endpointHandle);
    if (endpointRef == nullptr) {
      LOG(ERROR) << "auto exchange: no endpoint for the requesting peer";
      return;
    }

    VLOG(3) << "auto exchange: request for task " << parsed.taskId
            << " destination " << parsed.header.destination << " replyTag "
            << std::hex << parsed.header.replyTag << std::dec << " maxBytes "
            << parsed.header.maxBytes;

    // Asking over this transport is how a consumer says it reads device
    // memory. Recorded before the server runs, so pages rendered for this
    // request already know.
    DevicePageReaders::instance().record(
        parsed.taskId, parsed.header.destination);

    // The server decides whether this process can serve the task, and answers
    // with a refusal if not so the consumer can fall back.
    auto server = AutoExchangeServer::create(
        std::move(communicator),
        std::move(endpointRef),
        std::move(parsed.taskId),
        parsed.header.destination,
        parsed.header.replyTag,
        parsed.header.maxBytes,
        parsed.header.sequence);
    if (server == nullptr) {
      LOG(WARNING)
          << "auto exchange: communicator is shutting down, request dropped";
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "auto exchange: failed to accept a request: " << e.what();
  } catch (...) {
    LOG(ERROR) << "auto exchange: failed to accept a request";
  }
}

} // namespace facebook::velox::ucx_exchange
