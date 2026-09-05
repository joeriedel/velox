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

#include <ucxx/api.h>
#include <memory>

namespace facebook::velox::ucx_exchange {

/// Answers incoming `auto` transport requests on the shared UCX listener.
///
/// Registered under kAmAutoCallbackId through Communicator::registerAmCallback,
/// so it coexists with the cuDF acceptor (id 123) and the CPU-row acceptor
/// (id 124) without either being disturbed. A consumer sends one active message
/// per fetch; each is answered by its own short-lived AutoExchangeServer.
class AutoAcceptor {
 public:
  /// Active-message entry point. Runs on the UCXX progress thread, so it never
  /// throws: a malformed or unservable request is answered or dropped rather
  /// than propagated.
  static void cStyleAMCallback(
      std::shared_ptr<ucxx::Request> request,
      ucp_ep_h endpointHandle);
};

} // namespace facebook::velox::ucx_exchange
