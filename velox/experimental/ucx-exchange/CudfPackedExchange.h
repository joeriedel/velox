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

#include "velox/exec/Exchange.h"

namespace facebook::velox::ucx_exchange {

/// Exchange operator for pages that hold packed cuDF tables.
///
/// The receive-side counterpart to a cuDF producer writing into the ordinary
/// output buffer. Fetching is inherited unchanged from exec::Exchange --
/// splits, the exchange client, blocking, the queue -- and only the reading of
/// a page differs: exec::Exchange deserializes it as a Presto page, which is
/// wrong for a page whose bytes are on the device, so this unpacks it instead.
///
/// A page arrives as the chain its producer built, [host metadata][device
/// data], and cudf::unpack rebuilds a table_view over it in place. The page is
/// retained as the view's owner, so nothing is copied off the device.
///
/// The exchange client is the one the task already built, taken off the
/// operator being replaced with Exchange::releaseExchangeClient(). That
/// release matters: ~Exchange() closes its client, and a closed client
/// discards every source created against it, so an adapter that drops the
/// replaced operator while it still holds the client silently kills the
/// exchange it just substituted into.
class CudfPackedExchange : public exec::Exchange {
 public:
  CudfPackedExchange(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const core::ExchangeNode>& exchangeNode,
      std::shared_ptr<exec::ExchangeClient> exchangeClient);

  RowVectorPtr getOutput() override;

 private:
  // Deserializes a Presto page and uploads it, for a producer that did not
  // pack. Keeps this operator's promise to ToCudf that it produces GPU
  // output.
  RowVectorPtr uploadHostPage(std::unique_ptr<folly::IOBuf> buffer);
};

/// Substitutes CudfPackedExchange for exec::Exchange in cuDF drivers.
/// Idempotent.
///
/// UcxCudfDriverAdapter substitutes its own exchange operator, but only for
/// nodes whose plan says transportKind=kUcx. Nothing here reads a transport
/// out of the plan, so that adapter never fires and this one does the work: it
/// selects on the plan being a cuDF plan instead.
///
/// Those are different questions. Where the data lives decides which operator
/// reads it; how it travelled is answered by whichever exchange source filled
/// the queue.
void registerCudfPackedExchange();

/// Whether this process reads device pages out of an exchange queue, i.e.
/// whether registerCudfPackedExchange() has run. An in-process transport uses
/// this the way a wire transport uses its handshake: it is how a reader that
/// can take device memory says so.
bool readsDevicePages();

} // namespace facebook::velox::ucx_exchange
