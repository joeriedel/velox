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
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

namespace facebook::velox::ucx_exchange {

/// Wire protocol for the `auto` transport.
///
/// Deliberately smaller than the CPU-row and cuDF protocols. Those carry
/// broadcast destinations, late registration, abort requests and same-host
/// endpoint negotiation. This one covers a single question -- can you serve
/// this task, and if so send me the next pages -- because its purpose is to
/// show the transport does not need its own output buffer, not to replace a
/// production exchange.
///
/// Shape of one exchange, all of it driven by the consumer:
///
///   consumer --[active message: AutoRequestHeader]--> producer
///   consumer <--[tag: AutoResponseHeader, fixed size]------ producer
///   consumer <--[tag: page layout, if any segments]-------- producer
///   consumer <--[tag: one message per segment]------------- producer
///
/// The header is fixed size so the consumer can post its receive before
/// knowing how much follows; everything variable-length is described by it and
/// arrives on its own tag.
///
/// The consumer picks the reply tag and includes its worker address, so the
/// producer can open an endpoint back without a listener of its own.
///
/// Segments, not concatenated bytes. A page is an IOBuf chain, and its
/// segments need not live in the same kind of memory: a cuDF page is
/// [host metadata][device data]. Concatenating those into one message would
/// force a device-to-host copy and defeat the point, so every segment is sent
/// on its own, letting UCX detect the memory type of each pointer. A host page
/// is simply a chain of one, so the same path carries both payload kinds
/// without knowing which it has.

/// "AUTO" -- identifies this protocol's handshakes on a shared listener.
constexpr uint32_t kAutoRequestMagic{0x4155'544f};
constexpr uint32_t kAutoResponseMagic{0x4155'5450};
constexpr uint16_t kAutoProtocolVersion{1};

/// Active-message callback id for the auto transport. The cuDF path owns 123
/// and the CPU-row path owns 124; transports are told apart by this id, which
/// is what lets all three share one listener.
constexpr ucxx::AmReceiverCallbackIdType kAmAutoCallbackId{125};

/// Whether the producer will serve this task over the auto transport.
///
/// A refusal is the mechanism that makes fallback possible: the consumer takes
/// it as "not available here", not as a failure, and lets the next registered
/// ExchangeSource factory claim the task. This is the same role
/// CpuRowHandshakeResponseStatus::kServerUnavailable plays in the CPU-row
/// protocol, except that there a refusal fails the query.
enum class AutoResponseStatus : uint8_t {
  /// Pages follow.
  kAccepted = 0,
  /// No output buffer for this task in the producer process.
  kUnknownTask = 1,
  /// The task exists but its results have already been released.
  kGone = 2,
};

/// Consumer's request, sent as an active message under kAmAutoCallbackId.
/// Followed by 'taskIdBytes' of task id, then 'workerAddressBytes' of the
/// consumer's UCX worker address.
struct AutoRequestHeader {
  uint32_t magic{kAutoRequestMagic};
  uint16_t version{kAutoProtocolVersion};
  uint16_t headerSize{sizeof(AutoRequestHeader)};

  /// Output buffer destination being read.
  int32_t destination{0};

  /// Cap on the bytes of pages the producer should return. Zero asks for
  /// sizes only, which is how the exchange probes without consuming.
  uint64_t maxBytes{0};

  /// Tag the producer must send its response on. Chosen by the consumer so
  /// concurrent requests from one process do not collide.
  uint64_t replyTag{0};

  /// First page the consumer wants. The producer is stateless across
  /// requests -- each is answered by its own short-lived server -- so the
  /// consumer carries the position, exactly as it does over HTTP. Requesting
  /// sequence N also acknowledges everything before it.
  int64_t sequence{0};

  uint32_t taskIdBytes{0};
  uint32_t workerAddressBytes{0};
};

static_assert(std::is_standard_layout_v<AutoRequestHeader>);
static_assert(sizeof(AutoRequestHeader) == 48);

/// Producer's response, sent by tag on the consumer's 'replyTag'. When
/// 'numPages' is non-zero it is followed by that many uint64 page sizes, and
/// then by a second tag message holding the pages concatenated in order.
struct AutoResponseHeader {
  uint32_t magic{kAutoResponseMagic};
  uint16_t version{kAutoProtocolVersion};
  uint16_t headerSize{sizeof(AutoResponseHeader)};

  AutoResponseStatus status{AutoResponseStatus::kAccepted};
  /// Set once the producer has signalled that no more data will follow.
  uint8_t atEnd{0};
  uint16_t reserved{0};

  /// Number of pages in this response.
  uint32_t numPages{0};

  /// Total number of segments across those pages. Each is sent as its own
  /// message so segment boundaries survive, which is what lets a page be
  /// rebuilt as the chain it was sent as.
  uint32_t numSegments{0};

  /// Pages still buffered at the producer after this response, one entry
  /// each. The consumer needs these to decide whether another fetch is worth
  /// making: without them ExchangeClient never learns there is data to ask
  /// for and probes forever.
  uint32_t numRemaining{0};
};

static_assert(std::is_standard_layout_v<AutoResponseHeader>);
static_assert(sizeof(AutoResponseHeader) == 24);

/// Tag carrying the page layout for a response answered on 'replyTag'.
///
/// The layout message is 'numPages' uint32 segment counts, then 'numSegments'
/// uint64 segment sizes, then 'numRemaining' uint64 sizes of pages still
/// buffered at the producer. The counts are what let a consumer put segments
/// back into the right chains: a host page contributes one, a cuDF page
/// contributes two ([host metadata][device data]). It is sent whenever there
/// is anything to describe, including a response carrying no pages but
/// reporting what is still available.
inline uint64_t autoLayoutTag(uint64_t replyTag) {
  return replyTag ^ 0x9e37'79b9'7f4a'7c15ULL;
}

/// Tag carrying segment 'index' of the response answered on 'replyTag'.
/// Derived rather than negotiated so neither side has to track extra tags,
/// and distinct per segment so receives can be posted concurrently.
inline uint64_t autoSegmentTag(uint64_t replyTag, uint32_t index) {
  return autoLayoutTag(replyTag) + 1 + index;
}

} // namespace facebook::velox::ucx_exchange
