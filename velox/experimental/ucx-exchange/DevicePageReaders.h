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

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_set>

namespace facebook::velox::ucx_exchange {

/// Records which output destinations are known to read device memory.
///
/// A producer cannot tell from its own plan who will read its output. The
/// task's last operator is its last operator whether the consumer is another
/// worker's exchange or the coordinator over HTTP, so stage position says
/// nothing about what the reader can accept.
///
/// What does say something is the reader itself. Every consumer has to ask for
/// data, and a consumer that asks over a transport carrying device memory
/// identifies itself by doing so. That is recorded here, against the
/// destination it asked for, and the page consults it when it is read rather
/// than when it is written.
///
/// Absence is the safe answer: a destination nobody has claimed reads host
/// bytes, which is what an HTTP consumer needs.
///
/// Thread-safe.
class DevicePageReaders {
 public:
  static DevicePageReaders& instance();

  /// Records that this destination's consumer reads device memory. Called when
  /// its request arrives, which is always before its pages are rendered.
  void record(const std::string& taskId, int destination);

  /// Whether this destination's consumer has identified itself as reading
  /// device memory.
  bool accepts(const std::string& taskId, int destination) const;

  /// Forgets everything. For tests that need a known starting point.
  void clear();

 private:
  static std::string key(const std::string& taskId, int destination);

  mutable std::mutex mutex_;
  std::unordered_set<std::string> readers_;
};

} // namespace facebook::velox::ucx_exchange
