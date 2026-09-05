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

#include "velox/experimental/ucx-exchange/DevicePageReaders.h"

#include <fmt/format.h>

namespace facebook::velox::ucx_exchange {

// static
DevicePageReaders& DevicePageReaders::instance() {
  static DevicePageReaders readers;
  return readers;
}

// static
std::string DevicePageReaders::key(const std::string& taskId, int destination) {
  return fmt::format("{}#{}", taskId, destination);
}

void DevicePageReaders::record(const std::string& taskId, int destination) {
  std::lock_guard<std::mutex> lock(mutex_);
  readers_.insert(key(taskId, destination));
}

bool DevicePageReaders::accepts(const std::string& taskId, int destination)
    const {
  std::lock_guard<std::mutex> lock(mutex_);
  return readers_.count(key(taskId, destination)) != 0;
}

void DevicePageReaders::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  readers_.clear();
}

} // namespace facebook::velox::ucx_exchange
