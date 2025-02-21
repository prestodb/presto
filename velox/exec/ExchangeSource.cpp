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
#include "velox/exec/ExchangeSource.h"

namespace facebook::velox::exec {

std::shared_ptr<ExchangeSource> ExchangeSource::create(
    const std::string& remoteTaskId,
    int destination,
    std::shared_ptr<ExchangeQueue> queue,
    memory::MemoryPool* pool) {
  for (auto& factory : factories()) {
    auto result = factory(remoteTaskId, destination, queue, pool);
    if (result) {
      return result;
    }
  }
  VELOX_FAIL("No ExchangeSource factory matches {}", remoteTaskId);
}

// static
std::vector<ExchangeSource::Factory>& ExchangeSource::factories() {
  static std::vector<Factory> factories;
  return factories;
}

} // namespace facebook::velox::exec
