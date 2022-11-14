/*
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
#include "presto_cpp/main/operators/ShuffleInterface.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
static std::unordered_map<std::string, std::unique_ptr<ShuffleManager>>
    kShuffleManagerMap;
}

void clearShuffleManagers() {
  kShuffleManagerMap.clear();
}

void registerShuffleManager(
    const std::string& name,
    std::unique_ptr<ShuffleManager>& manager) {
  if (kShuffleManagerMap.find(name) != kShuffleManagerMap.end()) {
    VELOX_FAIL(fmt::format("Shuffle manager {} already registered", name));
  }
  kShuffleManagerMap.insert({name, std::move(manager)});
}

std::unique_ptr<ShuffleInterface> createShuffleInstance(
    const ShuffleInfo& info,
    velox::memory::MemoryPool* pool) {
  auto& name = info.name();
  if (kShuffleManagerMap.find(name) == kShuffleManagerMap.end()) {
    VELOX_FAIL(fmt::format(
        "Unable for fine shuffle manager associated with {}", name));
  }
  auto manager = kShuffleManagerMap.find(name)->second.get();
  return manager->create(info, pool);
}
} // namespace facebook::presto::operators