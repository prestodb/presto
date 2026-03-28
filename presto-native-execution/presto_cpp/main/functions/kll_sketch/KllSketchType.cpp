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

#include "presto_cpp/main/functions/kll_sketch/KllSketchType.h"
#include <unordered_map>
#include "velox/common/base/Exceptions.h"

namespace facebook::presto {

namespace {
// Cache for KllSketchType instances to avoid creating duplicates
struct KllSketchTypeCache {
  std::unordered_map<velox::TypePtr, std::shared_ptr<const KllSketchType>>
      cache;
  std::mutex mutex;
};

KllSketchTypeCache& getCache() {
  static KllSketchTypeCache cache;
  return cache;
}
} // namespace

std::shared_ptr<const KllSketchType> KllSketchType::get(
    const velox::TypePtr& dataType) {
  VELOX_CHECK_NOT_NULL(dataType, "KllSketch data type cannot be null");

  // Validate supported types
  VELOX_CHECK(
      dataType->isDouble() || dataType->isBigint() || dataType->isVarchar() ||
          dataType->isBoolean(),
      "KllSketch only supports DOUBLE, BIGINT, VARCHAR, and BOOLEAN types, got: {}",
      dataType->toString());

  auto& cache = getCache();
  std::lock_guard<std::mutex> lock(cache.mutex);

  auto it = cache.cache.find(dataType);
  if (it != cache.cache.end()) {
    return it->second;
  }

  auto type = std::shared_ptr<const KllSketchType>(new KllSketchType(dataType));
  cache.cache[dataType] = type;
  return type;
}

} // namespace facebook::presto
