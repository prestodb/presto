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

#include "velox/common/caching/DataCache.h"

#include <stdexcept>

namespace facebook::velox {

bool SimpleLRUDataCache::put(std::string_view key, std::string_view value) {
  std::lock_guard<std::mutex> l(mu_);
  std::string* cacheValue = new std::string(value.data(), value.size());
  std::string cacheKey(key.data(), key.size());
  const int64_t size = key.size() + value.size() + 2 * sizeof(std::string);
  const bool inserted = cache_.add(std::move(cacheKey), cacheValue, size);
  if (!inserted) {
    delete cacheValue;
  }
  return inserted;
}

bool SimpleLRUDataCache::get(std::string_view key, uint64_t size, void* buf) {
  std::lock_guard<std::mutex> l(mu_);
  const std::string cacheKey(key.data(), key.size());
  std::string* cacheValue = cache_.get(cacheKey);
  if (cacheValue) {
    const bool success = cacheValue->size() == size;
    if (success) {
      memcpy(buf, cacheValue->data(), size);
    }
    cache_.release(cacheKey);
    return success;
  }
  return false;
}

bool SimpleLRUDataCache::get(std::string_view key, std::string* value) {
  std::lock_guard<std::mutex> l(mu_);
  const std::string cacheKey(key.data(), key.size());
  std::string* cacheValue = cache_.get(cacheKey);
  if (cacheValue) {
    *value = *cacheValue;
    cache_.release(cacheKey);
    return true;
  }
  return false;
}

} // namespace facebook::velox
