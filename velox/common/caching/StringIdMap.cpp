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

#include "velox/common/caching/StringIdMap.h"

namespace facebook::velox {

uint64_t StringIdMap::id(std::string_view string) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = stringToId_.find(string);
  if (it != stringToId_.end()) {
    return it->second;
  }
  return kNoId;
}

void StringIdMap::release(uint64_t id) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = idToEntry_.find(id);
  if (it != idToEntry_.end()) {
    VELOX_CHECK_LT(
        0, it->second.numInUse, "Extra release of id in StringIdMap");
    if (--it->second.numInUse == 0) {
      pinnedSize_ -= it->second.string.size();
      auto strIter = stringToId_.find(it->second.string);
      VELOX_DCHECK(strIter != stringToId_.end());
      stringToId_.erase(strIter);
      idToEntry_.erase(it);
    }
  }
}

void StringIdMap::addReference(uint64_t id) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = idToEntry_.find(id);
  VELOX_CHECK(
      it != idToEntry_.end(),
      "Trying to add a reference to id {} that is not in StringIdMap",
      id);

  ++it->second.numInUse;
}

uint64_t StringIdMap::makeId(std::string_view string) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = stringToId_.find(string);
  if (it != stringToId_.end()) {
    auto entry = idToEntry_.find(it->second);
    VELOX_CHECK(entry != idToEntry_.end());
    if (++entry->second.numInUse == 1) {
      pinnedSize_ += entry->second.string.size();
    }
    return it->second;
  }
  Entry entry;
  entry.string = string;
  // Check that we do not use an id twice. In practice this never
  // happens because the int64 counter would have to wrap around for
  // this. Even if this happened, the time spent in the loop would
  // have a low cap since the number of mappings would in practice
  // be in the 100K range.
  do {
    entry.id = ++lastId_;
  } while (idToEntry_.find(entry.id) != idToEntry_.end());
  entry.numInUse = 1;
  pinnedSize_ += string.size();
  const auto id = entry.id;
  idToEntry_[id] = std::move(entry);
  stringToId_[string] = id;
  return lastId_;
}

uint64_t StringIdMap::recoverId(uint64_t id, std::string_view string) {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = stringToId_.find(string);
  if (it != stringToId_.end()) {
    VELOX_CHECK_EQ(
        id, it->second, "Multiple recover ids assigned to {}", string);
    auto entry = idToEntry_.find(it->second);
    VELOX_CHECK(entry != idToEntry_.end());
    if (++entry->second.numInUse == 1) {
      pinnedSize_ += entry->second.string.size();
    }
    return id;
  }

  VELOX_CHECK_EQ(
      idToEntry_.count(id),
      0,
      "Reused recover id {} assigned to {}",
      id,
      string);

  Entry entry;
  entry.string = string;
  entry.id = id;
  lastId_ = std::max(lastId_, id);
  entry.numInUse = 1;
  pinnedSize_ += string.size();
  idToEntry_[id] = std::move(entry);
  stringToId_[string] = id;
  return id;
}
} // namespace facebook::velox
