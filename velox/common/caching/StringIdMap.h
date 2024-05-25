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

#include <string_view>

#include <folly/container/F14Map.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

class StringIdMap {
 public:
  static constexpr uint64_t kNoId = ~0UL;

  StringIdMap() = default;

  StringIdMap(const StringIdMap& other) = delete;
  StringIdMap(StringIdMap&& other) = delete;
  void operator=(const StringIdMap& other) = delete;
  void operator=(StringIdMap&& other) = delete;

  // Returns the id of 'string' or kNoId if the string is not known.
  uint64_t id(std::string_view string);

  // Returns the total length of strings involved in currently referenced
  // mappings.
  int64_t pinnedSize() const {
    return pinnedSize_;
  }

  // Returns the id for 'string' and increments its use count. Assigns a
  // new id if none exists. Must be released with release() when no longer used.
  uint64_t makeId(std::string_view string);

  // Decrements the use count of id and may free the associated memory if no
  // uses remain.
  void release(uint64_t id);

  // Increments the use count of 'id'.
  void addReference(uint64_t id);

  // Returns a copy of the string associated with id or empty string if id has
  // no string.
  std::string string(uint64_t id) {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = idToEntry_.find(id);
    return it == idToEntry_.end() ? "" : it->second.string;
  }

  // Resets StringIdMap.
  void testingReset() {
    std::lock_guard<std::mutex> l(mutex_);
    stringToId_.clear();
    idToEntry_.clear();
    lastId_ = 0;
    pinnedSize_ = 0;
  }

 private:
  struct Entry {
    std::string string;
    uint64_t id;
    uint32_t numInUse{};
  };

  std::mutex mutex_;
  folly::F14FastMap<std::string, uint64_t> stringToId_;
  folly::F14FastMap<uint64_t, Entry> idToEntry_;
  uint64_t lastId_{0};
  uint64_t pinnedSize_{0};
};

// Keeps a string-id association live for the duration of this.
class StringIdLease {
 public:
  StringIdLease() = default;

  // Makes a lease for 'string' and makes sure it has an id.
  StringIdLease(StringIdMap& ids, std::string_view string)
      : ids_(&ids), id_(ids_->makeId(string)) {}

  // Makes a new lease for an id that already references a string.
  StringIdLease(StringIdMap& ids, uint64_t id) : ids_(&ids), id_(id) {
    ids_->addReference(id_);
  }

  StringIdLease(const StringIdLease& other) {
    ids_ = other.ids_;
    id_ = other.id_;
    if (ids_ && id_ != StringIdMap::kNoId) {
      ids_->addReference(id_);
    }
  }

  StringIdLease(StringIdLease&& other) noexcept {
    ids_ = other.ids_;
    id_ = other.id_;
    other.ids_ = nullptr;
    other.id_ = StringIdMap::kNoId;
  }

  void operator=(const StringIdLease& other) {
    clear();
    ids_ = other.ids_;
    if (ids_ && other.id_ != StringIdMap::kNoId) {
      ids_->addReference(other.id_);
    }
    id_ = other.id_;
  }

  void operator=(StringIdLease&& other) noexcept {
    clear();
    ids_ = other.ids_;
    id_ = other.id_;
    other.ids_ = nullptr;
    other.id_ = StringIdMap::kNoId;
  }

  ~StringIdLease() {
    clear();
  }

  void clear() {
    if (ids_) {
      ids_->release(id_);
      ids_ = nullptr;
      id_ = StringIdMap::kNoId;
    }
  }

  bool hasValue() const {
    return id_ != StringIdMap::kNoId;
  }

  uint64_t id() const {
    return id_;
  }

 private:
  StringIdMap* ids_{nullptr};
  uint64_t id_{StringIdMap::kNoId};
};

} // namespace facebook::velox
