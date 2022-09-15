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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <unordered_set>
#include "velox/common/memory/MappedMemory.h"

namespace facebook::velox::memory {

static constexpr uint64_t kMinGrainSizeBytes = 1024 * 1024;

class MmapArena {
 public:
  MmapArena(size_t capacityBytes);
  ~MmapArena();

  void* FOLLY_NULLABLE allocate(uint64_t bytes);
  void free(void* FOLLY_NONNULL address, uint64_t bytes);
  void* FOLLY_NONNULL address() const {
    return reinterpret_cast<void*>(address_);
  }

  uint64_t byteSize() const {
    return byteSize_;
  }

  const std::map<uint64_t, uint64_t>& freeList() const {
    return freeList_;
  }

  const std::map<uint64_t, std::unordered_set<uint64_t>>& freeLookup() const {
    return freeLookup_;
  }

  uint64_t freeBytes() {
    return freeBytes_;
  }

  bool empty() {
    return freeBytes_ == byteSize_;
  }
  // Checks internal consistency of this MmapArena. Returns true if OK. May
  // return false if there are concurrent alocations and frees during the
  // consistency check. This is a false positive but not dangerous. This is for
  // test only
  bool checkConsistency() const;

  // translate lookup table to a string for debugging purpose only.
  std::string freeLookupStr() {
    std::stringstream lookupStr;
    for (auto itr = freeLookup_.begin(); itr != freeLookup_.end(); itr++) {
      lookupStr << "\n{" << itr->first << "->[";
      for (auto itrInner = itr->second.begin(); itrInner != itr->second.end();
           itrInner++) {
        lookupStr << *itrInner << ", ";
      }
      lookupStr << "]}\n";
    }
    return lookupStr.str();
  }

 private:
  // Rounds up size to the next power of 2.
  static uint64_t roundBytes(uint64_t bytes);

  std::map<uint64_t, uint64_t>::iterator addFreeBlock(
      uint64_t addr,
      uint64_t bytes);

  void removeFromLookup(uint64_t addr, uint64_t bytes);

  void removeFreeBlock(uint64_t addr, uint64_t bytes);

  void removeFreeBlock(std::map<uint64_t, uint64_t>::iterator& itr);

  // Starting address of this arena
  uint8_t* FOLLY_NONNULL address_;

  // Total capacity size of this arena
  const uint64_t byteSize_;

  std::atomic<uint64_t> freeBytes_;

  // A sorted list with each entry mapping from free block address to size of
  // the free block
  std::map<uint64_t, uint64_t> freeList_;

  // A sorted look up structure that stores the block size as key and a set of
  // addresses of that size as value.
  std::map<uint64_t, std::unordered_set<uint64_t>> freeLookup_;
};

/// A class that manages a set of MmapArenas. It is able to adapt itself by
/// growing the number of its managed MmapArena's when extreme memory
/// fragmentation happens.
class ManagedMmapArenas {
 public:
  ManagedMmapArenas(uint64_t singleArenaCapacity);

  void* FOLLY_NULLABLE allocate(uint64_t bytes);

  void free(void* FOLLY_NONNULL address, uint64_t bytes);

  const std::map<uint64_t, std::shared_ptr<MmapArena>>& arenas() const {
    return arenas_;
  }

 private:
  /// A sorted list of MmapArena by its initial address
  std::map<uint64_t, std::shared_ptr<MmapArena>> arenas_;

  /// All allocations should come from this MmapArena. When it is no longer able
  /// to handle allocations it will be updated to a newly created MmapArena.
  std::shared_ptr<MmapArena> currentArena_;

  /// Capacity in bytes for a single MmapArena managed by this.
  const uint64_t singleArenaCapacity_;
};

} // namespace facebook::velox::memory
