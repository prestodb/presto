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

#include "velox/experimental/wave/common/GpuArena.h"
#include <sstream>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/experimental/wave/common/Exception.h"

namespace facebook::velox::wave {

uint64_t GpuSlab::roundBytes(uint64_t bytes) {
  return bits::nextPowerOfTwo(std::max<int64_t>(16, bytes));
}

GpuSlab::GpuSlab(void* ptr, size_t capacityBytes, GpuAllocator* allocator)
    : address_(reinterpret_cast<uint8_t*>(ptr)),
      byteSize_(capacityBytes),
      allocator_(allocator) {
  addFreeBlock(reinterpret_cast<uint64_t>(address_), byteSize_);
  freeBytes_ = byteSize_;
}

GpuSlab::~GpuSlab() {
  allocator_->free(address_, byteSize_);
}

void* GpuSlab::allocate(uint64_t bytes) {
  if (bytes == 0) {
    return nullptr;
  }
  bytes = roundBytes(bytes);

  // First match in the list that can give this many bytes
  auto lookupItr = freeLookup_.lower_bound(bytes);
  if (lookupItr == freeLookup_.end()) {
    VELOX_WAVE_LOG_EVERY_MS(WARNING, 1000)
        << "Cannot find a free block that is large enough to allocate " << bytes
        << " bytes. Current arena freeBytes " << freeBytes_ << " & lookup table"
        << freeLookupStr();
    return nullptr;
  }

  freeBytes_ -= bytes;
  auto address = *(lookupItr->second.begin());
  auto curFreeBytes = lookupItr->first;
  void* result = reinterpret_cast<void*>(address);
  if (curFreeBytes == bytes) {
    removeFreeBlock(address, curFreeBytes);
    return result;
  }
  addFreeBlock(address + bytes, curFreeBytes - bytes);
  removeFreeBlock(address, curFreeBytes);
  return result;
}

void GpuSlab::free(void* address, uint64_t bytes) {
  if (address == nullptr || bytes == 0) {
    return;
  }
  bytes = roundBytes(bytes);
  freeBytes_ += bytes;

  const auto curAddr = reinterpret_cast<uint64_t>(address);
  auto curIter = addFreeBlock(curAddr, bytes);
  auto prevIter = freeList_.end();
  uint64_t prevAddr;
  uint64_t prevBytes;
  bool mergePrev = false;
  if (curIter != freeList_.begin()) {
    prevIter = std::prev(curIter);
    prevAddr = prevIter->first;
    prevBytes = prevIter->second;
    auto prevEndAddr = prevAddr + prevBytes;
    VELOX_CHECK_LE(
        prevEndAddr,
        curAddr,
        "New free node (addr:{} size:{}) overlaps with previous free node (addr:{} size:{}) in free list",
        curAddr,
        bytes,
        prevAddr,
        prevBytes);
    mergePrev = prevEndAddr == curAddr;
  }

  auto nextItr = std::next(curIter);
  uint64_t nextAddr;
  uint64_t nextBytes;
  bool mergeNext = false;
  if (nextItr != freeList_.end()) {
    nextAddr = nextItr->first;
    nextBytes = nextItr->second;
    auto curEndAddr = curAddr + bytes;
    VELOX_CHECK_LE(
        curEndAddr,
        nextAddr,
        "New free node (addr:{} size:{}) overlaps with next free node (addr:{} size:{}) in free list",
        curAddr,
        bytes,
        nextAddr,
        nextBytes);
    mergeNext = (curEndAddr == nextAddr);
  }

  if (!mergePrev && !mergeNext) {
    return;
  }

  if (mergePrev) {
    removeFreeBlock(curIter);
    removeFromLookup(prevAddr, prevBytes);
    auto newFreeSize = curAddr - prevAddr + bytes;
    if (mergeNext) {
      removeFreeBlock(nextItr);
      newFreeSize = nextAddr - prevAddr + nextBytes;
    }
    freeList_[prevIter->first] = newFreeSize;
    freeLookup_[newFreeSize].emplace(prevAddr);
    return;
  }

  if (mergeNext) {
    VELOX_DCHECK(!mergePrev);
    removeFreeBlock(nextItr);
    removeFromLookup(curAddr, bytes);
    const auto newFreeSize = nextAddr - curAddr + nextBytes;
    freeList_[curIter->first] = newFreeSize;
    freeLookup_[newFreeSize].emplace(curAddr);
  }
}

void GpuSlab::removeFromLookup(uint64_t addr, uint64_t bytes) {
  freeLookup_[bytes].erase(addr);
  if (freeLookup_[bytes].empty()) {
    freeLookup_.erase(bytes);
  }
}

std::map<uint64_t, uint64_t>::iterator GpuSlab::addFreeBlock(
    uint64_t address,
    uint64_t bytes) {
  auto insertResult = freeList_.emplace(address, bytes);
  VELOX_CHECK(
      insertResult.second,
      "Trying to free a memory space that is already freed. Already in free list address {} size {}. Attempted to free address {} size {}",
      address,
      freeList_[address],
      address,
      bytes);
  freeLookup_[bytes].emplace(address);
  return insertResult.first;
}

void GpuSlab::removeFreeBlock(uint64_t addr, uint64_t bytes) {
  freeList_.erase(addr);
  removeFromLookup(addr, bytes);
}

void GpuSlab::removeFreeBlock(std::map<uint64_t, uint64_t>::iterator& iter) {
  removeFromLookup(iter->first, iter->second);
  freeList_.erase(iter);
}

bool GpuSlab::checkConsistency() const {
  uint64_t numErrors = 0;
  auto arenaEndAddress = reinterpret_cast<uint64_t>(address_) + byteSize_;
  auto iter = freeList_.begin();
  auto end = freeList_.end();
  int64_t freeListTotalBytes = 0;
  while (iter != end) {
    // Lookup list should contain the address
    auto freeLookupIter = freeLookup_.find(iter->second);
    if (freeLookupIter == freeLookup_.end() ||
        freeLookupIter->second.find(iter->first) ==
            freeLookupIter->second.end()) {
      LOG(WARNING)
          << "GpuSlab::checkConsistency(): freeLookup_ out of sync: Not "
             "containing item from freeList_ {addr:"
          << iter->first << ", size:" << iter->second << "}";
      numErrors++;
    }

    // Verify current free block end
    auto blockEndAddress = iter->first + iter->second;
    if (blockEndAddress > arenaEndAddress) {
      LOG(WARNING)
          << "GpuSlab::checkConsistency(): freeList_ out of sync: Block "
             "extruding arena boundary {addr:"
          << iter->first << ", size:" << iter->second << "}";
      numErrors++;
    }

    // Verify next free block not overlapping
    auto next = std::next(iter);
    if (next != end && blockEndAddress > next->first) {
      LOG(ERROR)
          << "GpuSlab::checkConsistency(): freeList_ out of sync: Overlapping"
             " blocks {addr:"
          << iter->first << ", size:" << iter->second
          << "} {addr:" << next->first << ", size:" << next->second << "}";
      numErrors++;
    }

    freeListTotalBytes += iter->second;
    iter++;
  }

  // Check consistency of lookup list
  int64_t freeLookupTotalBytes = 0;
  for (auto freeIter = freeLookup_.begin(); freeIter != freeLookup_.end();
       freeIter++) {
    if (freeIter->second.empty()) {
      LOG(ERROR)
          << "GpuSlab::checkConsistency(): freeLookup_ out of sync: Empty "
             "address list for size "
          << freeIter->first;
      numErrors++;
    }
    freeLookupTotalBytes += (freeIter->first * freeIter->second.size());
  }

  // Check consistency of freeList_ and freeLookup_ in terms of bytes
  if (freeListTotalBytes != freeLookupTotalBytes ||
      freeListTotalBytes != freeBytes_) {
    LOG(WARNING)
        << "GpuSlab::checkConsistency(): free bytes out of sync: freeListTotalBytes "
        << freeListTotalBytes << " freeLookupTotalBytes "
        << freeLookupTotalBytes << " freeBytes_ " << freeBytes_;
    numErrors++;
  }

  if (numErrors) {
    LOG(ERROR) << "GpuSlab::checkConsistency(): " << numErrors << " errors";
  }
  return numErrors == 0;
}

std::string GpuSlab::freeLookupStr() {
  std::stringstream lookupStr;
  for (auto itr = freeLookup_.begin(); itr != freeLookup_.end(); ++itr) {
    lookupStr << "\n{" << itr->first << "->[";
    for (auto itrInner = itr->second.begin(); itrInner != itr->second.end();
         itrInner++) {
      lookupStr << *itrInner << ", ";
    }
    lookupStr << "]}\n";
  }
  return lookupStr.str();
}

std::string GpuSlab::toString() const {
  return fmt::format(
      "GpuSlab[byteSize[{}] address[{}] freeBytes[{}] freeList[{}]]]",
      succinctBytes(byteSize_),
      reinterpret_cast<uint64_t>(address_),
      succinctBytes(freeBytes_),
      freeList_.size());
}

GpuArena::Buffers::Buffers() {
  for (auto i = 0; i < sizeof(buffers) / sizeof(buffers[0]); ++i) {
    new (&buffers[i]) Buffer();
  }
}

GpuArena::GpuArena(uint64_t singleArenaCapacity, GpuAllocator* allocator)
    : singleArenaCapacity_(singleArenaCapacity), allocator_(allocator) {
  auto arena = std::make_shared<GpuSlab>(
      allocator_->allocate(singleArenaCapacity),
      singleArenaCapacity,
      allocator_);
  arenas_.emplace(reinterpret_cast<uint64_t>(arena->address()), arena);
  currentArena_ = arena;
}

WaveBufferPtr GpuArena::getBuffer(void* ptr, size_t size) {
  auto result = firstFreeBuffer_;
  if (!result) {
    allBuffers_.push_back(std::make_unique<Buffers>());
    auto* buffers = allBuffers_.back().get();
    for (int32_t i = (sizeof(*buffers) / sizeof(Buffer)) - 1; i >= 0; --i) {
      buffers->buffers[i].ptr_ = firstFreeBuffer_;
      firstFreeBuffer_ = &buffers->buffers[i];
    }
    result = firstFreeBuffer_;
  }
  firstFreeBuffer_ = reinterpret_cast<Buffer*>(result->ptr_);
  new (result) Buffer();
  result->arena_ = this;
  result->ptr_ = ptr;
  result->size_ = size;
  result->capacity_ = size;
  return result;
}

WaveBufferPtr GpuArena::allocateBytes(uint64_t bytes) {
  bytes = GpuSlab::roundBytes(bytes);
  std::lock_guard<std::mutex> l(mutex_);
  auto* result = currentArena_->allocate(bytes);
  if (result != nullptr) {
    return getBuffer(result, bytes);
  }
  for (auto pair : arenas_) {
    if (pair.second == currentArena_ || pair.second->freeBytes() < bytes) {
      continue;
    }
    result = pair.second->allocate(bytes);
    if (result) {
      currentArena_ = pair.second;
      return getBuffer(result, bytes);
    }
  }

  // If first allocation fails we create a new GpuSlab for another attempt. If
  // it ever fails again then it means requested bytes is larger than a single
  // GpuSlab's capacity. No further attempts will happen.
  auto arenaBytes = std::max<uint64_t>(singleArenaCapacity_, bytes);
  auto newArena = std::make_shared<GpuSlab>(
      allocator_->allocate(arenaBytes), arenaBytes, allocator_);
  arenas_.emplace(reinterpret_cast<uint64_t>(newArena->address()), newArena);
  currentArena_ = newArena;
  result = currentArena_->allocate(bytes);
  if (result) {
    return getBuffer(result, bytes);
  }
  VELOX_FAIL("Failed to allocate {} bytes of universal address space", bytes);
}

void GpuArena::free(Buffer* buffer) {
  const uint64_t addressU64 = reinterpret_cast<uint64_t>(buffer->ptr_);
  VELOX_CHECK_EQ(0, buffer->referenceCount_);
  VELOX_CHECK_EQ(0, buffer->pinCount_);
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!arenas_.empty());

  auto iter = arenas_.lower_bound(addressU64);
  if (iter == arenas_.end() || iter->first != addressU64) {
    VELOX_CHECK(iter != arenas_.begin());
    --iter;
    VELOX_CHECK_GE(
        iter->first + singleArenaCapacity_, addressU64 + buffer->size_);
  }
  iter->second->free(buffer->ptr_, buffer->size_);
  if (iter->second->empty() && iter->second != currentArena_) {
    arenas_.erase(iter);
  }
  buffer->ptr_ = firstFreeBuffer_;
  buffer->size_ = 0;
  buffer->capacity_ = 0;
  firstFreeBuffer_ = buffer;
}

} // namespace facebook::velox::wave
