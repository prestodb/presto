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
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/common/base/Portability.h"
#include "velox/common/base/SimdUtil.h"

namespace facebook::velox {

namespace {
/// Returns the size of the previous free block. The size is stored in the
/// last 4 bytes of the free block, e.g. 4 bytes just before the current
/// header.
uint32_t* previousFreeSize(HashStringAllocator::Header* header) {
  return reinterpret_cast<uint32_t*>(header) - 1;
}

/// Returns the header of the previous free block or nullptr if previous block
/// is not free.
HashStringAllocator::Header* FOLLY_NULLABLE
getPreviousFree(HashStringAllocator::Header* FOLLY_NONNULL header) {
  if (!header->isPreviousFree()) {
    return nullptr;
  }
  auto numBytes = *previousFreeSize(header);
  auto previous = reinterpret_cast<HashStringAllocator::Header*>(
      header->begin() - numBytes - 2 * sizeof(HashStringAllocator::Header));
  VELOX_CHECK_EQ(previous->size(), numBytes);
  VELOX_CHECK(previous->isFree());
  VELOX_CHECK(!previous->isPreviousFree());
  return previous;
}

/// Sets kFree flag in the 'header' and writes the size of the block to the
/// last 4 bytes of the block. Sets kPreviousFree flag in the next block's
/// 'header'.
void markAsFree(HashStringAllocator::Header* FOLLY_NONNULL header) {
  header->setFree();
  auto nextHeader = header->next();
  if (nextHeader) {
    nextHeader->setPreviousFree();
    *previousFreeSize(nextHeader) = header->size();
  }
}
} // namespace

HashStringAllocator::~HashStringAllocator() {
  for (auto& pair : allocationsFromPool_) {
    pool()->free(pair.first, pair.second);
  }
}

void* HashStringAllocator::allocateFromPool(size_t size) {
  auto ptr = pool()->allocate(size);
  cumulativeBytes_ += size;
  allocationsFromPool_[ptr] = size;
  sizeFromPool_ += size;
  return ptr;
}

void HashStringAllocator::freeToPool(void* ptr, size_t size) {
  auto it = allocationsFromPool_.find(ptr);
  VELOX_CHECK(
      it != allocationsFromPool_.end(),
      "freeToPool for block not allocated from pool of HashStringAllocator");
  VELOX_CHECK_EQ(
      size, it->second, "Bad size in HashStringAllocator::freeToPool()");
  allocationsFromPool_.erase(it);
  sizeFromPool_ -= size;
  cumulativeBytes_ -= size;
  pool()->free(ptr, size);
}

// static
void HashStringAllocator::prepareRead(const Header* begin, ByteStream& stream) {
  std::vector<ByteRange> ranges;
  auto header = const_cast<Header*>(begin);
  for (;;) {
    ranges.push_back(ByteRange{
        reinterpret_cast<uint8_t*>(header->begin()), header->usableSize(), 0});
    if (!header->isContinued()) {
      break;
    }
    header = header->nextContinued();
  }
  stream.resetInput(std::move(ranges));
}

HashStringAllocator::Position HashStringAllocator::newWrite(
    ByteStream& stream,
    int32_t preferredSize) {
  VELOX_CHECK(
      !currentHeader_,
      "Do not call newWrite before finishing the previous write to "
      "HashStringAllocator");
  currentHeader_ = allocate(preferredSize, false);

  stream.setRange(ByteRange{
      reinterpret_cast<uint8_t*>(currentHeader_->begin()),
      currentHeader_->size(),
      0});

  startPosition_ = Position::atOffset(currentHeader_, 0);

  return startPosition_;
}

void HashStringAllocator::extendWrite(Position position, ByteStream& stream) {
  auto header = position.header;
  const auto offset = position.offset();
  VELOX_CHECK_GE(
      offset, 0, "Starting extendWrite outside of the current range");
  VELOX_CHECK_LE(
      offset,
      header->usableSize(),
      "Starting extendWrite outside of the current range");

  if (header->isContinued()) {
    free(header->nextContinued());
    header->clearContinued();
  }

  stream.setRange(ByteRange{
      reinterpret_cast<uint8_t*>(position.position),
      static_cast<int32_t>(header->end() - position.position),
      0});
  currentHeader_ = header;
  startPosition_ = position;
}

std::pair<HashStringAllocator::Position, HashStringAllocator::Position>
HashStringAllocator::finishWrite(ByteStream& stream, int32_t numReserveBytes) {
  VELOX_CHECK(
      currentHeader_, "Must call newWrite or extendWrite before finishWrite");
  auto writePosition = stream.writePosition();
  const auto offset = writePosition - currentHeader_->begin();

  VELOX_CHECK_GE(
      offset, 0, "finishWrite called with writePosition out of range");
  VELOX_CHECK_LE(
      offset,
      currentHeader_->usableSize(),
      "finishWrite called with writePosition out of range");

  Position currentPosition = Position::atOffset(currentHeader_, offset);
  if (currentHeader_->isContinued()) {
    free(currentHeader_->nextContinued());
    currentHeader_->clearContinued();
  }
  // Free remainder of block if there is a lot left over.
  freeRestOfBlock(
      currentHeader_,
      writePosition - currentHeader_->begin() + numReserveBytes);
  currentHeader_ = nullptr;

  // The starting position may have shifted if it was at the end of the block
  // and the block was extended. Calculate the new position.
  if (startPosition_.header->isContinued()) {
    auto header = startPosition_.header;
    const auto offset = startPosition_.offset();
    const auto extra = offset - header->usableSize();
    if (extra > 0) {
      auto newHeader = header->nextContinued();
      auto newPosition = newHeader->begin() + extra;
      startPosition_ = {newHeader, newPosition};
    }
  }
  return {startPosition_, currentPosition};
}

void HashStringAllocator::newSlab(int32_t size) {
  constexpr int32_t kSimdPadding = simd::kPadding - sizeof(Header);
  char* run = nullptr;
  uint64_t available = 0;
  int32_t needed = std::max<int32_t>(
      bits::roundUp(
          size + 2 * sizeof(Header) + kSimdPadding,
          memory::AllocationTraits::kPageSize),
      kUnitSize);
  auto pagesNeeded = memory::AllocationTraits::numPages(needed);
  // All large allocations are made standalone in pool().
  VELOX_CHECK_LE(pagesNeeded, pool()->largestSizeClass());
  if (pool_.allocatedBytes() >= pool_.hugePageThreshold()) {
    needed = memory::AllocationTraits::kHugePageSize;
  }
  run = pool_.allocateFixed(needed);
  available = needed - sizeof(Header) - kSimdPadding;

  VELOX_CHECK_NOT_NULL(run);
  VELOX_CHECK_GT(available, 0);
  // Write end  marker.
  *reinterpret_cast<uint32_t*>(run + available) = Header::kArenaEnd;
  cumulativeBytes_ += available;

  // Add the new memory to the free list: Placement construct a header
  // that covers the space from start to the end marker and add this
  // to free list.
  free(new (run) Header(available - sizeof(Header)));
}

void HashStringAllocator::newRange(
    int32_t bytes,
    ByteRange* range,
    bool contiguous) {
  // Allocates at least kMinContiguous or to the end of the current
  // run. At the end of the write the unused space will be made
  // free.
  VELOX_CHECK(
      currentHeader_,
      "Must have called newWrite or extendWrite before newRange");
  auto newHeader = allocate(bytes, contiguous);

  auto lastWordPtr = reinterpret_cast<void**>(
      currentHeader_->end() - Header::kContinuedPtrSize);
  *reinterpret_cast<void**>(newHeader->begin()) = *lastWordPtr;
  *lastWordPtr = newHeader;
  currentHeader_->setContinued();
  currentHeader_ = newHeader;
  *range = ByteRange{
      reinterpret_cast<uint8_t*>(currentHeader_->begin()),
      currentHeader_->size(),
      Header::kContinuedPtrSize};
}

void HashStringAllocator::newRange(int32_t bytes, ByteRange* range) {
  newRange(bytes, range, false);
}

void HashStringAllocator::newContiguousRange(int32_t bytes, ByteRange* range) {
  newRange(bytes, range, true);
}

// static
StringView HashStringAllocator::contiguousString(
    StringView view,
    std::string& storage) {
  if (view.isInline()) {
    return view;
  }
  auto header = headerOf(view.data());
  if (view.size() <= header->size()) {
    return view;
  }

  ByteStream stream;
  prepareRead(headerOf(view.data()), stream);
  storage.resize(view.size());
  stream.readBytes(storage.data(), view.size());
  return StringView(storage);
}

void HashStringAllocator::freeRestOfBlock(Header* header, int32_t keepBytes) {
  keepBytes = std::max(keepBytes, kMinAlloc);
  int32_t freeSize = header->size() - keepBytes - sizeof(Header);
  if (freeSize <= kMinAlloc) {
    return;
  }

  header->setSize(keepBytes);
  auto newHeader = new (header->end()) Header(freeSize);
  free(newHeader);
}

// Free list sizes align with size of containers. + 20 allows for padding for an
// alignment of 16 bytes.
int32_t HashStringAllocator::freeListSizes_[kNumFreeLists + 1] = {
    72,
    8 * 16 + 20,
    16 * 16 + 20,
    32 * 16 + 20,
    64 * 16 + 20,
    128 * 16 + 20,
    std::numeric_limits<int32_t>::max(),
    std::numeric_limits<int32_t>::max()};

int32_t HashStringAllocator::freeListIndex(int32_t size, uint32_t mask) {
  static_assert(sizeof(freeListSizes_) >= sizeof(xsimd::batch<int32_t>));
  auto vsize = xsimd::broadcast(size);
  if constexpr (sizeof(freeListSizes_) == sizeof(xsimd::batch<int32_t>)) {
    auto sizes = xsimd::load_unaligned(freeListSizes_);
    auto bits = simd::toBitMask(vsize < sizes) & mask;
    return count_trailing_zeros(bits);
  } else {
    int offset = 0;
    for (;;) {
      auto sizes = xsimd::load_unaligned(freeListSizes_ + offset);
      auto bits = simd::toBitMask(vsize < sizes) & mask;
      if (bits) {
        return offset + count_trailing_zeros(bits);
      }
      offset += xsimd::batch<int32_t>::size;
      mask >>= xsimd::batch<int32_t>::size;
    }
  }
}

HashStringAllocator::Header* FOLLY_NULLABLE
HashStringAllocator::allocate(int32_t size, bool exactSize) {
  if (size > kMaxAlloc && exactSize) {
    VELOX_CHECK(size <= Header::kSizeMask);
    auto header =
        reinterpret_cast<Header*>(allocateFromPool(size + sizeof(Header)));
    new (header) Header(size);
    return header;
  }
  auto header = allocateFromFreeLists(size, exactSize, exactSize);
  if (!header) {
    newSlab(size);
    header = allocateFromFreeLists(size, exactSize, exactSize);
    VELOX_CHECK(header != nullptr);
    VELOX_CHECK_GT(header->size(), 0);
  }

  return header;
}

HashStringAllocator::Header* FOLLY_NULLABLE
HashStringAllocator::allocateFromFreeLists(
    int32_t preferredSize,
    bool mustHaveSize,
    bool isFinalSize) {
  preferredSize = std::max(kMinAlloc, preferredSize);
  if (!numFree_) {
    return nullptr;
  }
  auto index = freeListIndex(preferredSize, freeNonEmpty_);
  while (index < kNumFreeLists) {
    if (auto header = allocateFromFreeList(
            preferredSize, mustHaveSize, isFinalSize, index)) {
      return header;
    }
    // Go to the next larger size non-empty free list.
    index = count_trailing_zeros(freeNonEmpty_ & ~bits::lowMask(index + 1));
  }
  if (mustHaveSize) {
    return nullptr;
  }
  index = freeListIndex(preferredSize) - 1;
  for (; index >= 0; --index) {
    if (auto header =
            allocateFromFreeList(preferredSize, false, isFinalSize, index)) {
      return header;
    }
  }
  return nullptr;
}

HashStringAllocator::Header* FOLLY_NULLABLE
HashStringAllocator::allocateFromFreeList(
    int32_t preferredSize,
    bool mustHaveSize,
    bool isFinalSize,
    int32_t freeListIndex) {
  constexpr int32_t kMaxCheckedForFit = 5;
  int32_t counter = 0;
  Header* largest = nullptr;
  Header* found = nullptr;
  for (auto* item = free_[freeListIndex].next(); item != &free_[freeListIndex];
       item = item->next()) {
    auto header = headerOf(item);
    VELOX_CHECK(header->isFree());
    auto size = header->size();
    if (size >= preferredSize) {
      found = header;
      break;
    }
    if (!largest || size > largest->size()) {
      largest = header;
    }
    if (!mustHaveSize && ++counter > kMaxCheckedForFit) {
      break;
    }
  }
  if (!mustHaveSize && !found) {
    found = largest;
  }
  if (!found) {
    return nullptr;
  }

  --numFree_;
  freeBytes_ -= found->size() + sizeof(Header);
  removeFromFreeList(found);

  auto next = found->next();
  if (next) {
    next->clearPreviousFree();
  }
  cumulativeBytes_ += found->size();
  if (isFinalSize) {
    freeRestOfBlock(found, preferredSize);
  }
  return found;
}

void HashStringAllocator::free(Header* _header) {
  Header* header = _header;
  if (header->size() > kMaxAlloc && !pool_.isInCurrentRange(header) &&
      allocationsFromPool_.find(header) != allocationsFromPool_.end()) {
    // A large free can either be a rest of block or a standalone allocation.
    VELOX_CHECK(!header->isContinued());
    freeToPool(header, header->size() + sizeof(Header));
    return;
  }

  do {
    Header* continued = nullptr;
    if (header->isContinued()) {
      continued = header->nextContinued();
      header->clearContinued();
    }
    VELOX_CHECK(!header->isFree());
    freeBytes_ += header->size() + sizeof(Header);
    cumulativeBytes_ -= header->size();
    Header* next = header->next();
    if (next) {
      VELOX_CHECK(!next->isPreviousFree());
      if (next->isFree()) {
        --numFree_;
        removeFromFreeList(next);
        header->setSize(header->size() + next->size() + sizeof(Header));
        next = reinterpret_cast<Header*>(header->end());
        VELOX_CHECK(next->isArenaEnd() || !next->isFree());
      }
    }
    if (header->isPreviousFree()) {
      auto previousFree = getPreviousFree(header);
      removeFromFreeList(previousFree);
      previousFree->setSize(
          previousFree->size() + header->size() + sizeof(Header));

      header = previousFree;
    } else {
      ++numFree_;
    }
    auto freeIndex = freeListIndex(header->size());
    freeNonEmpty_ |= 1 << freeIndex;
    free_[freeIndex].insert(
        reinterpret_cast<CompactDoubleList*>(header->begin()));
    markAsFree(header);
    header = continued;
  } while (header);
}

// static
int64_t HashStringAllocator::offset(
    Header* FOLLY_NONNULL header,
    Position position) {
  static const int64_t kOutOfRange = -1;
  if (!position.isSet()) {
    return kOutOfRange;
  }

  int64_t size = 0;
  for (;;) {
    assert(header);
    const auto length = header->usableSize();
    const auto offset = position.position - header->begin();
    if (offset >= 0 && offset <= length) {
      return size + offset;
    }
    if (!header->isContinued()) {
      return kOutOfRange;
    }
    size += length;
    header = header->nextContinued();
  }
}

// static
HashStringAllocator::Position HashStringAllocator::seek(
    Header* FOLLY_NONNULL header,
    int64_t offset) {
  int64_t size = 0;
  for (;;) {
    assert(header);
    auto length = header->usableSize();
    if (offset <= size + length) {
      return Position::atOffset(header, offset - size);
    }
    if (!header->isContinued()) {
      return Position::null();
    }
    size += length;
    header = header->nextContinued();
  }
}

// static
int64_t HashStringAllocator::available(const Position& position) {
  auto header = position.header;
  const auto startOffset = position.offset();
  // startOffset bytes from the first block are already used.
  int64_t size = -startOffset;
  for (;;) {
    assert(header);
    size += header->usableSize();
    if (!header->isContinued()) {
      return size;
    }
    header = header->nextContinued();
  }
}

void HashStringAllocator::ensureAvailable(int32_t bytes, Position& position) {
  if (available(position) >= bytes) {
    return;
  }

  ByteStream stream(this);
  extendWrite(position, stream);
  static char data[128];
  while (bytes) {
    auto written = std::min<size_t>(bytes, sizeof(data));
    stream.append(folly::StringPiece(data, written));
    bytes -= written;
  }
  position = finishWrite(stream, 0).first;
}

void HashStringAllocator::checkConsistency() const {
  uint64_t numFree = 0;
  uint64_t freeBytes = 0;
  for (auto i = 0; i < pool_.numRanges(); ++i) {
    auto topRange = pool_.rangeAt(i);
    const auto kHugePageSize = memory::AllocationTraits::kHugePageSize;
    auto topRangeSize = topRange.size();
    if (topRangeSize >= kHugePageSize) {
      VELOX_CHECK_EQ(0, topRangeSize % kHugePageSize);
    }
    // Some ranges are short and contain one arena. Some are multiples of huge
    // page size and contain one arena per huge page.
    for (int64_t subRangeStart = 0; subRangeStart < topRangeSize;
         subRangeStart += kHugePageSize) {
      auto range = folly::Range<char*>(
          topRange.data() + subRangeStart,
          std::min<int64_t>(topRangeSize, kHugePageSize));
      auto size = range.size() - simd::kPadding;
      bool previousFree = false;
      auto end = reinterpret_cast<Header*>(range.data() + size);
      auto header = reinterpret_cast<Header*>(range.data());
      while (header != end) {
        VELOX_CHECK_GE(reinterpret_cast<char*>(header), range.data());
        VELOX_CHECK_LT(
            reinterpret_cast<char*>(header), reinterpret_cast<char*>(end));
        VELOX_CHECK_LE(
            reinterpret_cast<char*>(header->end()),
            reinterpret_cast<char*>(end));
        VELOX_CHECK_EQ(header->isPreviousFree(), previousFree);

        if (header->isFree()) {
          VELOX_CHECK(!previousFree);
          VELOX_CHECK(!header->isContinued());
          if (header->next()) {
            VELOX_CHECK_EQ(
                header->size(),
                *(reinterpret_cast<int32_t*>(header->end()) - 1));
          }
          ++numFree;
          freeBytes += sizeof(Header) + header->size();
        } else if (header->isContinued()) {
          // If the content of the header is continued, check the
          // continue header is readable and not free.
          auto continued = header->nextContinued();
          VELOX_CHECK(!continued->isFree());
        }
        previousFree = header->isFree();
        header = reinterpret_cast<Header*>(header->end());
      }
    }
  }

  VELOX_CHECK_EQ(numFree, numFree_);
  VELOX_CHECK_EQ(freeBytes, freeBytes_);
  uint64_t numInFreeList = 0;
  uint64_t bytesInFreeList = 0;
  for (auto i = 0; i < kNumFreeLists; ++i) {
    bool hasData = freeNonEmpty_ & (1 << i);
    bool listNonEmpty = !free_[i].empty();
    VELOX_CHECK_EQ(hasData, listNonEmpty);
    for (auto free = free_[i].next(); free != &free_[i]; free = free->next()) {
      ++numInFreeList;
      auto size = headerOf(free)->size();
      if (i > 0) {
        VELOX_CHECK_GE(size, freeListSizes_[i - 1]);
      }
      VELOX_CHECK_LT(size, freeListSizes_[i]);
      bytesInFreeList += size + sizeof(Header);
    }
  }

  VELOX_CHECK_EQ(numInFreeList, numFree_);
  VELOX_CHECK_EQ(bytesInFreeList, freeBytes_);
}

} // namespace facebook::velox
