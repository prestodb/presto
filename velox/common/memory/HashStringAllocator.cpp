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
// Returns the size of the previous free block. The size is stored in the last 4
// bytes of the free block, e.g. 4 bytes just before the current header.
uint32_t* previousFreeSize(HashStringAllocator::Header* header) {
  return reinterpret_cast<uint32_t*>(header) - 1;
}

// Returns the header of the previous free block or nullptr if previous block is
// not free.
HashStringAllocator::Header* getPreviousFree(
    HashStringAllocator::Header* header) {
  if (!header->isPreviousFree()) {
    return nullptr;
  }
  const auto numBytes = *previousFreeSize(header);
  auto* previous = reinterpret_cast<HashStringAllocator::Header*>(
      header->begin() - numBytes - 2 * sizeof(HashStringAllocator::Header));
  VELOX_CHECK_EQ(previous->size(), numBytes);
  VELOX_CHECK(previous->isFree());
  VELOX_CHECK(!previous->isPreviousFree());
  return previous;
}

// Sets kFree flag in the 'header' and writes the size of the block to the last
// 4 bytes of the block. Sets kPreviousFree flag in the next block's 'header'.
void markAsFree(HashStringAllocator::Header* header) {
  header->setFree();
  auto* nextHeader = header->next();
  if (nextHeader != nullptr) {
    nextHeader->setPreviousFree();
    *previousFreeSize(nextHeader) = header->size();
  }
}
} // namespace

std::string HashStringAllocator::Header::toString() {
  std::ostringstream out;
  if (isFree()) {
    out << "|free| ";
  }
  if (isContinued()) {
    out << "|multipart| ";
  }
  out << "size: " << size();
  if (isContinued()) {
    auto next = nextContinued();
    out << " [" << next->size();
    while (next->isContinued()) {
      next = next->nextContinued();
      out << ", " << next->size();
    }
    out << "]";
  }
  if (isPreviousFree()) {
    out << ", previous is free (" << *previousFreeSize(this) << " bytes)";
  }
  if (next() == nullptr) {
    out << ", at end";
  }
  return out.str();
}

HashStringAllocator::~HashStringAllocator() {
  clear();
}

void HashStringAllocator::clear() {
  state_.numFree() = 0;
  state_.freeBytes() = 0;
  std::fill(
      std::begin(state_.freeNonEmpty()), std::end(state_.freeNonEmpty()), 0);
  for (auto& pair : state_.allocationsFromPool()) {
    const auto size = pair.second;
    pool()->free(pair.first, size);
    state_.sizeFromPool() -= size;
    state_.currentBytes() -= size;
  }
  state_.allocationsFromPool().clear();
  for (auto i = 0; i < kNumFreeLists; ++i) {
    new (&state_.freeLists()[i]) CompactDoubleList();
  }

#ifdef NDEBUG
  static const auto kHugePageSize = memory::AllocationTraits::kHugePageSize;
  for (auto i = 0; i < state_.pool().numRanges(); ++i) {
    const auto range = state_.pool().rangeAt(i);
    const auto rangeSize = range.size();
    if (rangeSize >= kHugePageSize) {
      VELOX_CHECK_EQ(0, rangeSize % kHugePageSize);
    }

    for (int64_t blockOffset = 0; blockOffset < rangeSize;
         blockOffset += kHugePageSize) {
      auto blockRange = folly::Range<char*>(
          range.data() + blockOffset,
          std::min<int64_t>(rangeSize, kHugePageSize));
      const auto size = blockRange.size() - simd::kPadding;
      auto* end = castToHeader(blockRange.data() + size);
      auto* header = castToHeader(blockRange.data());
      while (header != end) {
        VELOX_CHECK_GE(reinterpret_cast<char*>(header), blockRange.data());
        VELOX_CHECK_LT(
            reinterpret_cast<char*>(header), reinterpret_cast<char*>(end));
        VELOX_CHECK_LE(
            reinterpret_cast<char*>(header->end()),
            reinterpret_cast<char*>(end));

        // Continued block & Non-free block.
        if (!header->isFree()) {
          state_.currentBytes() -= blockBytes(header);
        }
        header = castToHeader(header->end());
      }
    }
  }

  VELOX_DCHECK_EQ(state_.currentBytes(), 0);
  VELOX_DCHECK_EQ(state_.sizeFromPool(), 0);
#endif
  state_.pool().clear();

  state_.currentBytes() = 0;
  state_.sizeFromPool() = 0;
}

void* HashStringAllocator::allocateFromPool(size_t size) {
  auto* ptr = pool()->allocate(size);
  state_.currentBytes() += size;
  state_.allocationsFromPool()[ptr] = size;
  state_.sizeFromPool() += size;
  return ptr;
}

void HashStringAllocator::freeToPool(void* ptr, size_t size) {
  auto it = state_.allocationsFromPool().find(ptr);
  VELOX_CHECK(
      it != state_.allocationsFromPool().end(),
      "freeToPool for block not allocated from pool of HashStringAllocator");
  VELOX_CHECK_EQ(
      size, it->second, "Bad size in HashStringAllocator::freeToPool()");
  state_.allocationsFromPool().erase(it);
  state_.sizeFromPool() -= size;
  state_.currentBytes() -= size;
  pool()->free(ptr, size);
}

// static
ByteInputStream HashStringAllocator::prepareRead(
    const Header* begin,
    size_t maxBytes) {
  std::vector<ByteRange> ranges;
  auto* header = const_cast<Header*>(begin);

  size_t totalBytes{0};
  for (;;) {
    ranges.push_back(ByteRange{
        reinterpret_cast<uint8_t*>(header->begin()), header->usableSize(), 0});
    totalBytes += ranges.back().size;
    if (!header->isContinued()) {
      break;
    }

    if (totalBytes >= maxBytes) {
      break;
    }

    header = header->nextContinued();
  }
  return ByteInputStream(std::move(ranges));
}

HashStringAllocator::Position HashStringAllocator::newWrite(
    ByteOutputStream& stream,
    int32_t preferredSize) {
  VELOX_CHECK_NULL(
      state_.currentHeader(),
      "Do not call newWrite before finishing the previous write to "
      "HashStringAllocator");
  state_.currentHeader() = allocate(preferredSize, false);

  stream.setRange(
      ByteRange{
          reinterpret_cast<uint8_t*>(state_.currentHeader()->begin()),
          state_.currentHeader()->size(),
          0},
      0);

  state_.startPosition() = Position::atOffset(state_.currentHeader(), 0);

  return state_.startPosition();
}

void HashStringAllocator::extendWrite(
    Position position,
    ByteOutputStream& stream) {
  auto* header = position.header;
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

  stream.setRange(
      ByteRange{
          reinterpret_cast<uint8_t*>(position.header->begin()),
          position.header->size(),
          static_cast<int32_t>(position.position - position.header->begin())},
      0);
  state_.currentHeader() = header;
  state_.startPosition() = position;
}

std::pair<HashStringAllocator::Position, HashStringAllocator::Position>
HashStringAllocator::finishWrite(
    ByteOutputStream& stream,
    int32_t numReserveBytes) {
  VELOX_CHECK_NOT_NULL(
      state_.currentHeader(),
      "Must call newWrite or extendWrite before finishWrite");
  auto* writePosition = stream.writePosition();
  const auto offset = writePosition - state_.currentHeader()->begin();

  VELOX_CHECK_GE(
      offset, 0, "finishWrite called with writePosition out of range");
  VELOX_CHECK_LE(
      offset,
      state_.currentHeader()->usableSize(),
      "finishWrite called with writePosition out of range");

  const Position currentPosition =
      Position::atOffset(state_.currentHeader(), offset);
  if (state_.currentHeader()->isContinued()) {
    free(state_.currentHeader()->nextContinued());
    state_.currentHeader()->clearContinued();
  }
  // Free remainder of block if there is a lot left over.
  freeRestOfBlock(
      state_.currentHeader(),
      writePosition - state_.currentHeader()->begin() + numReserveBytes);
  state_.currentHeader() = nullptr;

  // The starting position may have shifted if it was at the end of the block
  // and the block was extended. Calculate the new position.
  if (state_.startPosition().header->isContinued()) {
    auto* header = state_.startPosition().header;
    const auto offset = state_.startPosition().offset();
    const auto extra = offset - header->usableSize();
    if (extra > 0) {
      auto* newHeader = header->nextContinued();
      auto* newPosition = newHeader->begin() + extra;
      state_.startPosition() = {newHeader, newPosition};
    }
  }
  return {state_.startPosition(), currentPosition};
}

void HashStringAllocator::newSlab() {
  constexpr int32_t kSimdPadding = simd::kPadding - kHeaderSize;
  const int64_t needed =
      state_.pool().allocatedBytes() >= state_.pool().hugePageThreshold()
      ? memory::AllocationTraits::kHugePageSize
      : kUnitSize;
  auto* run = state_.pool().allocateFixed(needed);
  VELOX_CHECK_NOT_NULL(run);
  // We check we got exactly the requested amount. checkConsistency() depends on
  // slabs made here coinciding with ranges from AllocationPool::rangeAt().
  // Sometimes the last range can be several huge pages for severl huge page
  // sized arenas but checkConsistency() can interpret that.
  VELOX_CHECK_EQ(state_.pool().freeBytes(), 0);
  const auto available = needed - kHeaderSize - kSimdPadding;
  VELOX_CHECK_GT(available, 0);

  // Write end marker.
  *reinterpret_cast<uint32_t*>(run + available) = Header::kArenaEnd;
  state_.currentBytes() += available;

  // Add the new memory to the free list: Placement construct a header that
  // covers the space from start to the end marker and add this to free list.
  free(new (run) Header(available - kHeaderSize));
}

void HashStringAllocator::newRange(
    int32_t bytes,
    ByteRange* lastRange,
    ByteRange* range,
    bool contiguous) {
  // Allocates at least kMinContiguous or to the end of the current run. At the
  // end of the write, the unused space will be made free.
  VELOX_CHECK_NOT_NULL(
      state_.currentHeader(),
      "Must have called newWrite or extendWrite before newRange");
  auto* newHeader = allocate(bytes, contiguous);

  // Copy the last word of the current range to the head of new range, and then
  // used the space to store the new range pointer.
  auto** lastWordPtr = reinterpret_cast<void**>(
      state_.currentHeader()->end() - Header::kContinuedPtrSize);
  *reinterpret_cast<void**>(newHeader->begin()) = *lastWordPtr;
  *lastWordPtr = newHeader;
  state_.currentHeader()->setContinued();
  state_.currentHeader() = newHeader;
  if (lastRange) {
    // The last bytes of the last range are no longer payload. So do not count
    // them in size and do not overwrite them if overwriting the multi-range
    // entry. Set position at the new end.
    lastRange->size -= Header::kContinuedPtrSize;
    lastRange->position = std::min(lastRange->size, lastRange->position);
  }
  *range = ByteRange{
      reinterpret_cast<uint8_t*>(state_.currentHeader()->begin()),
      state_.currentHeader()->size(),
      Header::kContinuedPtrSize};
}

void HashStringAllocator::newRange(
    int32_t bytes,
    ByteRange* lastRange,
    ByteRange* range) {
  newRange(bytes, lastRange, range, false);
}

void HashStringAllocator::newContiguousRange(int32_t bytes, ByteRange* range) {
  newRange(bytes, nullptr, range, true);
}

// static
StringView HashStringAllocator::contiguousString(
    StringView view,
    std::string& storage) {
  if (view.isInline()) {
    return view;
  }
  auto* header = headerOf(view.data());
  if (view.size() <= header->size()) {
    return view;
  }

  auto stream = prepareRead(headerOf(view.data()));
  storage.resize(view.size());
  stream.readBytes(storage.data(), view.size());
  return StringView(storage);
}

void HashStringAllocator::freeRestOfBlock(Header* header, int32_t keepBytes) {
  keepBytes = std::max(keepBytes, kMinAlloc);
  const int32_t freeSize = header->size() - keepBytes - kHeaderSize;
  if (freeSize <= kMinAlloc) {
    return;
  }

  header->setSize(keepBytes);
  auto* newHeader = new (header->end()) Header(freeSize);
  free(newHeader);
}

int32_t HashStringAllocator::freeListIndex(int size) {
  return std::min(size - kMinAlloc, kNumFreeLists - 1);
}

void HashStringAllocator::removeFromFreeList(Header* header) {
  VELOX_CHECK(header->isFree());
  header->clearFree();
  const auto index = freeListIndex(header->size());
  reinterpret_cast<CompactDoubleList*>(header->begin())->remove();
  if (state_.freeLists()[index].empty()) {
    bits::clearBit(state_.freeNonEmpty(), index);
  }
}

HashStringAllocator::Header* HashStringAllocator::allocate(
    int32_t size,
    bool exactSize) {
  if (size > kMaxAlloc && exactSize) {
    VELOX_CHECK_LE(size, Header::kSizeMask);
    auto* header = castToHeader(allocateFromPool(size + kHeaderSize));
    new (header) Header(size);
    return header;
  }

  auto* header = allocateFromFreeLists(size, exactSize, exactSize);
  if (header == nullptr) {
    newSlab();
    header = allocateFromFreeLists(size, exactSize, exactSize);
    VELOX_CHECK_NOT_NULL(header);
    VELOX_CHECK_GT(header->size(), 0);
  }
  return header;
}

HashStringAllocator::Header* HashStringAllocator::allocateFromFreeLists(
    int32_t preferredSize,
    bool mustHaveSize,
    bool isFinalSize) {
  if (state_.numFree() == 0) {
    return nullptr;
  }
  preferredSize = std::max(kMinAlloc, preferredSize);
  const auto index = freeListIndex(preferredSize);
  auto available =
      bits::findFirstBit(state_.freeNonEmpty(), index, kNumFreeLists);
  if (!mustHaveSize && available == -1) {
    available = bits::findLastBit(state_.freeNonEmpty(), 0, index);
  }
  if (available == -1) {
    return nullptr;
  }
  auto* header =
      allocateFromFreeList(preferredSize, mustHaveSize, isFinalSize, available);
  VELOX_CHECK_NOT_NULL(header);
  return header;
}

HashStringAllocator::Header* HashStringAllocator::allocateFromFreeList(
    int32_t preferredSize,
    bool mustHaveSize,
    bool isFinalSize,
    int32_t freeListIndex) {
  auto* item = state_.freeLists()[freeListIndex].next();
  if (item == &state_.freeLists()[freeListIndex]) {
    return nullptr;
  }
  auto* found = headerOf(item);
  VELOX_CHECK(
      found->isFree() && (!mustHaveSize || found->size() >= preferredSize));
  --state_.numFree();
  state_.freeBytes() -= blockBytes(found);
  removeFromFreeList(found);
  auto* next = found->next();
  if (next != nullptr) {
    next->clearPreviousFree();
  }
  state_.currentBytes() += blockBytes(found);
  if (isFinalSize) {
    freeRestOfBlock(found, preferredSize);
  }
  return found;
}

void HashStringAllocator::free(Header* header) {
  Header* headerToFree = header;
  do {
    Header* continued = nullptr;
    if (headerToFree->isContinued()) {
      continued = headerToFree->nextContinued();
      headerToFree->clearContinued();
    }
    if (headerToFree->size() > kMaxAlloc &&
        !state_.pool().isInCurrentRange(headerToFree) &&
        state_.allocationsFromPool().find(headerToFree) !=
            state_.allocationsFromPool().end()) {
      freeToPool(headerToFree, headerToFree->size() + kHeaderSize);
    } else {
      VELOX_CHECK(!headerToFree->isFree());
      state_.freeBytes() += blockBytes(headerToFree);
      state_.currentBytes() -= blockBytes(headerToFree);
      Header* next = headerToFree->next();
      if (next != nullptr) {
        VELOX_CHECK(!next->isPreviousFree());
        if (next->isFree()) {
          --state_.numFree();
          removeFromFreeList(next);
          headerToFree->setSize(
              headerToFree->size() + next->size() + kHeaderSize);
          next = castToHeader(headerToFree->end());
          VELOX_CHECK(next->isArenaEnd() || !next->isFree());
        }
      }
      if (headerToFree->isPreviousFree()) {
        auto* previousFree = getPreviousFree(headerToFree);
        removeFromFreeList(previousFree);
        previousFree->setSize(
            previousFree->size() + headerToFree->size() + kHeaderSize);

        headerToFree = previousFree;
      } else {
        ++state_.numFree();
      }
      const auto freedSize = headerToFree->size();
      const auto freeIndex = freeListIndex(freedSize);
      bits::setBit(state_.freeNonEmpty(), freeIndex);
      state_.freeLists()[freeIndex].insert(
          reinterpret_cast<CompactDoubleList*>(headerToFree->begin()));
      markAsFree(headerToFree);
    }
    headerToFree = continued;
  } while (headerToFree != nullptr);
}

// static
int64_t HashStringAllocator::offset(Header* header, Position position) {
  static const int64_t kOutOfRange = -1;
  if (!position.isSet()) {
    return kOutOfRange;
  }

  int64_t size = 0;
  for (;;) {
    VELOX_CHECK_NOT_NULL(header);
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
    Header* header,
    int64_t offset) {
  int64_t size = 0;
  for (;;) {
    VELOX_CHECK_NOT_NULL(header);
    const auto length = header->usableSize();
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
  auto* header = position.header;
  const auto startOffset = position.offset();
  // startOffset bytes from the first block are already used.
  int64_t size = -startOffset;
  for (;;) {
    VELOX_CHECK_NOT_NULL(header);
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

  ByteOutputStream stream(this);
  extendWrite(position, stream);
  static char data[128];
  while (bytes > 0) {
    const auto written = std::min<size_t>(bytes, sizeof(data));
    stream.append(folly::StringPiece(data, written));
    bytes -= written;
  }
  position = finishWrite(stream, 0).first;
}

inline bool HashStringAllocator::storeStringFast(
    const char* bytes,
    int32_t numBytes,
    char* destination) {
  const auto roundedBytes = std::max(numBytes, kMinAlloc);

  Header* header = nullptr;
  if (state_.freeLists()[kNumFreeLists - 1].empty()) {
    if (roundedBytes >= kMaxAlloc) {
      return false;
    }
    const auto index = freeListIndex(roundedBytes);
    const auto available =
        bits::findFirstBit(state_.freeNonEmpty(), index, kNumFreeLists);
    if (available < 0) {
      return false;
    }
    header = allocateFromFreeList(roundedBytes, true, true, available);
    VELOX_CHECK_NOT_NULL(header);
  } else {
    auto& freeList = state_.freeLists()[kNumFreeLists - 1];
    header = headerOf(freeList.next());
    const auto spaceTaken = roundedBytes + kHeaderSize;
    if (spaceTaken > header->size()) {
      return false;
    }
    if (header->size() - spaceTaken > kMaxAlloc) {
      // The entry after allocation stays in the largest free list.
      // The size at the end of the block is changed in place.
      reinterpret_cast<int32_t*>(header->end())[-1] -= spaceTaken;
      auto* freeHeader = new (header->begin() + roundedBytes)
          Header(header->size() - spaceTaken);
      freeHeader->setFree();
      header->clearFree();
      ::memcpy(freeHeader->begin(), header->begin(), sizeof(CompactDoubleList));
      freeList.nextMoved(
          reinterpret_cast<CompactDoubleList*>(freeHeader->begin()));
      header->setSize(roundedBytes);
      state_.freeBytes() -= spaceTaken;
      state_.currentBytes() += spaceTaken;
    } else {
      header =
          allocateFromFreeList(roundedBytes, true, true, kNumFreeLists - 1);
      if (!header) {
        return false;
      }
    }
  }

  simd::memcpy(header->begin(), bytes, numBytes);
  *reinterpret_cast<StringView*>(destination) =
      StringView(reinterpret_cast<char*>(header->begin()), numBytes);
  return true;
}

void HashStringAllocator::copyMultipartNoInline(
    const StringView& srcStr,
    char* group,
    int32_t offset) {
  const auto numBytes = srcStr.size();
  if (storeStringFast(srcStr.data(), numBytes, group + offset)) {
    return;
  }
  // Write the string as non-contiguous chunks.
  ByteOutputStream stream(this, false, false);
  auto position = newWrite(stream, numBytes);
  stream.appendStringView(srcStr);
  finishWrite(stream, 0);

  // The stringView has a pointer to the first byte and the total
  // size. Read with contiguousString().
  *reinterpret_cast<StringView*>(group + offset) =
      StringView(reinterpret_cast<char*>(position.position), numBytes);
}

std::string HashStringAllocator::toString() const {
  std::ostringstream out;

  out << "allocated: " << state_.currentBytes() << " bytes" << std::endl;
  out << "free: " << state_.freeBytes() << " bytes in " << state_.numFree()
      << " blocks" << std::endl;
  out << "standalone allocations: " << state_.sizeFromPool() << " bytes in "
      << state_.allocationsFromPool().size() << " allocations" << std::endl;
  out << "ranges: " << state_.pool().numRanges() << std::endl;

  static const auto kHugePageSize = memory::AllocationTraits::kHugePageSize;

  for (auto i = 0; i < state_.pool().numRanges(); ++i) {
    auto topRange = state_.pool().rangeAt(i);
    auto topRangeSize = topRange.size();

    out << "range " << i << ": " << topRangeSize << " bytes" << std::endl;

    // Some ranges are short and contain one arena. Some are multiples of huge
    // page size and contain one arena per huge page.
    for (int64_t subRangeStart = 0; subRangeStart < topRangeSize;
         subRangeStart += kHugePageSize) {
      auto range = folly::Range<char*>(
          topRange.data() + subRangeStart,
          std::min<int64_t>(topRangeSize, kHugePageSize));
      auto size = range.size() - simd::kPadding;

      auto end = castToHeader(range.data() + size);
      auto header = castToHeader(range.data());
      while (header != nullptr && header != end) {
        out << "\t" << header->toString() << std::endl;
        header = header->next();
      }
    }
  }

  return out.str();
}

int64_t HashStringAllocator::checkConsistency() const {
  static const auto kHugePageSize = memory::AllocationTraits::kHugePageSize;

  uint64_t numFree = 0;
  uint64_t freeBytes = 0;
  int64_t allocatedBytes = 0;
  for (auto i = 0; i < state_.pool().numRanges(); ++i) {
    auto topRange = state_.pool().rangeAt(i);
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
      const auto size = range.size() - simd::kPadding;
      bool previousFree = false;
      auto* end = castToHeader(range.data() + size);
      auto* header = castToHeader(range.data());
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
          if (header->next() != nullptr) {
            VELOX_CHECK_EQ(
                header->size(),
                *(reinterpret_cast<int32_t*>(header->end()) - 1));
          }
          ++numFree;
          freeBytes += blockBytes(header);
        } else if (header->isContinued()) {
          // If the content of the header is continued, check the continued
          // header is readable and not free.
          auto* continued = header->nextContinued();
          VELOX_CHECK(!continued->isFree());
          allocatedBytes += blockBytes(header);
        } else {
          allocatedBytes += blockBytes(header);
        }
        previousFree = header->isFree();
        header = castToHeader(header->end());
      }
    }
  }

  VELOX_CHECK_EQ(numFree, state_.numFree());
  VELOX_CHECK_EQ(freeBytes, state_.freeBytes());
  uint64_t numInFreeList = 0;
  uint64_t bytesInFreeList = 0;
  for (auto i = 0; i < kNumFreeLists; ++i) {
    const bool hasData = bits::isBitSet(state_.freeNonEmpty(), i);
    const bool listNonEmpty = !state_.freeLists()[i].empty();
    VELOX_CHECK_EQ(hasData, listNonEmpty);
    for (auto* free = state_.freeLists()[i].next();
         free != &state_.freeLists()[i];
         free = free->next()) {
      ++numInFreeList;
      VELOX_CHECK(
          free->next()->previous() == free,
          "free list previous link inconsistent");
      const auto size = headerOf(free)->size();
      VELOX_CHECK_GE(size, kMinAlloc);
      if (size - kMinAlloc < kNumFreeLists - 1) {
        VELOX_CHECK_EQ(size - kMinAlloc, i);
      } else {
        VELOX_CHECK_GE(size - kMinAlloc, kNumFreeLists - 1);
      }
      bytesInFreeList += size + kHeaderSize;
    }
  }

  VELOX_CHECK_EQ(numInFreeList, state_.numFree());
  VELOX_CHECK_EQ(bytesInFreeList, state_.freeBytes());
  return allocatedBytes;
}

bool HashStringAllocator::isEmpty() const {
  return state_.sizeFromPool() == 0 && checkConsistency() == 0;
}

void HashStringAllocator::checkEmpty() const {
  VELOX_CHECK_EQ(0, state_.sizeFromPool());
  VELOX_CHECK_EQ(0, checkConsistency());
}

} // namespace facebook::velox
