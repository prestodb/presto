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

#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/memory/AllocationPool.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/CompactDoubleList.h"
#include "velox/common/memory/StreamArena.h"
#include "velox/type/StringView.h"

namespace facebook::velox {

// Implements an arena backed by MappedMemory::Allocation. This is for backing
// ByteStream or for allocating single blocks. Blocks can be individually freed.
// Adjacent frees are coalesced and free blocks are kept in a free list.
// Allocated blocks are prefixed with a Header. This has a size and flags.
// kContinue means that last 8 bytes are a pointer to another Header after which
// the contents of this allocation continue. kFree means the block is free. A
// free block has pointers to the next and previous free block via a
// CompactDoubleList struct immediately after the header. The last 4 bytes of a
// free block contain its length. kPreviousFree means that the block immediately
// below is free. In this case the uint32_t below the header has the size of the
// previous free block. The last word of a MappedMemory::PageRun backing a
// HashStringAllocator is set to kArenaEnd.
class HashStringAllocator : public StreamArena {
 public:
  // The minimum allocation must have space after the header for the
  // free list pointers and the trailing length.
  static constexpr int32_t kMinAlloc =
      sizeof(CompactDoubleList) + sizeof(uint32_t);

  class Header {
   public:
    static constexpr uint32_t kFree = 1U << 31;
    static constexpr uint32_t kContinued = 1U << 30;
    static constexpr uint32_t kPreviousFree = 1U << 29;
    static constexpr uint32_t kSizeMask = (1U << 29) - 1;

    // Marker at end of a PageRun. Distinct from valid headers since
    // all the 3 high bits are set, which is not valid for a header.
    static constexpr uint32_t kArenaEnd = 0xf0aeab0d;

    explicit Header(uint32_t size) : data_(size) {
      VELOX_CHECK(size <= kSizeMask);
    }

    bool isContinued() const {
      return (data_ & kContinued) != 0;
    }

    bool isFree() const {
      return (data_ & kFree) != 0;
    }

    bool isPreviousFree() const {
      return (data_ & kPreviousFree) != 0;
    }

    void setContinued() {
      data_ |= kContinued;
    }

    void setFree() {
      data_ |= kFree;
    }

    void setPreviousFree() {
      data_ |= kPreviousFree;
    }

    void clearContinued() {
      data_ &= ~kContinued;
    }

    void clearFree() {
      data_ &= ~kFree;
    }

    void clearPreviousFree() {
      data_ &= ~kPreviousFree;
    }

    bool isArenaEnd() const {
      return data_ == kArenaEnd;
    }

    int32_t size() const {
      return data_ & kSizeMask;
    }

    void setSize(int32_t size) {
      VELOX_CHECK(size <= kSizeMask);
      data_ = size | (data_ & ~kSizeMask);
    }

    char* FOLLY_NONNULL begin() {
      return reinterpret_cast<char*>(this + 1);
    }

    char* FOLLY_NONNULL end() {
      return begin() + size();
    }

    // Returns Header of the next block or null if at the end of arena.
    Header* FOLLY_NULLABLE next() {
      auto next = reinterpret_cast<Header*>(end());
      return next->data_ == kArenaEnd ? nullptr : next;
    }

   private:
    uint32_t data_;
  };

  struct Position {
    Header* FOLLY_NULLABLE header;
    char* FOLLY_NULLABLE position;
  };

  explicit HashStringAllocator(memory::MappedMemory* FOLLY_NONNULL mappedMemory)
      : StreamArena(mappedMemory),
        pool_(mappedMemory, AllocationPool::kHashTableOwner) {}

  // Copies a StringView at 'offset' in 'group' to storage owned by
  // the hash table. Updates the StringView.
  void copy(char* FOLLY_NONNULL group, int32_t offset) {
    StringView* string = reinterpret_cast<StringView*>(group + offset);
    if (string->isInline()) {
      return;
    }
    auto data = pool_.allocateFixed(string->size());
    memcpy(data, string->data(), string->size());
    *string = StringView(data, string->size());
  }

  // Copies a StringView at 'offset' in 'group' to storage owned by
  // 'this'. Updates the StringView. A large string may be copied into
  // non-contiguous allocation pieces. The size in the StringView is
  // the sum of the sizes. The pieces are linked via Headers, the
  // first header is below the first byte of the StringView's
  // data. StringViews written by this are to be read with
  // contiguousString(). This is nearly always zero copy but will
  // accommodate the odd extra large string.
  void copyMultipart(char* FOLLY_NONNULL group, int32_t offset) {
    auto string = reinterpret_cast<StringView*>(group + offset);
    if (string->isInline()) {
      return;
    }
    auto numBytes = string->size();

    // Write the string as non-contiguous chunks.
    ByteStream stream(this, false, false);
    auto position = newWrite(stream, numBytes);
    stream.appendStringPiece(folly::StringPiece(string->data(), numBytes));
    finishWrite(stream, 0);

    // The stringView has a pointer to the first byte and the total
    // size. Read with contiguousString().
    *string = StringView(reinterpret_cast<char*>(position.position), numBytes);
  }

  // Returns a contiguous view on 'view', where 'view' comes from
  // copyMultipart(). Uses 'storage' to own a possible temporary
  // copy. Making a temporary copy only happens for non-contiguous
  // strings.
  static StringView contiguousString(StringView view, std::string& storage);

  // Allocates 'size' contiguous bytes preceded by a Header. Returns
  // the address of Header.
  Header* FOLLY_NONNULL allocate(int32_t size) {
    VELOX_CHECK(
        !currentHeader_, "Do not call allocate() when a write is in progress");
    return allocate(std::max(size, kMinAlloc), true);
  }

  // Returns the header immediately below 'data'.
  static Header* FOLLY_NONNULL headerOf(const void* FOLLY_NONNULL data) {
    return reinterpret_cast<Header*>(
               const_cast<char*>(reinterpret_cast<const char*>(data))) -
        1;
  }

  // Sets 'stream' to range over the data in the range of 'header' and
  // possible continuation ranges.
  static void prepareRead(
      const Header* FOLLY_NONNULL header,
      ByteStream& stream);

  // Returns the number of payload bytes between 'header->begin()' and
  // 'position'.
  static int64_t offset(Header* FOLLY_NONNULL header, Position position);

  // Returns a position 'offset' bytes after 'header->begin()'.
  static Position seek(Header* FOLLY_NONNULL header, int64_t offset);

  // Returns the number of bytes that can be written starting at 'position'
  // without allocating more space.
  static int64_t available(const Position& position);

  // Ensures that one can write at least 'bytes' data starting at
  // 'position' without allocating more space. 'position' can be
  // changed but will logically point at the same data. Data to the
  // right of 'position is not preserved.
  void ensureAvailable(int32_t bytes, Position& position);

  // Sets stream to write to this pool. The write can span multiple
  // non-contiguous runs. Each contiguous run will have at least
  // kMinContiguous bytes of contiguous space. finishWrite finalizes
  // the allocation information after the write is done.
  // Returns the position at the start of the allocated block.
  Position newWrite(ByteStream& stream, int32_t preferredSize = kMinContiguous);

  // Sets 'stream' to write starting at 'position'. If new ranges have to
  // be allocated when writing, headers will be updated accordingly.
  void extendWrite(Position position, ByteStream& stream);

  // Completes a write prepared with newWrite or
  // extendWrite. Up to 'numReserveBytes' unused bytes, if available, are left
  // after the end of the write to accommodate another write. Returns the
  // position immediately after the last written byte.
  Position finishWrite(ByteStream& stream, int32_t numReserveBytes);

  // Allocates a new range for a stream writing to 'this'. Sets the last word of
  // the previous range to point to the new range and copies the overwritten
  // word as the first word of the new range.
  void newRange(int32_t bytes, ByteRange* FOLLY_NONNULL range) override;

  void newTinyRange(int32_t bytes, ByteRange* FOLLY_NONNULL range) override {
    newRange(bytes, range);
  }

  // Returns the total memory footprint of 'this'.
  int64_t retainedSize() const {
    return pool_.allocatedBytes();
  }

  // Adds the allocation of 'header' and any extensions (if header has
  // kContinued set) to the free list.
  void free(Header* FOLLY_NONNULL header);

  // Returns a lower bound on bytes available without growing
  // 'this'. This is the sum of free block sizes minus size of pointer
  // for each. We subtract the pointer because in the worst case we
  // would have one allocation that chains many small free blocks
  // together via kContinued.
  uint64_t freeSpace() const {
    int64_t minFree = freeBytes_ - numFree_ * (sizeof(Header) + sizeof(void*));
    VELOX_CHECK_GE(minFree, 0, "Guaranteed free space cannot be negative");
    return minFree;
  }

  // Frees all memory associated with 'this' and leaves 'this' ready for reuse.
  void clear() {
    numFree_ = 0;
    freeBytes_ = 0;
    new (&free_) CompactDoubleList();
    pool_.clear();
  }

  memory::MappedMemory* FOLLY_NONNULL mappedMemory() const {
    return pool_.mappedMemory();
  }

  uint64_t cumulativeBytes() const {
    return cumulativeBytes_;
  }

  // Checks the free space accounting and consistency of
  // Headers. Throws when detects corruption.
  void checkConsistency() const;

 private:
  static constexpr int32_t kUnitSize = 16 * memory::MappedMemory::kPageSize;
  static constexpr int32_t kMinContiguous = 48;

  // Adds 'bytes' worth of contiguous space to the free list. This
  // grows the footprint in MappedMemory but does not allocate
  // anything yet. Throws if fails to grow. The caller typically knows
  // a cap on memory to allocate and uses this and freeSpace() to make
  // sure that there is space to accommodate the expected need before
  // starting to process a batch of input.
  void newSlab(int32_t size);

  void removeFromFreeList(Header* FOLLY_NONNULL header) {
    VELOX_CHECK(header->isFree());
    header->clearFree();
    reinterpret_cast<CompactDoubleList*>(header->begin())->remove();
  }

  /// Allocates a block of specified size. If exactSize is false, the block may
  /// be smaller or larger. Checks free list before allocating new memory.
  Header* FOLLY_NULLABLE allocate(int32_t size, bool exactSize);

  // Allocates memory from free list. Returns nullptr if no memory in
  // free list, otherwise returns a header of a free block of some
  // size. if 'mustHaveSize' is true, the block will not be smaller
  // than 'preferredSize'. If 'isFinalSize' is true, this will not
  // return a block that is much larger than preferredSize. Otherwise,
  // the block can be larger and the user is expected to call
  // freeRestOfBlock to finalize the allocation.
  Header* FOLLY_NULLABLE allocateFromFreeList(
      int32_t preferredSize,
      bool mustHaveSize,
      bool isFinalSize);

  // Sets 'header' to be 'keepBytes' long and adds the remainder of
  // 'header's memory to free list. Does nothing if the resulting
  // blocks would be below minimum size.
  void freeRestOfBlock(Header* FOLLY_NONNULL header, int32_t keepBytes);

  // Circular list of free blocks.
  CompactDoubleList free_;

  // Count of elements in 'free_'. This is 0 when free_.next() == &free_.
  uint64_t numFree_ = 0;

  // Sum of the size of blocks in 'free_', excluding headers.
  uint64_t freeBytes_ = 0;

  // Counter of allocated bytes. The difference of two point in time values
  // tells how much memory has been consumed by activity between these points in
  // time. Incremented by allocation and decremented by free. Used for tracking
  // the row by row space usage in a RowContainer.
  uint64_t cumulativeBytes_{0};

  // Pointer to Header for the range being written. nullptr if a write is not in
  // progress.
  Header* FOLLY_NULLABLE currentHeader_ = nullptr;

  // Pool for getting new slabs.
  AllocationPool pool_;
};

// Utility for keeping track of allocation between two points in
// time. A counter on a row supplied at construction is incremented
// by the change in allocation between construction and
// destruction. This is a scoped guard to use around setting
// variable length data in a RowContainer or similar.
template <typename T, typename TCounter = uint32_t>
class RowSizeTracker {
 public:
  //  Will update the counter at pointer cast to TCounter*
  //  with the change in allocation during the lifetime of 'this'
  RowSizeTracker(T& counter, HashStringAllocator& allocator)
      : allocator_(allocator),
        size_(allocator_.cumulativeBytes()),
        counter_(counter) {}

  ~RowSizeTracker() {
    auto delta = allocator_.cumulativeBytes() - size_;
    if (delta) {
      saturatingIncrement(&counter_, delta);
    }
  }

 private:
  // Increments T at *pointer without wrapping around at overflow.
  void saturatingIncrement(T* FOLLY_NONNULL pointer, int64_t delta) {
    auto value = *reinterpret_cast<TCounter*>(pointer) + delta;
    *reinterpret_cast<TCounter*>(pointer) =
        std::min<uint64_t>(value, std::numeric_limits<TCounter>::max());
  }

  HashStringAllocator& allocator_;
  const uint64_t size_;
  T& counter_;
};

// An Allocator based by HashStringAllocator to use with STL containers.
template <class T>
struct StlAllocator {
  using value_type = T;

  explicit StlAllocator(HashStringAllocator* FOLLY_NONNULL allocator)
      : allocator_{allocator} {
    VELOX_CHECK(allocator);
  }

  template <class U>
  explicit StlAllocator(const StlAllocator<U>& allocator)
      : allocator_{allocator.allocator()} {
    VELOX_CHECK(allocator_);
  }

  T* FOLLY_NONNULL allocate(std::size_t n) {
    return reinterpret_cast<T*>(
        allocator_->allocate(checkedMultiply(n, sizeof(T)))->begin());
  }

  void deallocate(T* FOLLY_NONNULL p, std::size_t /*n*/) noexcept {
    allocator_->free(HashStringAllocator::headerOf(p));
  }

  HashStringAllocator* FOLLY_NONNULL allocator() const {
    return allocator_;
  }

  friend bool operator==(const StlAllocator& lhs, const StlAllocator& rhs) {
    return lhs.allocator_ == rhs.allocator_;
  }

  friend bool operator!=(const StlAllocator& lhs, const StlAllocator& rhs) {
    return !(lhs == rhs);
  }

 private:
  HashStringAllocator* FOLLY_NONNULL allocator_;
};

// An allocator backed by HashStringAllocator that guaratees a configurable
// alignment. The alignment must be a power of 2 and not be 0. This allocator
// can be used with folly F14 containers that requires 16-bytes alignment.
template <class T, uint8_t Alignment>
struct AlignedStlAllocator {
  using value_type = T;

  static_assert(
      Alignment != 0,
      "Alignment of AlignmentStlAllocator cannot be 0.");
  static_assert(
      (Alignment & (Alignment - 1)) == 0,
      "Alignment of AlignmentStlAllocator must be a power of 2.");

  template <class Other>
  struct rebind {
    using other = AlignedStlAllocator<Other, Alignment>;
  };

  explicit AlignedStlAllocator(HashStringAllocator* FOLLY_NONNULL allocator)
      : allocator_{allocator} {
    VELOX_CHECK(allocator);
  }

  template <class U, uint8_t A>
  explicit AlignedStlAllocator(const AlignedStlAllocator<U, A>& allocator)
      : allocator_{allocator.allocator()} {
    VELOX_CHECK(allocator_);
  }

  T* FOLLY_NONNULL allocate(std::size_t n) {
    // Allocate extra Alignment bytes for alignment and 4 bytes to store the
    // delta between unaligned and aligned pointers.
    auto size =
        checkedPlus<size_t>(Alignment + 4, checkedMultiply(n, sizeof(T)));
    auto ptr = reinterpret_cast<T*>(allocator_->allocate(size)->begin());

    // Align 'ptr + 4'.
    void* alignedPtr = (char*)ptr + 4;
    size -= 4;
    std::align(Alignment, n * sizeof(T), alignedPtr, size);

    // Write alignment delta just before the aligned pointer.
    int32_t delta = (char*)alignedPtr - (char*)ptr - 4;
    *reinterpret_cast<int32_t*>((char*)alignedPtr - 4) = delta;

    return reinterpret_cast<T*>(alignedPtr);
  }

  void deallocate(T* FOLLY_NONNULL p, std::size_t /*n*/) noexcept {
    auto delta = *reinterpret_cast<int32_t*>((char*)p - 4);
    allocator_->free(HashStringAllocator::headerOf((char*)p - 4 - delta));
  }

  HashStringAllocator* FOLLY_NONNULL allocator() const {
    return allocator_;
  }

  friend bool operator==(
      const AlignedStlAllocator& lhs,
      const AlignedStlAllocator& rhs) {
    return lhs.allocator_ == rhs.allocator_;
  }

  friend bool operator!=(
      const AlignedStlAllocator& lhs,
      const AlignedStlAllocator& rhs) {
    return !(lhs == rhs);
  }

 private:
  HashStringAllocator* FOLLY_NONNULL allocator_;
};

} // namespace facebook::velox
