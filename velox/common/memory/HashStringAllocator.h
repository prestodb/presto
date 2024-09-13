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
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/StreamArena.h"
#include "velox/type/StringView.h"

#include <folly/container/F14Map.h>

namespace facebook::velox {

/// Implements an arena backed by memory::Allocation. This is for backing
/// ByteOutputStream or for allocating single blocks. Blocks can be individually
/// freed. Adjacent frees are coalesced and free blocks are kept in a free list.
/// Allocated blocks are prefixed with a Header. This has a size and flags.
/// kContinue means that last 8 bytes are a pointer to another Header after
/// which the contents of this allocation continue. kFree means the block is
/// free. A free block has pointers to the next and previous free block via a
/// CompactDoubleList struct immediately after the header. The last 4 bytes of a
/// free block contain its length. kPreviousFree means that the block
/// immediately below is free. In this case the uint32_t below the header has
/// the size of the previous free block. The last word of a Allocation::PageRun
/// backing a HashStringAllocator is set to kArenaEnd.
class HashStringAllocator : public StreamArena {
 public:
  /// The minimum allocation must have space after the header for the free list
  /// pointers and the trailing length.
  static constexpr int32_t kMinAlloc =
      sizeof(CompactDoubleList) + sizeof(uint32_t);

  /// Sizes larger than this will come direct from pool().
  static constexpr int32_t kMaxAlloc =
      memory::AllocationTraits::kPageSize / 4 * 3;

  class Header {
   public:
    static constexpr uint32_t kFree = 1U << 31;
    static constexpr uint32_t kContinued = 1U << 30;
    static constexpr uint32_t kPreviousFree = 1U << 29;
    static constexpr uint32_t kSizeMask = (1U << 29) - 1;
    static constexpr uint32_t kContinuedPtrSize = sizeof(void*);

    /// Marker at end of a PageRun. Distinct from valid headers since all the 3
    /// high bits are set, which is not valid for a header.
    static constexpr uint32_t kArenaEnd = 0xf0aeab0d;

    explicit Header(uint32_t size) : data_(size) {
      VELOX_CHECK_LE(size, kSizeMask);
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

    int32_t usableSize() const {
      return isContinued() ? (size() - kContinuedPtrSize) : size();
    }

    void setSize(int32_t size) {
      VELOX_CHECK_LE(size, kSizeMask);
      data_ = size | (data_ & ~kSizeMask);
    }

    char* begin() {
      return reinterpret_cast<char*>(this + 1);
    }

    char* end() {
      return begin() + size();
    }

    /// Returns the Header of the block that is physically next to this block or
    /// null if this is the last block of the arena.
    Header* next() {
      auto* next = castToHeader(end());
      return next->data_ == kArenaEnd ? nullptr : next;
    }

    /// Returns the header of the next block in a multi-part allocation. The
    /// caller must ensure that isContinued() returns true before calling this
    /// method.
    HashStringAllocator::Header* nextContinued() {
      VELOX_DCHECK(isContinued());
      return *reinterpret_cast<Header**>(end() - kContinuedPtrSize);
    }

    std::string toString();

   private:
    uint32_t data_;
  };

  struct Position {
    Header* header{nullptr};
    char* position{nullptr};

    int32_t offset() const {
      VELOX_DCHECK_NOT_NULL(header);
      VELOX_DCHECK_NOT_NULL(position);
      return position - header->begin();
    }

    bool isSet() const {
      return header != nullptr && position != nullptr;
    }

    static Position atOffset(Header* header, int32_t offset) {
      VELOX_DCHECK_NOT_NULL(header);
      VELOX_DCHECK_GE(offset, 0);
      VELOX_DCHECK_LE(offset, header->usableSize());
      return {header, header->begin() + offset};
    }

    static Position null() {
      return {nullptr, nullptr};
    }
  };

  explicit HashStringAllocator(memory::MemoryPool* pool)
      : StreamArena(pool), state_(pool) {}

  ~HashStringAllocator();

  // Copies the StringView 'srcStr' to storage owned by 'this'. Creates a new
  // StringView at 'offset' in 'group' pointing to the copy. A large string may
  // be copied into non-contiguous allocation pieces. The size in the StringView
  // is the sum of the sizes. The pieces are linked via Headers, the first
  // header is below the first byte of the StringView's data. StringViews
  // written by this are to be read with contiguousString(). This is nearly
  // always zero copy but will accommodate the odd extra large string.
  void copyMultipart(const StringView& str, char* group, int32_t offset) {
    if (str.isInline()) {
      *reinterpret_cast<StringView*>(group + offset) = str;
      return;
    }
    copyMultipartNoInline(str, group, offset);
  }

  /// Returns a contiguous view on 'view', where 'view' comes from
  /// copyMultipart(). Uses 'storage' to own a possible temporary copy. Making a
  /// temporary copy only happens for non-contiguous strings.
  static StringView contiguousString(StringView view, std::string& storage);

  /// Allocates 'size' contiguous bytes preceded by a Header. Returns the
  /// address of Header.
  Header* allocate(int32_t size) {
    VELOX_CHECK_NULL(
        state_.currentHeader(),
        "Do not call allocate() when a write is in progress");
    return allocate(std::max(size, kMinAlloc), true);
  }

  /// Allocates a block that is independently freeable but is freed on
  /// destruction of 'this'. The block has no header and must be freed by
  /// freeToPool() if to be freed before destruction of 'this'.
  void* allocateFromPool(size_t size);

  /// Frees a block allocated with allocateFromPool(). The pointer and size must
  /// match.
  void freeToPool(void* ptr, size_t size);

  /// Returns the header immediately below 'data'.
  static Header* headerOf(const void* data) {
    return castToHeader(data) - 1;
  }

  /// Returns the header below 'data'.
  static Header* castToHeader(const void* data) {
    return reinterpret_cast<Header*>(
        const_cast<char*>(reinterpret_cast<const char*>(data)));
  }

  /// Returns the byte size of block pointed by 'header'.
  inline size_t blockBytes(const Header* header) const {
    return header->size() + kHeaderSize;
  }

  /// Returns ByteInputStream over the data in the range of 'header' and
  /// possible continuation ranges.
  /// @param maxBytes If provided, the returned stream will cover at most that
  /// many bytes.
  static std::unique_ptr<ByteInputStream> prepareRead(
      const Header* header,
      size_t maxBytes = std::numeric_limits<size_t>::max());

  /// Returns the number of payload bytes between 'header->begin()' and
  /// 'position'.
  static int64_t offset(Header* header, Position position);

  /// Returns a position 'offset' bytes after 'header->begin()'.
  static Position seek(Header* header, int64_t offset);

  /// Returns the number of bytes that can be written starting at 'position'
  /// without allocating more space.
  static int64_t available(const Position& position);

  /// Ensures that one can write at least 'bytes' data starting at 'position'
  /// without allocating more space. 'position' can be changed but will
  /// logically point at the same data. Data to the right of 'position is not
  /// preserved.
  void ensureAvailable(int32_t bytes, Position& position);

  /// Sets stream to write to this pool. The write can span multiple
  /// non-contiguous runs. Each contiguous run will have at least kMinContiguous
  /// bytes of contiguous space. finishWrite finalizes the allocation
  /// information after the write is done. Returns the position at the start of
  /// the allocated block.
  Position newWrite(
      ByteOutputStream& stream,
      int32_t preferredSize = kMinContiguous);

  // Sets 'stream' to write starting at 'position'. If new ranges have to
  // be allocated when writing, headers will be updated accordingly.
  void extendWrite(Position position, ByteOutputStream& stream);

  /// Completes a write prepared with newWrite or extendWrite. Up to
  /// 'numReserveBytes' unused bytes, if available, are left after the end of
  /// the write to accommodate another write. Returns a pair of positions: (1)
  /// position at the start of this 'write', (2) position immediately after the
  /// last written byte.
  std::pair<Position, Position> finishWrite(
      ByteOutputStream& stream,
      int32_t numReserveBytes);

  /// Allocates a new range for a stream writing to 'this'. Sets the last word
  /// of the previous range to point to the new range and copies the overwritten
  /// word as the first word of the new range. If 'lastRange' is non-null, we
  /// are continuing an existing entry and setting the last word  of the
  /// previous entry point to the new one. In this case, we decrement the size
  /// in 'lastEntry' by the size of the continue pointer, so that the sum of the
  /// sizes reflects the payload size without any overheads. Furthermore,
  /// rewriting a multirange entry is safe because a write spanning multiple
  /// ranges will not overwrite the next pointer.
  ///
  /// May allocate less than 'bytes'.
  void newRange(int32_t bytes, ByteRange* lastRange, ByteRange* range) override;

  /// Allocates a new range of at least 'bytes' size.
  void newContiguousRange(int32_t bytes, ByteRange* range);

  void newTinyRange(int32_t bytes, ByteRange* lastRange, ByteRange* range)
      override {
    newRange(bytes, lastRange, range);
  }

  /// Returns the total memory footprint of 'this'.
  int64_t retainedSize() const {
    return state_.pool().allocatedBytes() + state_.sizeFromPool();
  }

  /// Adds the allocation of 'header' and any extensions (if header has
  /// kContinued set) to the free list.
  void free(Header* header);

  /// Returns a lower bound on bytes available without growing 'this'. This is
  /// the sum of free block sizes minus size of pointer for each. We subtract
  /// the pointer because in the worst case we would have one allocation that
  /// chains many small free blocks together via kContinued.
  uint64_t freeSpace() const {
    const int64_t minFree = state_.freeBytes() -
        state_.numFree() * (kHeaderSize + Header::kContinuedPtrSize);
    VELOX_CHECK_GE(minFree, 0, "Guaranteed free space cannot be negative");
    return minFree;
  }

  /// Frees all memory associated with 'this' and leaves 'this' ready for reuse.
  void clear() override;

  memory::MemoryPool* pool() const {
    return state_.pool().pool();
  }

  uint64_t currentBytes() const {
    return state_.currentBytes();
  }

  /// Checks the free space accounting and consistency of Headers. Throws when
  /// detects corruption. Returns the number of allocated payload bytes,
  /// excluding headers, continue links and other overhead.
  int64_t checkConsistency() const;

  /// Returns 'true' if this is empty. The implementation includes a call to
  /// checkConsistency() which makes it slow. Do not use in hot paths.
  bool isEmpty() const;

  std::string toString() const;

  /// Effectively makes this immutable while executing f, any attempt to access
  /// state_ in a mutable way while f is executing will cause an exception to be
  /// thrown.
  template <typename F>
  void freezeAndExecute(F&& f) {
    state_.freeze();

    SCOPE_EXIT {
      state_.unfreeze();
    };

    f();
  }

 private:
  static constexpr int32_t kUnitSize = 16 * memory::AllocationTraits::kPageSize;
  static constexpr int32_t kMinContiguous = 48;
  static constexpr int32_t kNumFreeLists = kMaxAlloc - kMinAlloc + 2;
  static constexpr uint32_t kHeaderSize = sizeof(Header);

  void newRange(
      int32_t bytes,
      ByteRange* lastRange,
      ByteRange* range,
      bool contiguous);

  // Adds a new standard size slab to the free list. This
  // grows the footprint in MemoryAllocator but does not allocate
  // anything yet. Throws if fails to grow.
  void newSlab();

  void removeFromFreeList(Header* header);

  // Allocates a block of specified size. If exactSize is false, the block may
  // be smaller or larger. Checks free list before allocating new memory.
  Header* allocate(int32_t size, bool exactSize);

  // Allocates memory from free list. Returns nullptr if no memory in free list,
  // otherwise returns a header of a free block of some size. if 'mustHaveSize'
  // is true, the block will not be smaller than 'preferredSize'. If
  // 'isFinalSize' is true, this will not return a block that is much larger
  // than preferredSize. Otherwise, the block can be larger and the user is
  // expected to call freeRestOfBlock to finalize the allocation.
  Header* allocateFromFreeLists(
      int32_t preferredSize,
      bool mustHaveSize,
      bool isFinalSize);

  Header* allocateFromFreeList(
      int32_t preferredSize,
      bool mustHaveSize,
      bool isFinalSize,
      int32_t freeListIndex);

  // Sets 'header' to be 'keepBytes' long and adds the remainder of
  // 'header's memory to free list. Does nothing if the resulting
  // blocks would be below minimum size.
  void freeRestOfBlock(Header* header, int32_t keepBytes);

  void
  copyMultipartNoInline(const StringView& str, char* group, int32_t offset);

  // Fast path for storing a string as a single part. Returns true if succeeded,
  // has no effect if returns false.
  bool storeStringFast(const char* bytes, int32_t size, char* destination);

  // Returns the free list index for 'size'.
  int32_t freeListIndex(int size);

  /// A class that wraps any fields in the HashStringAllocator, it's main
  /// purpose is to simplify the freeze/unfreeze mechanic.  Fields are exposed
  /// via accessor methods, attempting to invoke a non-const accessor when the
  /// HashStringAllocator is frozen will cause an exception to be thrown.
  class State {
   public:
    explicit State(memory::MemoryPool* pool) : pool_(pool) {}

    void freeze() {
      VELOX_CHECK(
          mutable_,
          "Attempting to freeze an already frozen HashStringAllocator.");
      mutable_ = false;
    }

    void unfreeze() {
      VELOX_CHECK(
          !mutable_,
          "Attempting to unfreeze an already unfrozen HashStringAllocator.");
      mutable_ = true;
    }

   private:
// Every field has two accessors, one that returns a reference and one that
// returns a const reference.  The one that returns a reference ensures that the
// HashStringAllocator isn't frozen first.
#define DECLARE_GETTERS(TYPE, NAME) \
 public:                            \
  inline TYPE& NAME() {             \
    assertMutability();             \
    return NAME##_;                 \
  }                                 \
                                    \
  inline TYPE const& NAME() const { \
    return NAME##_;                 \
  }

// Declare a default initialized field.
#define DECLARE_FIELD(TYPE, NAME) \
  DECLARE_GETTERS(TYPE, NAME)     \
                                  \
 private:                         \
  TYPE NAME##_;

// Declare a field initialized with a specific value.
#define DECLARE_FIELD_WITH_INIT_VALUE(TYPE, NAME, VALUE) \
  DECLARE_GETTERS(TYPE, NAME)                            \
                                                         \
 private:                                                \
  TYPE NAME##_{VALUE};

    typedef CompactDoubleList FreeList[kNumFreeLists];
    typedef uint64_t FreeNonEmptyBitMap[bits::nwords(kNumFreeLists)];
    typedef folly::F14FastMap<void*, size_t> AllocationsFromPool;

    // Circular list of free blocks.
    DECLARE_FIELD(FreeList, freeLists);

    // Bitmap with a 1 if the corresponding list in 'free_' is not empty.
    DECLARE_FIELD_WITH_INIT_VALUE(FreeNonEmptyBitMap, freeNonEmpty, {});

    // Count of elements in 'free_'. This is 0 when all free_[i].next() ==
    // &free_[i].
    DECLARE_FIELD_WITH_INIT_VALUE(uint64_t, numFree, 0);

    // Sum of the size of blocks in 'free_', excluding headers.
    DECLARE_FIELD_WITH_INIT_VALUE(uint64_t, freeBytes, 0);

    // Counter of allocated bytes. The difference of two point in time values
    // tells how much memory has been consumed by activity between these points
    // in time. Incremented by allocation and decremented by free. Used for
    // tracking the row by row space usage in a RowContainer.
    DECLARE_FIELD_WITH_INIT_VALUE(uint64_t, currentBytes, 0);

    // Pointer to Header for the range being written. nullptr if a write is not
    // in progress.
    DECLARE_FIELD(Position, startPosition);
    DECLARE_FIELD_WITH_INIT_VALUE(Header*, currentHeader, nullptr);

    // Pool for getting new slabs.
    DECLARE_FIELD(memory::AllocationPool, pool);

    // Map from pointer to size for large blocks allocated from pool().
    DECLARE_FIELD(AllocationsFromPool, allocationsFromPool);

    // Sum of sizes in 'allocationsFromPool_'.
    DECLARE_FIELD_WITH_INIT_VALUE(int64_t, sizeFromPool, 0);

#undef DECLARE_FIELD_WITH_INIT_VALUE
#undef DECLARE_FIELD
#undef DECLARE_GETTERS

    void assertMutability() const {
      VELOX_CHECK(mutable_, "The HashStringAllocator is immutable.");
    }

    tsan_atomic<bool> mutable_ = true;
  };

  // This should be the only field in HashStringAllocator, any additional fields
  // should be added as private members of State exposed through accessors.
  State state_;
};

/// Utility for keeping track of allocation between two points in time. A
/// counter on a row supplied at construction is incremented by the change in
/// allocation between construction and destruction. This is a scoped guard to
/// use around setting variable length data in a RowContainer or similar.
template <typename T, typename TCounter = uint32_t>
class RowSizeTracker {
 public:
  ///  Will update the counter at pointer cast to TCounter* with the change in
  ///  allocation during the lifetime of 'this'
  RowSizeTracker(T& counter, HashStringAllocator& allocator)
      : allocator_(&allocator),
        size_(allocator_->currentBytes()),
        counter_(counter) {}

  ~RowSizeTracker() {
    auto delta = allocator_->currentBytes() - size_;
    if (delta) {
      saturatingIncrement(&counter_, delta);
    }
  }

 private:
  // Increments T at *pointer without wrapping around at overflow.
  void saturatingIncrement(T* pointer, int64_t delta) {
    auto value = *reinterpret_cast<TCounter*>(pointer) + delta;
    *reinterpret_cast<TCounter*>(pointer) =
        std::min<uint64_t>(value, std::numeric_limits<TCounter>::max());
  }

  HashStringAllocator* const allocator_;
  const uint64_t size_;
  T& counter_;
};

/// An Allocator based by HashStringAllocator to use with STL containers.
template <class T>
struct StlAllocator {
  using value_type = T;

  explicit StlAllocator(HashStringAllocator* allocator)
      : allocator_{allocator} {
    VELOX_CHECK(allocator);
  }

  template <class U>
  explicit StlAllocator(const StlAllocator<U>& allocator)
      : allocator_{allocator.allocator()} {
    VELOX_CHECK_NOT_NULL(allocator_);
  }

  T* allocate(std::size_t n) {
    if (n * sizeof(T) > HashStringAllocator::kMaxAlloc) {
      return reinterpret_cast<T*>(allocator_->allocateFromPool(n * sizeof(T)));
    }
    return reinterpret_cast<T*>(
        allocator_->allocate(checkedMultiply(n, sizeof(T)))->begin());
  }

  void deallocate(T* p, std::size_t n) noexcept {
    if (n * sizeof(T) > HashStringAllocator::kMaxAlloc) {
      return allocator_->freeToPool(p, n * sizeof(T));
    }
    allocator_->free(HashStringAllocator::headerOf(p));
  }

  HashStringAllocator* allocator() const {
    return allocator_;
  }

  friend bool operator==(const StlAllocator& lhs, const StlAllocator& rhs) {
    return lhs.allocator_ == rhs.allocator_;
  }

  friend bool operator!=(const StlAllocator& lhs, const StlAllocator& rhs) {
    return !(lhs == rhs);
  }

 private:
  HashStringAllocator* allocator_;
};

/// An allocator backed by HashStringAllocator that guaratees a configurable
/// alignment. The alignment must be a power of 2 and not be 0. This allocator
/// can be used with folly F14 containers that requires 16-bytes alignment.
template <class T, uint8_t Alignment>
struct AlignedStlAllocator {
  using value_type = T;

  static_assert(
      Alignment != 0,
      "Alignment of AlignedStlAllocator cannot be 0.");
  static_assert(
      (Alignment & (Alignment - 1)) == 0,
      "Alignment of AlignedStlAllocator must be a power of 2.");

  template <class Other>
  struct rebind {
    using other = AlignedStlAllocator<Other, Alignment>;
  };

  explicit AlignedStlAllocator(HashStringAllocator* allocator)
      : allocator_{allocator},
        poolAligned_(allocator_->pool()->alignment() >= Alignment) {
    VELOX_CHECK(allocator);
  }

  template <class U, uint8_t A>
  explicit AlignedStlAllocator(const AlignedStlAllocator<U, A>& allocator)
      : allocator_{allocator.allocator()},
        poolAligned_(allocator_->pool()->alignment() >= Alignment) {
    VELOX_CHECK(allocator_);
  }

  T* allocate(std::size_t n) {
    if (n * sizeof(T) > HashStringAllocator::kMaxAlloc) {
      if (poolAligned_) {
        return reinterpret_cast<T*>(
            allocator_->allocateFromPool(n * sizeof(T)));
      } else {
        auto paddedSize = calculatePaddedSize(n);
        // Allocate the memory from pool directly.
        auto ptr =
            reinterpret_cast<T*>(allocator_->allocateFromPool(paddedSize));

        return alignPtr((char*)ptr, n, paddedSize);
      }
    }

    auto paddedSize = calculatePaddedSize(n);
    auto ptr = reinterpret_cast<T*>(allocator_->allocate(paddedSize)->begin());

    return alignPtr((char*)ptr, n, paddedSize);
  }

  void deallocate(T* p, std::size_t n) noexcept {
    if (n * sizeof(T) > HashStringAllocator::kMaxAlloc) {
      if (poolAligned_) {
        return allocator_->freeToPool(p, n * sizeof(T));
      } else {
        auto delta = *reinterpret_cast<int32_t*>((char*)p - 4);
        return allocator_->freeToPool(
            (char*)p - 4 - delta, calculatePaddedSize(n));
      }
    }

    auto delta = *reinterpret_cast<int32_t*>((char*)p - 4);
    allocator_->free(HashStringAllocator::headerOf((char*)p - 4 - delta));
  }

  HashStringAllocator* allocator() const {
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
  // Pad the memory user requested by some padding to facilitate memory
  // alignment later. Memory layout:
  // - padding(length is stored in `delta`)
  // - delta(4 bytes storing the size of padding)
  // - the aligned ptr
  FOLLY_ALWAYS_INLINE std::size_t calculatePaddedSize(std::size_t n) {
    return checkedPlus<size_t>(Alignment + 4, checkedMultiply(n, sizeof(T)));
  }

  FOLLY_ALWAYS_INLINE T*
  alignPtr(char* ptr, std::size_t allocateCount, std::size_t& paddedSize) {
    // Align 'ptr + 4'.
    void* alignedPtr = ptr + 4;
    paddedSize -= 4;
    std::align(Alignment, allocateCount * sizeof(T), alignedPtr, paddedSize);

    // Write alignment delta just before the aligned pointer.
    int32_t delta = (char*)alignedPtr - ptr - 4;
    *reinterpret_cast<int32_t*>((char*)alignedPtr - 4) = delta;

    return reinterpret_cast<T*>(alignedPtr);
  }

  HashStringAllocator* allocator_;
  const bool poolAligned_;
};

} // namespace facebook::velox
