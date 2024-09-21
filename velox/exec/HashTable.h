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

#include "velox/common/base/Portability.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

using PartitionBoundIndexType = int64_t;
/// Provides the partition info for parallel join table build use.
struct TableInsertPartitionInfo {
  /// ['start', 'end') specifies the insert range of this table partition.
  PartitionBoundIndexType start;
  PartitionBoundIndexType end;
  /// Used to contains the overflowed rows which can't be inserted into the
  /// given table partition range.
  std::vector<char*>& overflows;

  TableInsertPartitionInfo(
      PartitionBoundIndexType _start,
      PartitionBoundIndexType _end,
      std::vector<char*>& _overflows)
      : start(_start), end(_end), overflows(_overflows) {
    VELOX_CHECK_GE(start, 0);
    VELOX_CHECK_LT(start, end);
  }

  /// Indicates if 'index' is within this partition range.
  bool inRange(PartitionBoundIndexType index) const {
    return index >= start && index < end;
  }

  /// Adds 'row' falls outside of this partititon range into 'overflows'.
  void addOverflow(char* row) {
    overflows.push_back(row);
  }
};

/// Contains input and output parameters for groupProbe and joinProbe APIs.
struct HashLookup {
  explicit HashLookup(const std::vector<std::unique_ptr<VectorHasher>>& h)
      : hashers(h) {}

  void reset(vector_size_t size) {
    rows.resize(size);
    hashes.resize(size);
    hits.resize(size);
    newGroups.clear();
  }

  /// One entry per group-by or join key.
  const std::vector<std::unique_ptr<VectorHasher>>& hashers;

  /// Scratch memory used to call VectorHasher::lookupValueIds.
  VectorHasher::ScratchMemory scratchMemory;

  /// Input to groupProbe and joinProbe APIs.

  /// Set of row numbers of row to probe.
  raw_vector<vector_size_t> rows;

  /// Hashes or value IDs for rows in 'rows'. Not aligned with 'rows'. Index is
  /// the row number.
  raw_vector<uint64_t> hashes;

  /// Results of groupProbe and joinProbe APIs.

  /// Contains one entry for each row in 'rows'. Index is the row number.
  /// For groupProbe, a pointer to an existing or new row with matching grouping
  /// keys. For joinProbe, a pointer to the first row with matching keys or null
  /// if no match.
  raw_vector<char*> hits;

  /// For groupProbe, row numbers for which a new entry was inserted (didn't
  /// exist before the groupProbe). Empty for joinProbe.
  std::vector<vector_size_t> newGroups;

  /// If using valueIds, list of concatenated valueIds. 1:1 with 'hashes'.
  /// Populated by groupProbe and joinProbe.
  raw_vector<uint64_t> normalizedKeys;
};

struct HashTableStats {
  int64_t capacity{0};
  int64_t numRehashes{0};
  int64_t numDistinct{0};
  /// Counts the number of tombstone table slots.
  int64_t numTombstones{0};
};

class BaseHashTable {
 public:
#if XSIMD_WITH_SSE2
  using TagVector = xsimd::batch<uint8_t, xsimd::sse2>;
#elif XSIMD_WITH_NEON
  using TagVector = xsimd::batch<uint8_t, xsimd::neon>;
#endif

  using MaskType = uint16_t;

  /// 2M entries, i.e. 16MB is the largest array based hash table.
  static constexpr uint64_t kArrayHashMaxSize = 2L << 20;

  /// Specifies the hash mode of a table.
  enum class HashMode { kHash, kArray, kNormalizedKey };

  static constexpr int8_t kNoSpillInputStartPartitionBit = -1;

  /// The name of the runtime stats collected and reported by operators that use
  /// the HashTable (HashBuild, HashAggregation).
  static inline const std::string kCapacity{"hashtable.capacity"};
  static inline const std::string kNumRehashes{"hashtable.numRehashes"};
  static inline const std::string kNumDistinct{"hashtable.numDistinct"};
  static inline const std::string kNumTombstones{"hashtable.numTombstones"};

  /// The same as above but only reported by the HashBuild operator.
  static inline const std::string kBuildWallNanos{"hashtable.buildWallNanos"};

  /// Returns the string of the given 'mode'.
  static std::string modeString(HashMode mode);

  /// Keeps track of results returned from a join table. One batch of keys can
  /// produce multiple batches of results. This is initialized from HashLookup,
  /// which is expected to stay constant while 'this' is being used.
  struct JoinResultIterator {
    JoinResultIterator(
        std::vector<vector_size_t>&& _varSizeListColumns,
        uint64_t _fixedSizeListColumnsSizeSum)
        : varSizeListColumns(std::move(_varSizeListColumns)),
          fixedSizeListColumnsSizeSum(_fixedSizeListColumnsSizeSum) {}

    void reset(const HashLookup& lookup) {
      rows = &lookup.rows;
      hits = &lookup.hits;
      lastRowIndex = 0;
      lastDuplicateRowIndex = 0;
    }

    bool atEnd() const {
      return !rows || lastRowIndex == rows->size();
    }

    /// The indexes of the build side projected columns that are variable sized.
    const std::vector<vector_size_t> varSizeListColumns;
    /// The per row total bytes of the build side projected columns that are
    /// fixed sized.
    const uint64_t fixedSizeListColumnsSizeSum{0};

    const raw_vector<vector_size_t>* rows{nullptr};
    const raw_vector<char*>* hits{nullptr};

    vector_size_t lastRowIndex{0};
    vector_size_t lastDuplicateRowIndex{0};
  };

  struct RowsIterator {
    int32_t hashTableIndex_{-1};
    RowContainerIterator rowContainerIterator_;

    void reset() {
      *this = {};
    }

    std::string toString() const;
  };

  struct NullKeyRowsIterator {
    bool initialized = false;
    char* nextHit;
    vector_size_t lastDuplicateRowIndex{0};
  };

  /// Takes ownership of 'hashers'. These are used to keep key-level
  /// encodings like distinct values, ranges. These are stateful for
  /// kArray and kNormalizedKey hash modes and track the data
  /// population while adding payload for either aggregation or join
  /// build.
  explicit BaseHashTable(std::vector<std::unique_ptr<VectorHasher>>&& hashers)
      : hashers_(std::move(hashers)) {}

  virtual ~BaseHashTable() = default;

  virtual HashStringAllocator* stringAllocator() = 0;

  /// Populates 'hashes' and 'rows' fields in 'lookup' in preparation for
  /// 'groupProbe' call. Rehashes the table if necessary. Uses lookup.hashes to
  /// decode grouping keys from 'input'. If 'ignoreNullKeys' is true, updates
  /// 'rows' to remove entries with null grouping keys. After this call, 'rows'
  /// may have no entries selected.
  virtual void prepareForGroupProbe(
      HashLookup& lookup,
      const RowVectorPtr& input,
      SelectivityVector& rows,
      int8_t spillInputStartPartitionBit) = 0;

  /// Finds or creates a group for each key in 'lookup'. The keys are
  /// returned in 'lookup.hits'.
  virtual void groupProbe(
      HashLookup& lookup,
      int8_t spillInputStartPartitionBit) = 0;

  /// Returns the first hit for each key in 'lookup'. The keys are in
  /// 'lookup.hits' with a nullptr representing a miss. This is for use in hash
  /// join probe. Use listJoinResults to iterate over the results.
  virtual void joinProbe(HashLookup& lookup) = 0;

  /// Populates 'hashes' and 'rows' fields in 'lookup' in preparation for
  /// 'joinProbe' call. If hash mode is not kHash, populates 'hashes' with
  /// values IDs. Rows which do not have value IDs are removed from 'rows'
  /// (these rows cannot possibly match). if 'decodeAndRemoveNulls' is true,
  /// uses lookup.hashes to decode grouping keys from 'input' and updates 'rows'
  /// to remove entries with null grouping keys. Otherwise, assumes the caller
  /// has done that already. After this call, 'rows' may have no entries
  /// selected.
  virtual void prepareForJoinProbe(
      HashLookup& lookup,
      const RowVectorPtr& input,
      SelectivityVector& rows,
      bool decodeAndRemoveNulls) = 0;

  /// Fills 'hits' with consecutive hash join results. The corresponding element
  /// of 'inputRows' is set to the corresponding row number in probe keys.
  /// Returns the number of hits produced. If this is less than hits.size() then
  /// all the hits have been produced.
  /// Adds input rows without a match to 'inputRows' with corresponding hit
  /// set to nullptr if 'includeMisses' is true. Otherwise, skips input rows
  /// without a match. 'includeMisses' is set to true when listing results for
  /// the LEFT join.
  /// The filling stops when the total size of currently listed rows exceeds
  /// 'maxBytes'.
  virtual int32_t listJoinResults(
      JoinResultIterator& iter,
      bool includeMisses,
      folly::Range<vector_size_t*> inputRows,
      folly::Range<char**> hits,
      uint64_t maxBytes) = 0;

  /// Returns rows with 'probed' flag unset. Used by the right/full join.
  virtual int32_t listNotProbedRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) = 0;

  /// Returns rows with 'probed' flag set. Used by the right semi join.
  virtual int32_t listProbedRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) = 0;

  /// Returns all rows. Used by the right semi join project.
  virtual int32_t listAllRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) = 0;

  /// Returns all rows with null keys.  Used by null-aware joins (e.g. anti or
  /// left semi project).
  virtual int32_t
  listNullKeyRows(NullKeyRowsIterator* iter, int32_t maxRows, char** rows) = 0;

  virtual void prepareJoinTable(
      std::vector<std::unique_ptr<BaseHashTable>> tables,
      int8_t spillInputStartPartitionBit,
      folly::Executor* executor = nullptr) = 0;

  /// Returns the memory footprint in bytes for any data structures
  /// owned by 'this'.
  virtual int64_t allocatedBytes() const = 0;

  /// Deletes any content of 'this'. If 'freeTable' is false, then hash table is
  /// not freed which can be used for flushing a partial group by, for example.
  virtual void clear(bool freeTable = false) = 0;

  /// Returns the capacity of the internal hash table which is number of rows
  /// it can stores in a group by or hash join build.
  virtual uint64_t capacity() const = 0;

  /// Returns the number of rows in a group by or hash join build
  /// side. This is used for sizing the internal hash table.
  virtual uint64_t numDistinct() const = 0;

  /// Return a number of current stats that can help with debugging and
  /// profiling.
  virtual HashTableStats stats() const = 0;

  /// Returns table growth in bytes after adding 'numNewDistinct' distinct
  /// entries. This only concerns the hash table, not the payload rows.
  virtual uint64_t hashTableSizeIncrease(int32_t numNewDistinct) const = 0;

  /// Returns the estimated new hash table size in bytes with the given number
  /// of distinct entries.
  virtual uint64_t estimateHashTableSize(uint64_t numDistinct) const = 0;

  /// Returns true if the hash table contains rows with duplicate keys.
  virtual bool hasDuplicateKeys() const = 0;

  /// Returns the hash mode. This is needed for the caller to calculate
  /// the hash numbers using the appropriate method of the
  /// VectorHashers of 'this'.
  virtual HashMode hashMode() const = 0;

  /// Disables use of array or normalized key hash modes.
  void forceGenericHashMode(int8_t spillInputStartPartitionBit) {
    setHashMode(HashMode::kHash, 0, spillInputStartPartitionBit);
  }

  /// Decides the hash table representation based on the statistics in
  /// VectorHashers of 'this'. This must be called if we are in
  /// normalized key or array based hash mode and some new keys are not
  /// compatible with the encoding. This is notably the case on first
  /// insert where there are no encodings in place. Rehashes the table
  /// based on the statistics in Vectorhashers if the table is not
  /// empty. After calling this, the caller must recompute the hash of
  /// the key columns as the mappings in VectorHashers will have
  /// changed. The table is set up so as to take at least 'numNew'
  /// distinct entries before needing to rehash. If 'disableRangeArrayHash' is
  /// true, this will avoid kArray hash mode with value range mode keys. These
  /// can make large arrays with very few keys.  This setting persists for the
  /// lifetime of 'this'.
  virtual void decideHashMode(
      int32_t numNew,
      int8_t spillInputStartPartitionBit,
      bool disableRangeArrayHash = false) = 0;

  // Removes 'rows' from the hash table and its RowContainer. 'rows' must exist
  // and be unique.
  virtual void erase(folly::Range<char**> rows) = 0;

  /// Returns a brief description for use in debugging.
  virtual std::string toString() = 0;

  const std::vector<std::unique_ptr<VectorHasher>>& hashers() const {
    return hashers_;
  }

  RowContainer* rows() const {
    return rows_.get();
  }

  /// Returns all the row containers of a composed hash table such as for hash
  /// join use.
  virtual std::vector<RowContainer*> allRows() const = 0;

  /// Static functions for processing internals. Public because used in
  /// structs that define probe and insert algorithms.

  /// Extracts a 7 bit tag from a hash number. The high bit is always set.
  static uint8_t hashTag(uint64_t hash) {
    // This is likely all 0 for small key types (<= 32 bits).  Not an issue
    // because small types have a range that makes them normalized key cases.
    // If there are multiple small type keys, they are mixed which makes them a
    // 64 bit hash.  Normalized keys are mixed before being used as hash
    // numbers.
    return static_cast<uint8_t>(hash >> 38) | 0x80;
  }

  /// Loads a vector of tags for bulk comparison. Disables tsan errors
  /// because with parallel join build different ranges of the table
  /// are filled by different threads, after which the main thread
  /// inserts the entries that would have overflowed past the
  /// inserting thread's range. There is a sync barrier between but
  /// tsan does not recognize this.
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
  __attribute__((__no_sanitize__("thread")))
#endif
#endif
  static TagVector
  loadTags(uint8_t* tags, int64_t tagIndex) {
    // Cannot use xsimd::batch::unaligned here because we need to skip TSAN.
    auto src = tags + tagIndex;
#if XSIMD_WITH_SSE2
    return TagVector(_mm_loadu_si128(reinterpret_cast<__m128i const*>(src)));
#elif XSIMD_WITH_NEON
    return TagVector(vld1q_u8(src));
#endif
  }

  const CpuWallTiming& offThreadBuildTiming() const {
    return offThreadBuildTiming_;
  }

  /// Copies the values at 'columnIndex' into 'result' for the 'rows.size' rows
  /// pointed to by 'rows'. If an entry in 'rows' is null, sets corresponding
  /// row in 'result' to null.
  virtual void extractColumn(
      folly::Range<char* const*> rows,
      int32_t columnIndex,
      const VectorPtr& result) = 0;

 protected:
  static FOLLY_ALWAYS_INLINE size_t tableSlotSize() {
    // Each slot is 8 bytes.
    return sizeof(void*);
  }

  virtual void setHashMode(
      HashMode mode,
      int32_t numNew,
      int8_t spillInputStartPartitionBit) = 0;

  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  std::unique_ptr<RowContainer> rows_;

  // Time spent in build outside of the calling thread.
  CpuWallTiming offThreadBuildTiming_;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    const BaseHashTable::HashMode& mode) {
  os << BaseHashTable::modeString(mode);
  return os;
}

class ProbeState;
namespace test {
template <bool ignoreNullKeys>
class HashTableTestHelper;
}

template <bool ignoreNullKeys>
class HashTable : public BaseHashTable {
 public:
  // Can be used for aggregation or join. An aggregation hash table
  // can also double as a join build side. 'isJoinBuild' is true if
  // this is a build side. 'allowDuplicates' is false for a build side if
  // second occurrences of a key are to be silently ignored or will
  // not occur. In this case the row does not need a link to the next
  // match. 'hasProbedFlag' adds an extra bit in every row for tracking rows
  // that matches join condition for right and full outer joins.
  HashTable(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      const std::vector<Accumulator>& accumulators,
      const std::vector<TypePtr>& dependentTypes,
      bool allowDuplicates,
      bool isJoinBuild,
      bool hasProbedFlag,
      uint32_t minTableSizeForParallelJoinBuild,
      memory::MemoryPool* pool);

  ~HashTable() override = default;

  static std::unique_ptr<HashTable> createForAggregation(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      const std::vector<Accumulator>& accumulators,
      memory::MemoryPool* pool) {
    return std::make_unique<HashTable>(
        std::move(hashers),
        accumulators,
        std::vector<TypePtr>{},
        false, // allowDuplicates
        false, // isJoinBuild
        false, // hasProbedFlag
        0, // minTableSizeForParallelJoinBuild
        pool);
  }

  static std::unique_ptr<HashTable> createForJoin(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      const std::vector<TypePtr>& dependentTypes,
      bool allowDuplicates,
      bool hasProbedFlag,
      uint32_t minTableSizeForParallelJoinBuild,
      memory::MemoryPool* pool) {
    return std::make_unique<HashTable>(
        std::move(hashers),
        std::vector<Accumulator>{},
        dependentTypes,
        allowDuplicates,
        true, // isJoinBuild
        hasProbedFlag,
        minTableSizeForParallelJoinBuild,
        pool);
  }

  void groupProbe(HashLookup& lookup, int8_t spillInputStartPartitionBit)
      override;

  void joinProbe(HashLookup& lookup) override;

  int32_t listJoinResults(
      JoinResultIterator& iter,
      bool includeMisses,
      folly::Range<vector_size_t*> inputRows,
      folly::Range<char**> hits,
      uint64_t maxBytes) override;

  int32_t listNotProbedRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) override;

  int32_t listProbedRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) override;

  int32_t listAllRows(
      RowsIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) override;

  int32_t listNullKeyRows(
      NullKeyRowsIterator* iter,
      int32_t maxRows,
      char** rows) override;

  void clear(bool freeTable = false) override;

  int64_t allocatedBytes() const override {
    // For each row: sizeof(char*) per table entry + memory
    // allocated with MemoryAllocator for fixed-width rows and strings.
    return sizeof(char*) * capacity_ + rows_->allocatedBytes();
  }

  HashStringAllocator* stringAllocator() override {
    return &rows_->stringAllocator();
  }

  uint64_t capacity() const override {
    return capacity_;
  }

  uint64_t numDistinct() const override {
    return numDistinct_;
  }

  HashTableStats stats() const override {
    return HashTableStats{
        capacity_, numRehashes_, numDistinct_, numTombstones_};
  }

  bool hasDuplicateKeys() const override {
    return hasDuplicates_;
  }

  HashMode hashMode() const override {
    return hashMode_;
  }

  void decideHashMode(
      int32_t numNew,
      int8_t spillInputStartPartitionBit,
      bool disableRangeArrayHash = false) override;

  void erase(folly::Range<char**> rows) override;

  /// Moves the contents of 'tables' into 'this' and prepares 'this'
  /// for use in hash join probe. A hash join build side is prepared as
  /// follows: 1. Each build side thread gets a random selection of the
  /// build stream. Each accumulates rows into its own
  /// HashTable'sRowContainer and updates the VectorHashers of the
  /// table to reflect the data as long as the data shows promise for
  /// kArray or kNormalizedKey representation. After all the build
  /// tables are filled, they are combined into one top level table
  /// with prepareJoinTable. This then takes ownership of all the data
  /// and VectorHashers and decides the hash mode and representation.
  void prepareJoinTable(
      std::vector<std::unique_ptr<BaseHashTable>> tables,
      int8_t spillInputStartPartitionBit,
      folly::Executor* executor = nullptr) override;

  void prepareForJoinProbe(
      HashLookup& lookup,
      const RowVectorPtr& input,
      SelectivityVector& rows,
      bool decodeAndRemoveNulls) override;

  void prepareForGroupProbe(
      HashLookup& lookup,
      const RowVectorPtr& input,
      SelectivityVector& rows,
      int8_t spillInputStartPartitionBit) override;

  uint64_t hashTableSizeIncrease(int32_t numNewDistinct) const override {
    if (numDistinct_ + numNewDistinct > rehashSize()) {
      // If rehashed, the table adds size_ entries (i.e. doubles),
      // adding one pointer worth for each new position.  (16 tags, 16 6 byte
      // pointers, 16 bytes padding).
      return capacity_ * tableSlotSize();
    }
    return 0;
  }

  uint64_t estimateHashTableSize(uint64_t numDistinct) const override {
    // Take the max of max size in array mode and estimated size in non-array
    // mode.
    const uint64_t maxByteSizeInArrayMode = kArrayHashMaxSize * tableSlotSize();
    return bits::roundUp(
        std::max(
            maxByteSizeInArrayMode,
            newHashTableEntries(numDistinct, 0) * tableSlotSize()),
        memory::AllocationTraits::kPageSize);
  }

  std::vector<RowContainer*> allRows() const override;

  std::string toString() override;

  /// Returns the details of the range of buckets. The range starts from
  /// zero-based 'startBucket' and contains 'numBuckets' or however many there
  /// are left till the end of the table.
  std::string toString(int64_t startBucket, int64_t numBuckets = 1) const;

  /// Invoked to check the consistency of the internal state. The function scans
  /// all the table slots to check if the relevant slot counting are correct
  /// such as the number of used slots ('numDistinct_') and the number of
  /// tombstone slots ('numTombstones_').
  ///
  /// NOTE: the check cost is non-trivial and is mostly intended for testing
  /// purpose.
  void checkConsistency() const;

  auto& testingOtherTables() const {
    return otherTables_;
  }

  uint64_t testingRehashSize() const {
    return rehashSize();
  }

  void extractColumn(
      folly::Range<char* const*> rows,
      int32_t columnIndex,
      const VectorPtr& result) override {
    RowContainer::extractColumn(
        rows.data(),
        rows.size(),
        rows_->columnAt(columnIndex),
        columnHasNulls_[columnIndex],
        result);
  }

 private:
  // Enables debug stats for collisions for debug build.
#ifdef NDEBUG
  static constexpr bool kTrackLoads = false;
#else
  static constexpr bool kTrackLoads = true;
#endif

  // The table in non-kArray mode has a power of two number of buckets each with
  // 16 slots. Each slot has a 1 byte tag (a field of hash number) and a 48 bit
  // pointer. All the tags are in a 16 byte SIMD word followed by the 6 byte
  // pointers. There are 16 bytes of padding at the end to make the bucket
  // occupy exactly two (64 bytes) cache lines.
  class Bucket {
   public:
    uint8_t tagAt(int32_t slotIndex) {
      return reinterpret_cast<uint8_t*>(&tags_)[slotIndex];
    }

    char* pointerAt(int32_t slotIndex) {
      return reinterpret_cast<char*>(
          *reinterpret_cast<uintptr_t*>(&pointers_[kPointerSize * slotIndex]) &
          kPointerMask);
    }

    void setTag(int32_t slotIndex, uint8_t tag) {
      reinterpret_cast<uint8_t*>(&tags_)[slotIndex] = tag;
    }

    void setPointer(int32_t slotIndex, void* pointer) {
      auto* const slot =
          reinterpret_cast<uintptr_t*>(&pointers_[slotIndex * kPointerSize]);
      *slot = (*slot & ~kPointerMask) | reinterpret_cast<uintptr_t>(pointer);
    }

   private:
    static constexpr uint8_t kPointerSignificantBits = 48;
    static constexpr uint64_t kPointerMask =
        bits::lowMask(kPointerSignificantBits);
    static constexpr int32_t kPointerSize = kPointerSignificantBits / 8;

    TagVector tags_;
    char pointers_[sizeof(TagVector) * kPointerSize];
    char padding_[16];
  };

  static_assert(sizeof(Bucket) == 128);
  static constexpr uint64_t kBucketSize = sizeof(Bucket);

  // Returns the bucket at byte offset 'offset' from 'table_'.
  Bucket* bucketAt(int64_t offset) const {
    VELOX_DCHECK_EQ(0, offset & (kBucketSize - 1));
    return reinterpret_cast<Bucket*>(reinterpret_cast<char*>(table_) + offset);
  }

  // Returns the number of entries after which the table gets rehashed.
  static uint64_t rehashSize(int64_t size) {
    // This implements the F14 load factor: Resize if less than 1/8 unoccupied.
    return size - (size / 8);
  }

  // Returns the number of entries with 'numNew' and existing 'numDistincts'
  // distincts to create a new hash table.
  static uint64_t newHashTableEntries(uint64_t numDistincts, uint64_t numNew) {
    // Initial guess of cardinality is double the first input batch or at
    // least 2K entries.
    auto numNewEntries = std::max(
        (uint64_t)2048, bits::nextPowerOfTwo(numNew * 2 + numDistincts));
    const auto newNumDistincts = numDistincts + numNew;
    if (newNumDistincts > rehashSize(numNewEntries)) {
      numNewEntries *= 2;
    }
    return numNewEntries;
  }

  template <RowContainer::ProbeType probeType>
  int32_t
  listRows(RowsIterator* iter, int32_t maxRows, uint64_t maxBytes, char** rows);

  void arrayGroupProbe(HashLookup& lookup);

  void setHashMode(
      HashMode mode,
      int32_t numNew,
      int8_t spillInputStartPartitionBit) override;

  // Fast path for join results when there are no duplicates in the table and
  // only fixed size rows are to be extract.
  int32_t listJoinResultsFastPath(
      JoinResultIterator& iter,
      bool includeMisses,
      folly::Range<vector_size_t*> inputRows,
      folly::Range<char**> hits,
      uint64_t maxBytes);

  // Tries to use as many range hashers as can in a normalized key situation.
  void enableRangeWhereCan(
      const std::vector<uint64_t>& rangeSizes,
      const std::vector<uint64_t>& distinctSizes,
      std::vector<bool>& useRange);

  // Sets value ranges or distinct value ids mode for VectorHashers in a kArray
  // or kNormalizedKeys mode table.
  uint64_t setHasherMode(
      const std::vector<std::unique_ptr<VectorHasher>>& hashers,
      const std::vector<bool>& useRange,
      const std::vector<uint64_t>& rangeSizes,
      const std::vector<uint64_t>& distinctSizes);

  // Clears all elements of 'useRange' except ones that correspond to boolean
  // VectorHashers.
  void clearUseRange(std::vector<bool>& useRange);

  void rehash(bool initNormalizedKeys, int8_t spillInputStartPartitionBit);

  uint64_t rehashSize() const {
    return rehashSize(capacity_ - numTombstones_);
  }

  void storeKeys(HashLookup& lookup, vector_size_t row);

  void storeRowPointer(uint64_t index, uint64_t hash, char* row);

  // Allocates new tables for tags and payload pointers. The size must
  // a power of 2.
  void allocateTables(uint64_t size, int8_t spillInputStartPartitionBit);

  // 'initNormalizedKeys' is passed to 'rehash' --> 'rehash' --> 'insertBatch'.
  // If it's false and the table is in normalized keys mode,
  // the keys are retrieved from the row and the hash is made
  // from this, without recomputing the normalized key.
  void checkSize(
      int32_t numNew,
      bool initNormalizedKeys,
      int8_t spillInputStartPartitionBit);

  // Computes hash numbers of the appropriate hash mode for 'groups',
  // stores these in 'hashes' and inserts the groups using
  // insertForJoin or insertForGroupBy.
  bool insertBatch(
      char** groups,
      int32_t numGroups,
      raw_vector<uint64_t>& hashes,
      bool initNormalizedKeys);

  // Inserts 'numGroups' entries into 'this'. 'groups' point to contents in a
  // RowContainer owned by 'this'. 'hashes' are the hash numbers or array
  // indices (if kArray mode) for each group. Duplicate key rows are chained
  // via their next link. If not null, 'partitionInfo' provides the table
  // partition info for parallel join table build. It specifies the first and
  // (exclusive) last indexes of the insert entries in the table. If a row
  // can't be inserted within this range, it is not inserted but rather added
  // to the end of 'overflows' in 'partitionInfo'. 'allocator' is provided for
  // duplicate row vector allocations.
  void insertForJoin(
      RowContainer* rows,
      char** groups,
      uint64_t* hashes,
      int32_t numGroups,
      TableInsertPartitionInfo* partitionInfo,
      HashStringAllocator* allocator);

  // Inserts 'numGroups' entries into 'this'. 'groups' point to
  // contents in a RowContainer owned by 'this'. 'hashes' are the hash
  // numbers or array indices (if kArray mode) for each
  // group. 'groups' is expected to have no duplicate keys.
  void insertForGroupBy(char** groups, uint64_t* hashes, int32_t numGroups);

  // Checks if we can apply parallel table build optimization for hash join.
  // The function returns true if all of the following conditions:
  // 1. the hash table is built for parallel join;
  // 2. there is more than one sub-tables;
  // 3. the build executor has been set;
  // 4. the table is not in kArray mode;
  // 5. the number of table entries per each parallel build shard is no less
  //    than a pre-defined threshold: 1000 for now.
  bool canApplyParallelJoinBuild() const;

  // Builds a join table with '1 + otherTables_.size()' independent
  // threads using 'executor_'. First all RowContainers get partition
  // numbers assigned to each row. Next, all threads pick all rows
  // assigned to their thread-specific partition and insert these. If
  // a row would overflow past the end of its partition it is added to
  // a set of overflow rows that are sequentially inserted after all
  // else.
  void parallelJoinBuild();

  // Inserts the rows in 'partition' from this and 'otherTables' into 'this'.
  // The rows that would have gone past the end of the partition are returned in
  // 'overflow'.
  void buildJoinPartition(
      uint8_t partition,
      const std::vector<std::unique_ptr<RowPartitions>>& rowPartitions,
      std::vector<char*>& overflow);

  // Assigns a partition to each row of 'subtable' in RowPartitions of
  // subtable's RowContainer. If 'hashMode_' is kNormalizedKeys, records the
  // normalized key of each row below the row in its container.
  void partitionRows(
      HashTable<ignoreNullKeys>& subtable,
      RowPartitions& rowPartitions);

  // Calculates hashes for 'rows' and returns them in 'hashes'. If
  // 'initNormalizedKeys' is true, the normalized keys are stored below each row
  // in the container. If 'initNormalizedKeys' is false and the table is in
  // normalized keys mode, the keys are retrieved from the row and the hash is
  // made from this, without recomputing the normalized key. Returns false if
  // the hash keys are not mappable via the VectorHashers.
  bool hashRows(
      folly::Range<char**> rows,
      bool initNormalizedKeys,
      raw_vector<uint64_t>& hashes);

  char* insertEntry(HashLookup& lookup, uint64_t index, vector_size_t row);

  bool compareKeys(const char* group, HashLookup& lookup, vector_size_t row);

  bool compareKeys(const char* group, const char* inserted);

  template <bool isJoin, bool isNormalizedKey = false>
  void fullProbe(HashLookup& lookup, ProbeState& state, bool extraCheck);

  // Shortcut path for group by with normalized keys.
  void groupNormalizedKeyProbe(HashLookup& lookup);

  // Array probe with SIMD.
  void arrayJoinProbe(HashLookup& lookup);

  // Shortcut for probe with normalized keys.
  void joinNormalizedKeyProbe(HashLookup& lookup);

  // Returns the total size of the variable size 'columns' in 'row'.
  // NOTE: No checks are done in the method for performance considerations.
  // Caller needs to make sure only variable size columns are inside of
  // 'columns'.
  inline uint64_t joinProjectedVarColumnsSize(
      const std::vector<vector_size_t>& columns,
      const char* row) const;

  // Adds a row to a hash join table in kArray hash mode. Returns true if a new
  // entry was made and false if the row was added to an existing set of rows
  // with the same key. 'allocator' is provided for duplicate row vector
  // allocations.
  bool arrayPushRow(
      RowContainer* rows,
      char* row,
      int32_t index,
      HashStringAllocator* allocator);

  // Adds a row to a hash join build side entry with multiple rows with the same
  // key.  'rows' should be the same as the one in hash table except for
  // 'parallelJoinBuild'. 'allocator' is provided for duplicate row vector
  // allocations.
  void pushNext(
      RowContainer* rows,
      char* row,
      char* next,
      HashStringAllocator* allocator);

  // Finishes inserting an entry into a join hash table. If 'partitionInfo' is
  // not null and the insert falls out-side of the partition range, then insert
  // is not made but row is instead added to 'overflow' in 'partitionInfo'
  template <bool isNormailizedKeyMode>
  void buildFullProbe(
      RowContainer* rows,
      ProbeState& state,
      uint64_t hash,
      char* row,
      bool extraCheck,
      TableInsertPartitionInfo* partitionInfo,
      HashStringAllocator* allocator);

  template <bool isNormailizedKeyMode>
  void insertForJoinWithPrefetch(
      RowContainer* rows,
      char** groups,
      uint64_t* hashes,
      int32_t numGroups,
      TableInsertPartitionInfo* partitionInfo,
      HashStringAllocator* allocator);

  // Updates 'hashers_' to correspond to the keys in the
  // content. Returns true if all hashers offer a mapping to value ids
  // for array or normalized key.
  bool analyze();

  // Erases the entries of rows from the hash table and its RowContainer.
  // 'hashes' must be computed according to 'hashMode_'.
  void eraseWithHashes(folly::Range<char**> rows, uint64_t* hashes);

  // Returns the percentage of values to reserve for new keys in range
  // or distinct mode VectorHashers in a group by hash table. 0 for
  // join build sides.
  int32_t reservePct() const {
    return isJoinBuild_ ? 0 : 50;
  }

  // Returns the byte offset of the bucket for 'hash' starting from 'table_'.
  int64_t bucketOffset(uint64_t hash) const {
    return hash & bucketOffsetMask_;
  }

  // Returns the byte offset of the next bucket from 'offset'. Wraps around at
  // the end of the table.
  int64_t nextBucketOffset(int64_t bucketOffset) const {
    VELOX_DCHECK_EQ(0, bucketOffset & (kBucketSize - 1));
    VELOX_DCHECK_LT(bucketOffset, sizeMask_);
    return sizeMask_ & (bucketOffset + kBucketSize);
  }

  int64_t numBuckets() const {
    return numBuckets_;
  }

  // Return the row pointer at 'slotIndex' of bucket at 'bucketOffset'.
  char* row(int64_t bucketOffset, int32_t slotIndex) const {
    return bucketAt(bucketOffset)->pointerAt(slotIndex);
  }

  // Returns the tag vector for bucket at 'bucketOffset'.
  TagVector loadTags(int64_t bucketOffset) const {
    return BaseHashTable::loadTags(
        reinterpret_cast<uint8_t*>(table_), bucketOffset);
  }

  void incrementProbes(int32_t n = 1) {
    if (kTrackLoads) {
      VELOX_DCHECK_GT(n, 0);
      numProbes_ += n;
    }
  }

  void incrementTagLoads() const {
    if (kTrackLoads) {
      ++numTagLoads_;
    }
  }

  void incrementRowLoads() const {
    if (kTrackLoads) {
      ++numRowLoads_;
    }
  }

  void incrementHits() const {
    if (kTrackLoads) {
      ++numHits_;
    }
  }

  // We don't want any overlap in the bit ranges used by bucket index and those
  // used by spill partitioning; otherwise because we receive data from only one
  // partition, the overlapped bits would be the same and only a fraction of the
  // buckets would be used.  This would cause the insertion taking very long
  // time and block driver threads.
  void checkHashBitsOverlap(int8_t spillInputStartPartitionBit);

  // The min table size in row to trigger parallel join table build.
  const uint32_t minTableSizeForParallelJoinBuild_;

  int8_t sizeBits_;
  bool isJoinBuild_ = false;

  // Set at join build time if the table has duplicates, meaning that
  // the join can be cardinality increasing. Atomic for tsan because
  // many threads can set this.
  std::atomic<bool> hasDuplicates_{false};

  // Offset of next row link for join build side set from 'rows_'.
  int32_t nextOffset_{0};
  char** table_ = nullptr;
  memory::ContiguousAllocation tableAllocation_;

  // Number of slots across all buckets.
  int64_t capacity_{0};

  // Mask for extracting low bits of hash number for use as byte offsets into
  // the table. This is set to 'capacity_ * sizeof(void*) - 1'.
  int64_t sizeMask_{0};

  // Mask used to get the byte offset of a bucket from 'table_' given a hash
  // number.
  int64_t bucketOffsetMask_{0};
  int64_t numBuckets_{0};
  int64_t numDistinct_{0};
  // Counts the number of tombstone table slots.
  int64_t numTombstones_{0};
  // Counts the number of rehash() calls.
  int64_t numRehashes_{0};
  HashMode hashMode_ = HashMode::kArray;
  // Owns the memory of multiple build side hash join tables that are
  // combined into a single probe hash table.
  std::vector<std::unique_ptr<HashTable<ignoreNullKeys>>> otherTables_;
  // The allocators used for duplicate row vector allocations under parallel
  // join insert with one per each parallel join partition. These allocators
  // all allocate memory from the memory pool of the top level memory pool.
  std::vector<std::unique_ptr<HashStringAllocator>> joinInsertAllocators_;
  // Statistics maintained if kTrackLoads is set.

  // Flags indicate whether the same column in all build-side join hash tables
  // contains null values.
  std::vector<bool> columnHasNulls_;

  // Number of times a row is looked up or inserted.
  mutable tsan_atomic<int64_t> numProbes_{0};

  // Number of times a word of 16 tags is accessed. At least once per probe.
  mutable tsan_atomic<int64_t> numTagLoads_{0};

  // Number of times a row of payload is accessed. At least once per hit.
  mutable tsan_atomic<int64_t> numRowLoads_{0};

  // Number of times a match is found.
  mutable tsan_atomic<int64_t> numHits_{0};

  // Bounds of independently buildable index ranges in the table. The
  // range of partition i starts at [i] and ends at [i +1]. Bounds are multiple
  // of cache line  size.
  raw_vector<PartitionBoundIndexType> buildPartitionBounds_;

  // Executor for parallelizing hash join build. This may be the
  // executor for Drivers. If this executor is indefinitely taken by
  // other work, the thread of prepareJoinTable() will sequentially
  // execute the parallel build steps.
  folly::Executor* buildExecutor_{nullptr};

  //  Counts parallel build rows. Used for consistency check.
  std::atomic<int64_t> numParallelBuildRows_{0};

  // If true, avoids using VectorHasher value ranges with kArray hash mode.
  bool disableRangeArrayHash_{false};

  friend class ProbeState;
  friend test::HashTableTestHelper<ignoreNullKeys>;
};

} // namespace facebook::velox::exec

template <>
struct fmt::formatter<facebook::velox::exec::BaseHashTable::HashMode>
    : formatter<std::string> {
  auto format(
      facebook::velox::exec::BaseHashTable::HashMode s,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::exec::BaseHashTable::modeString(s), ctx);
  }
};
