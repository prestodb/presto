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

#include "velox/common/memory/MappedMemory.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

struct HashLookup {
  explicit HashLookup(const std::vector<std::unique_ptr<VectorHasher>>& h)
      : hashers(h) {}

  void reset(vector_size_t size) {
    rows.resize(size);
    hashes.resize(size);
    hits.resize(size);
    std::fill(hits.begin(), hits.end(), nullptr);
    newGroups.clear();
  }

  // One entry per aggregation or join key
  const std::vector<std::unique_ptr<VectorHasher>>& hashers;
  raw_vector<vector_size_t> rows;
  // Hash number for all input rows.
  raw_vector<uint64_t> hashes;
  // If using valueIds, list of concatenated valueIds. 1:1 with 'hashes'.
  raw_vector<uint64_t> normalizedKeys;
  // Hit for each row of input. nullptr if no hit. Points to the
  // corresponding group row.
  raw_vector<char*> hits;
  std::vector<vector_size_t> newGroups;
};

class BaseHashTable {
 public:
  using normalized_key_t = uint64_t;

#if XSIMD_WITH_SSE2
  using TagVector = xsimd::batch<uint8_t, xsimd::sse2>;
#elif XSIMD_WITH_NEON
  using TagVector = xsimd::batch<uint8_t, xsimd::neon>;
#endif

  using MaskType = uint16_t;

  // 2M entries, i.e. 16MB is the largest array based hash table.
  static constexpr uint64_t kArrayHashMaxSize = 2L << 20;
  enum class HashMode { kHash, kArray, kNormalizedKey };

  // Keeps track of results returned from a join table. One batch of
  // keys can produce multiple batches of results. This is initialized
  // from HashLookup, which is expected to stay constant while 'this'
  // is being used.
  struct JoinResultIterator {
    void reset(const HashLookup& lookup) {
      rows = &lookup.rows;
      hits = &lookup.hits;
      nextHit = nullptr;
      lastRowIndex = 0;
    }

    bool atEnd() const {
      return !rows || lastRowIndex == rows->size();
    }

    const raw_vector<vector_size_t>* FOLLY_NULLABLE rows{nullptr};
    const raw_vector<char*>* FOLLY_NULLABLE hits{nullptr};
    char* FOLLY_NULLABLE nextHit{nullptr};
    vector_size_t lastRowIndex{0};
  };

  struct RowsIterator {
    int32_t hashTableIndex_{-1};
    RowContainerIterator rowContainerIterator_;
  };

  /// Takes ownership of 'hashers'. These are used to keep key-level
  /// encodings like distinct values, ranges. These are stateful for
  /// kArray and kNormalizedKey hash modes and track the data
  /// population while adding payload for either aggregation or join
  /// build.
  explicit BaseHashTable(std::vector<std::unique_ptr<VectorHasher>>&& hashers)
      : hashers_(std::move(hashers)) {}

  virtual ~BaseHashTable() = default;

  virtual HashStringAllocator* FOLLY_NULLABLE stringAllocator() = 0;

  /// Finds or creates a group for each key in 'lookup'. The keys are
  /// returned in 'lookup.hits'.
  virtual void groupProbe(HashLookup& lookup) = 0;

  /// Returns the first hit for each key in 'lookup'. The keys are in
  /// 'lookup.hits' with a nullptr representing a miss. This is for use in hash
  /// join probe. Use listJoinResults to iterate over the results.
  virtual void joinProbe(HashLookup& lookup) = 0;

  /// Fills 'hits' with consecutive hash join results. The corresponding element
  /// of 'inputRows' is set to the corresponding row number in probe keys.
  /// Returns the number of hits produced. If this s less than hits.size() then
  /// all the hits have been produced.
  /// Adds input rows without a match to 'inputRows' with corresponding hit
  /// set to nullptr if 'includeMisses' is true. Otherwise, skips input rows
  /// without a match. 'includeMisses' is set to true when listing results for
  /// the LEFT join.
  virtual int32_t listJoinResults(
      JoinResultIterator& iter,
      bool includeMisses,
      folly::Range<vector_size_t*> inputRows,
      folly::Range<char**> hits) = 0;

  /// Returns rows with 'probed' flag unset. Used by the right/full join.
  virtual int32_t listNotProbedRows(
      RowsIterator* FOLLY_NULLABLE iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NULLABLE* FOLLY_NULLABLE rows) = 0;

  /// Returns rows with 'probed' flag set. Used by the right semi join.
  virtual int32_t listProbedRows(
      RowsIterator* FOLLY_NULLABLE iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NULLABLE* FOLLY_NULLABLE rows) = 0;

  virtual void prepareJoinTable(
      std::vector<std::unique_ptr<BaseHashTable>> tables,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) = 0;

  /// Returns the memory footprint in bytes for any data structures
  /// owned by 'this'.
  virtual int64_t allocatedBytes() const = 0;

  /// Deletes any content of 'this' but does not free the memory. Can
  /// be used for flushing a partial group by, for example.
  virtual void clear() = 0;

  /// Returns the number of rows in a group by or hash join build
  /// side. This is used for sizing the internal hash table.
  virtual uint64_t numDistinct() const = 0;

  // Returns table growth in bytes after adding 'numNewDistinct' distinct
  // entries. This only concerns the hash table, not the payload rows.
  virtual uint64_t hashTableSizeIncrease(int32_t numNewDistinct) const = 0;

  /// Returns true if the hash table contains rows with duplicate keys.
  virtual bool hasDuplicateKeys() const = 0;

  /// Returns the hash mode. This is needed for the caller to calculate
  /// the hash numbers using the appropriate method of the
  /// VectorHashers of 'this'.
  virtual HashMode hashMode() const = 0;

  /// Disables use of array or normalized key hash modes.
  void forceGenericHashMode() {
    setHashMode(HashMode::kHash, 0);
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
  /// distinct entries before needing to rehash.
  virtual void decideHashMode(int32_t numNew) = 0;

  // Removes 'rows'  from the hash table and its RowContainer. 'rows' must exist
  // and be unique.
  virtual void erase(folly::Range<char**> rows) = 0;

  /// Returns a brief description for use in debugging.
  virtual std::string toString() = 0;

  static void
  storeTag(uint8_t* FOLLY_NULLABLE tags, int32_t index, uint8_t tag) {
    tags[index] = tag;
  }

  const std::vector<std::unique_ptr<VectorHasher>>& hashers() const {
    return hashers_;
  }

  RowContainer* FOLLY_NULLABLE rows() const {
    return rows_.get();
  }

  std::unique_ptr<RowContainer> moveRows() {
    return std::move(rows_);
  }

  // Static functions for processing internals. Public because used in
  // structs that define probe and insert algorithms. These are
  // concentrated here to abstract away data layout, e.g tags and
  // payload pointers separate/interleaved.

  /// Extracts a 7 bit tag from a hash number. The high bit is always set.
  static uint8_t hashTag(uint64_t hash) {
    return static_cast<uint8_t>(hash >> 32) | 0x80;
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
  loadTags(uint8_t* FOLLY_NULLABLE tags, int32_t tagIndex) {
    // Cannot use xsimd::batch::unaligned here because we need to skip TSAN.
    auto src = tags + tagIndex;
#if XSIMD_WITH_SSE2
    return TagVector(_mm_loadu_si128(reinterpret_cast<__m128i const*>(src)));
#elif XSIMD_WITH_NEON
    return TagVector(vld1q_u8(src));
#endif
  }

  /// Loads the payload row pointer corresponding to the tag at 'index'.
  static char* FOLLY_NULLABLE
  loadRow(char* FOLLY_NULLABLE* FOLLY_NULLABLE table, int32_t index) {
    return table[index];
  }

 protected:
  virtual void setHashMode(HashMode mode, int32_t numNew) = 0;
  std::vector<std::unique_ptr<VectorHasher>> hashers_;
  std::unique_ptr<RowContainer> rows_;
};

class ProbeState;

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
      const std::vector<std::unique_ptr<Aggregate>>& aggregates,
      const std::vector<TypePtr>& dependentTypes,
      bool allowDuplicates,
      bool isJoinBuild,
      bool hasProbedFlag,
      memory::MappedMemory* FOLLY_NULLABLE memory);

  static std::unique_ptr<HashTable> createForAggregation(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      const std::vector<std::unique_ptr<Aggregate>>& aggregates,
      memory::MappedMemory* FOLLY_NULLABLE memory) {
    return std::make_unique<HashTable>(
        std::move(hashers),
        aggregates,
        std::vector<TypePtr>{},
        false, // allowDuplicates
        false, // isJoinBuild
        false, // hasProbedFlag
        memory);
  }

  static std::unique_ptr<HashTable> createForJoin(
      std::vector<std::unique_ptr<VectorHasher>>&& hashers,
      const std::vector<TypePtr>& dependentTypes,
      bool allowDuplicates,
      bool hasProbedFlag,
      memory::MappedMemory* FOLLY_NULLABLE memory) {
    static const std::vector<std::unique_ptr<Aggregate>> kNoAggregates;
    return std::make_unique<HashTable>(
        std::move(hashers),
        kNoAggregates,
        dependentTypes,
        allowDuplicates,
        true, // isJoinBuild
        hasProbedFlag,
        memory);
  }

  virtual ~HashTable() override = default;

  void groupProbe(HashLookup& lookup) override;

  void joinProbe(HashLookup& lookup) override;

  int32_t listJoinResults(
      JoinResultIterator& iter,
      bool includeMisses,
      folly::Range<vector_size_t*> inputRows,
      folly::Range<char**> hits) override;

  int32_t listNotProbedRows(
      RowsIterator* FOLLY_NULLABLE iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NULLABLE* FOLLY_NULLABLE rows) override;

  int32_t listProbedRows(
      RowsIterator* FOLLY_NULLABLE iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NULLABLE* FOLLY_NULLABLE rows) override;

  void clear() override;

  int64_t allocatedBytes() const override {
    // for each row: 1 byte per tag + sizeof(Entry) per table entry + memory
    // allocated with MappedMemory for fixed-width rows and strings.
    return (1 + sizeof(char*)) * size_ + rows_->allocatedBytes();
  }

  HashStringAllocator* FOLLY_NULLABLE stringAllocator() override {
    return &rows_->stringAllocator();
  }

  uint64_t numDistinct() const override {
    return numDistinct_;
  }

  bool hasDuplicateKeys() const override {
    return hasDuplicates_;
  }

  HashMode hashMode() const override {
    return hashMode_;
  }

  void decideHashMode(int32_t numNew) override;

  void erase(folly::Range<char**> rows) override;

  // Moves the contents of 'tables' into 'this' and prepares 'this'
  // for use in hash join probe. A hash join build side is prepared as
  // follows: 1. Each build side thread gets a random selection of the
  // build stream. Each accumulates rows into its own
  // HashTable'sRowContainer and updates the VectorHashers of the
  // table to reflect the data as long as the data shows promise for
  // kArray or kNormalizedKey representation. After all the build
  // tables are filled, they are combined into one top level table
  // with prepareJoinTable. This then takes ownership of all the data
  // and VectorHashers and decides the hash mode and representation.
  void prepareJoinTable(
      std::vector<std::unique_ptr<BaseHashTable>> tables,
      folly::Executor* FOLLY_NULLABLE executor = nullptr) override;

  uint64_t hashTableSizeIncrease(int32_t numNewDistinct) const override {
    if (numDistinct_ + numNewDistinct > rehashSize()) {
      // If rehashed, the table adds size_ entries (i.e. doubles),
      // adding one pointer and one tag byte for each new position.
      return size_ * (sizeof(void*) + 1);
    }
    return 0;
  }

  std::string toString() override;

 private:
  // Returns the number of entries after which the table gets rehashed.
  uint64_t rehashSize() const {
    // This implements the F14 load factor: Resize if less than 1/8 unoccupied.
    return size_ - (size_ / 8);
  }

  template <RowContainer::ProbeType probeType>
  int32_t listRows(
      RowsIterator* FOLLY_NULLABLE iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NULLABLE* FOLLY_NULLABLE rows);

  char* FOLLY_NULLABLE& nextRow(char* FOLLY_NULLABLE row) {
    return *reinterpret_cast<char**>(row + nextOffset_);
  }

  void arrayGroupProbe(HashLookup& lookup);

  void setHashMode(HashMode mode, int32_t numNew) override;

  /// Tries to use as many range hashers as can in a normalized key situation.
  void enableRangeWhereCan(
      const std::vector<uint64_t>& rangeSizes,
      const std::vector<uint64_t>& distinctSizes,
      std::vector<bool>& useRange);

  /// Sets  value ranges or distinct value ids mode for
  /// VectorHashers in a kArray or kNormalizedKeys mode table.
  uint64_t setHasherMode(
      const std::vector<std::unique_ptr<VectorHasher>>& hashers,
      const std::vector<bool>& useRange,
      const std::vector<uint64_t>& rangeSizes,
      const std::vector<uint64_t>& distinctSizes);

  // Clears all elements of 'useRange' except ones that correspond to boolean
  // VectorHashers.
  void clearUseRange(std::vector<bool>& useRange);

  void rehash();
  void storeKeys(HashLookup& lookup, vector_size_t row);

  void storeRowPointer(int32_t index, uint64_t hash, char* FOLLY_NULLABLE row);

  // Allocates new tables for tags and payload pointers. The size must
  // a power of 2.
  void allocateTables(uint64_t size);

  void checkSize(int32_t numNew);

  // Computes hash numbers of the appropriate hash mode for 'groups',
  // stores these in 'hashes' and inserts the groups using
  // insertForJoin or insertForGroupBy.
  bool insertBatch(
      char* FOLLY_NULLABLE* FOLLY_NULLABLE groups,
      int32_t numGroups,
      raw_vector<uint64_t>& hashes);

  // Inserts 'numGroups' entries into 'this'. 'groups' point to
  // contents in a RowContainer owned by 'this'. 'hashes' are the hash
  // numbers or array indices (if kArray mode) for each
  // group. Duplicate key rows are chained via their next link. if
  // parallel build, partitionEnd is the index of the first entry
  // after the partition being inserted. If a row would be inserted to
  // the right of the end, it is not inserted but rather added to the
  // end of 'overflows'.
  void insertForJoin(
      char* FOLLY_NULLABLE* FOLLY_NULLABLE groups,
      uint64_t* FOLLY_NULLABLE hashes,
      int32_t numGroups,
      int32_t partitionBegin = 0,
      int32_t partitionEnd = std::numeric_limits<int32_t>::max(),
      std::vector<char*>* FOLLY_NULLABLE overflows = nullptr);

  // Inserts 'numGroups' entries into 'this'. 'groups' point to
  // contents in a RowContainer owned by 'this'. 'hashes' are the hash
  // numbers or array indices (if kArray mode) for each
  // group. 'groups' is expected to have no duplicate keys.
  void insertForGroupBy(
      char* FOLLY_NULLABLE* FOLLY_NULLABLE groups,
      uint64_t* FOLLY_NULLABLE hashes,
      int32_t numGroups);

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
  void buildJoinPartition(uint8_t partition, std::vector<char*>& overflow);

  // Assigns a partition to each row of 'subtable' in RowPartitions of
  // subtable's RowContainer. If 'hashMode_' is kNormalizedKeys, records the
  // normalized key of each row below the row in its container.
  void partitionRows(HashTable<ignoreNullKeys>& subtable);

  // Calculates hashes for 'rows' and returns them in 'hashes'. If
  // 'initNormalizedKeys' is true, the normalized keys are stored
  // below each row in the container. If 'initNormalizedKeys' is false
  // and the table is in normalized keys mode, the keys are retrieved
  // from the row and the hash is made from this, without recomputing
  // the normalized key. Returns false if the hash keys are not mappable via the
  // VectorHashers.
  bool hashRows(
      folly::Range<char**> rows,
      bool initNormalizedKeys,
      raw_vector<uint64_t>& hashes);

  char* FOLLY_NULLABLE
  insertEntry(HashLookup& lookup, int32_t index, vector_size_t row);

  bool compareKeys(
      const char* FOLLY_NULLABLE group,
      HashLookup& lookup,
      vector_size_t row);

  bool compareKeys(
      const char* FOLLY_NULLABLE group,
      const char* FOLLY_NULLABLE inserted);

  template <bool isJoin>
  void fullProbe(HashLookup& lookup, ProbeState& state, bool extraCheck);

  // Adds a row to a hash join table in kArray hash mode. Returns true
  // if a new entry was made and false if the row was added to an
  // existing set of rows with the same key.
  bool arrayPushRow(char* FOLLY_NULLABLE row, int32_t index);

  // Adds a row to a hash join build side entry with multiple rows
  // with the same key.
  void pushNext(char* FOLLY_NULLABLE row, char* FOLLY_NULLABLE next);

  // Finishes inserting an entry into a join hash table. If the insert
  // would fall outside of 'partitionBegin' ... 'partitionEnd', the
  // insert is not made but the row is instead added to 'overflow'.
  void buildFullProbe(
      ProbeState& state,
      uint64_t hash,
      char* FOLLY_NULLABLE row,
      bool extraCheck,
      int32_t partitionBegin,
      int32_t partitionEnd,
      std::vector<char*>* FOLLY_NULLABLE overflows);

  // Updates 'hashers_' to correspond to the keys in the
  // content. Returns true if all hashers offer a mapping to value ids
  // for array or normalized key.
  bool analyze();
  // Erases the entries of rows from the hash table and its RowContainer.
  // 'hashes' must be computed according to 'hashMode_'.
  void eraseWithHashes(
      folly::Range<char**> rows,
      uint64_t* FOLLY_NULLABLE hashes);

  // Returns the percentage of values to reserve for new keys in range
  // or distinct mode VectorHashers in a group by hash table. 0 for
  // join build sides.
  int32_t reservePct() const {
    return isJoinBuild_ ? 0 : 50;
  }

  int8_t sizeBits_;
  bool isJoinBuild_ = false;

  // Set at join build time if the table has duplicates, meaning that
  // the join can be cardinality increasing. Atomic for tsan because
  // many threads can set this.
  std::atomic<bool> hasDuplicates_{false};

  // Offset of next row link for join build side, 0 if none. Copied
  // from 'rows_'.
  int32_t nextOffset_;
  uint8_t* FOLLY_NULLABLE tags_ = nullptr;
  char* FOLLY_NULLABLE* FOLLY_NULLABLE table_ = nullptr;
  memory::MappedMemory::ContiguousAllocation tableAllocation_;
  int64_t size_ = 0;
  int64_t sizeMask_ = 0;
  int64_t numDistinct_ = 0;
  HashMode hashMode_ = HashMode::kArray;
  // Owns the memory of multiple build side hash join tables that are
  // combined into a single probe hash table.
  std::vector<std::unique_ptr<HashTable<ignoreNullKeys>>> otherTables_;

  // Bounds of independently buildable index ranges in the table. The
  // range of partition i starts at [i] and ends at [i +1]. Bounds are multiple
  // of cache line  size.
  raw_vector<int32_t> buildPartitionBounds_;

  // Executor for parallelizing hash join build. This may be the
  // executor for Drivers. If this executor is indefinitely taken by
  // other work, the thread of prepareJoinTables() will sequentially
  // execute the parallel build steps.
  folly::Executor* FOLLY_NULLABLE buildExecutor_{nullptr};

  //  Counts parallel build rows. Used for consistency check.
  std::atomic<int64_t> numParallelBuildRows_{0};
};

} // namespace facebook::velox::exec
