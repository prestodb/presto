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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/common/memory/MappedMemory.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/ContainerRowSerde.h"
#include "velox/exec/Spill.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"
namespace facebook::velox::exec {

using normalized_key_t = uint64_t;

struct RowContainerIterator {
  int32_t allocationIndex = 0;
  int32_t runIndex = 0;
  int32_t rowOffset = 0;
  // Number of unvisited entries that are prefixed by an uint64_t for
  // normalized key. Set in listRows() on first call.
  int64_t normalizedKeysLeft = 0;

  // Ordinal position of 'currentRow' in RowContainer.
  int32_t rowNumber{0};
  char* FOLLY_NULLABLE rowBegin{nullptr};
  // First byte after the end of the PageRun containing 'currentRow'.
  char* FOLLY_NULLABLE endOfRun{nullptr};

  // Returns the current row, skipping a possible normalized key below the first
  // byte of row.
  inline char* FOLLY_NULLABLE currentRow() const {
    return (rowBegin && normalizedKeysLeft)
        ? rowBegin + sizeof(normalized_key_t)
        : rowBegin;
  }

  void reset() {
    allocationIndex = 0;
    runIndex = 0;
    rowOffset = 0;
    normalizedKeysLeft = 0;
    rowBegin = nullptr;
    rowNumber = 0;
    endOfRun = nullptr;
  }
};

/// Container with a 8-bit partition number field for each row in a
/// RowContainer. The partition number bytes correspond 1:1 to rows. Used only
/// for parallel hash join build.
class RowPartitions {
 public:
  /// Initializes this to hold up to 'numRows'.
  RowPartitions(int32_t numRows, memory::MappedMemory& mappedMemory);

  /// Appends 'partitions' to the end of 'this'. Throws if adding more than the
  /// capacity given at construction.
  void appendPartitions(folly::Range<const uint8_t*> partitions);

  auto& allocation() const {
    return allocation_;
  }

  int32_t size() const {
    return size_;
  }

 private:
  const int32_t capacity_;

  // Number of partition numbers added.
  int32_t size_{0};

  // Partition numbers. 1 byte each.
  memory::MappedMemory::Allocation allocation_;
};

// Packed representation of offset, null byte offset and null mask for
// a column inside a RowContainer.
class RowColumn {
 public:
  // Used as null offset for a non-null column.
  static constexpr int32_t kNotNullOffset = -1;

  RowColumn(int32_t offset, int32_t nullOffset)
      : packedOffsets_(PackOffsets(offset, nullOffset)) {}
  int32_t offset() const {
    return packedOffsets_ >> 32;
  }

  int32_t nullByte() const {
    return static_cast<uint32_t>(packedOffsets_) >> 8;
  }

  uint8_t nullMask() const {
    return packedOffsets_ & 0xff;
  }

 private:
  static uint64_t PackOffsets(int32_t offset, int32_t nullOffset) {
    if (nullOffset == kNotNullOffset) {
      // If the column is not nullable, The low word is 0, meaning
      // that a null check will AND 0 to the 0th byte of the row,
      // which is always false and always safe to do.
      return static_cast<uint64_t>(offset) << 32;
    }
    return (1UL << (nullOffset & 7)) | ((nullOffset & ~7UL) << 5) |
        static_cast<uint64_t>(offset) << 32;
  }

  const uint64_t packedOffsets_;
};

// Collection of rows for aggregation, hash join, order by
class RowContainer {
 public:
  static constexpr uint64_t kUnlimited = std::numeric_limits<uint64_t>::max();
  using Eraser = std::function<void(folly::Range<char**> rows)>;

  // 'keyTypes' gives the type of row and use 'mappedMemory' for bulk
  // allocation.
  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      memory::MappedMemory* FOLLY_NONNULL mappedMemory)
      : RowContainer(keyTypes, std::vector<TypePtr>{}, mappedMemory) {}

  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes,
      memory::MappedMemory* FOLLY_NONNULL mappedMemory)
      : RowContainer(
            keyTypes,
            true, // nullableKeys
            emptyAggregates(),
            dependentTypes,
            false, // hasNext
            false, // isJoinBuild
            false, // hasProbedFlag
            false, // hasNormalizedKey
            mappedMemory,
            ContainerRowSerde::instance()) {}

  // 'keyTypes' gives the type of the key of each row. For a group by,
  // order by or right outer join build side these may be
  // nullable. 'nullableKeys' specifies if these have a null flag.
  // 'aggregates' is a vector of Aggregate for a group by payload,
  // empty otherwise. 'DependentTypes' gives the types of non-key
  // columns for a hash join build side or an order by. 'hasNext' is
  // true for a hash join build side where keys can be
  // non-unique. 'isJoinBuild' is true for hash join build sides. This
  // implies that hashing of keys ignores null keys even if these were
  // allowed. 'hasProbedFlag' indicates that an extra bit is reserved
  // for a probed state of a full or right outer
  // join. 'hasNormalizedKey' specifies that an extra word is left
  // below each row for a normalized key that collapses all parts
  // into one word for faster comparison. The bulk allocation is done
  // from 'mappedMemory'.  'serde_' is used for serializing complex
  // type values into the container.
  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      bool nullableKeys,
      const std::vector<std::unique_ptr<Aggregate>>& aggregates,
      const std::vector<TypePtr>& dependentTypes,
      bool hasNext,
      bool isJoinBuild,
      bool hasProbedFlag,
      bool hasNormalizedKey,
      memory::MappedMemory* FOLLY_NONNULL mappedMemory,
      const RowSerde& serde);

  // Allocates a new row and initializes possible aggregates to null.
  char* FOLLY_NONNULL newRow();

  uint32_t rowSize(const char* FOLLY_NONNULL row) const {
    return fixedRowSize_ +
        (rowSizeOffset_
             ? *reinterpret_cast<const uint32_t*>(row + rowSizeOffset_)
             : 0);
  }

  // The row size excluding any out-of-line stored variable length values.
  int32_t fixedRowSize() const {
    return fixedRowSize_;
  }

  // Adds 'rows' to the free rows list and frees any associated
  // variable length data.
  void eraseRows(folly::Range<char**> rows);

  void incrementRowSize(char* FOLLY_NONNULL row, uint64_t bytes) {
    uint32_t* ptr = reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
    uint64_t size = *ptr + bytes;
    *ptr = std::min<uint64_t>(size, std::numeric_limits<uint32_t>::max());
  }

  // Initialize row. 'reuse' specifies whether the 'row' is reused or
  // not. If it is reused, it will free memory associated with the row
  // elsewhere (such as in HashStringAllocator).
  char* FOLLY_NONNULL initializeRow(char* FOLLY_NONNULL row, bool reuse);

  // Stores the 'index'th value in 'decoded' into 'row' at
  // 'columnIndex'.
  void store(
      const DecodedVector& decoded,
      vector_size_t index,
      char* FOLLY_NONNULL row,
      int32_t columnIndex);

  HashStringAllocator& stringAllocator() {
    return stringAllocator_;
  }

  // Returns the number of used rows in 'this'. This is the number of
  // rows a RowContainerIterator would access.
  int64_t numRows() const {
    return numRows_;
  }

  /// Copies the values at 'col' into 'result' (starting at 'resultOffset')
  /// for the 'numRows' rows pointed to by 'rows'. If a 'row' is null, sets
  /// corresponding row in 'result' to null.
  static void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      RowColumn col,
      vector_size_t resultOffset,
      VectorPtr& result);

  // Copies the values at 'col' into 'result' for the
  // 'numRows' rows pointed to by 'rows'. If an entry in 'rows' is null, sets
  // corresponding row in 'result' to null.
  static void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      RowColumn col,
      VectorPtr& result) {
    extractColumn(rows, numRows, col, 0, result);
  }

  /// Copies the values from the array pointed to by 'rows' at 'col' into
  /// 'result' (starting at 'resultOffset') for the rows at positions in
  /// the 'rowNumbers' array. If a 'row' is null, sets
  /// corresponding row in 'result' to null. The positions in 'rowNumbers'
  /// array can repeat and also appear out of order.
  static void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      RowColumn col,
      vector_size_t resultOffset,
      VectorPtr& result);

  // Copies the values at 'columnIndex' into 'result' for the
  // 'numRows' rows pointed to by 'rows'. If an entry in 'rows' is null, sets
  //  corresponding row in 'result' to null.
  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      int32_t columnIndex,
      VectorPtr& result) {
    extractColumn(rows, numRows, columnAt(columnIndex), result);
  }

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the 'numRows' rows pointed to by 'rows'. If an
  /// entry in 'rows' is null, sets corresponding row in 'result' to null.
  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      int32_t numRows,
      int32_t columnIndex,
      int32_t resultOffset,
      VectorPtr& result) {
    extractColumn(rows, numRows, columnAt(columnIndex), resultOffset, result);
  }

  /// Copies the values at 'columnIndex' at positions in the 'rowNumbers' array
  /// for the rows pointed to by 'rows'. The values are copied into the 'result'
  /// vector at the offset pointed by 'resultOffset'. If an entry in 'rows'
  /// is null, sets corresponding row in 'result' to null.
  void extractColumn(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t columnIndex,
      const vector_size_t resultOffset,
      VectorPtr& result) {
    extractColumn(
        rows, rowNumbers, columnAt(columnIndex), resultOffset, result);
  }

  static inline int32_t nullByte(int32_t nullOffset) {
    return nullOffset / 8;
  }

  static inline uint8_t nullMask(int32_t nullOffset) {
    return 1 << (nullOffset & 7);
  }

  // No tsan because probed flags may have been set by a different
  // thread. There is a barrier but tsan does not know this.
  enum class ProbeType { kAll, kProbed, kNotProbed };

  template <ProbeType probeType>
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
  __attribute__((__no_sanitize__("thread")))
#endif
#endif
  int32_t
  listRows(
      RowContainerIterator* FOLLY_NONNULL iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NONNULL* FOLLY_NONNULL rows) {
    int32_t count = 0;
    uint64_t totalBytes = 0;
    VELOX_CHECK_EQ(rows_.numLargeAllocations(), 0);
    auto numAllocations = rows_.numSmallAllocations();
    if (iter->allocationIndex == 0 && iter->runIndex == 0 &&
        iter->rowOffset == 0) {
      iter->normalizedKeysLeft = numRowsWithNormalizedKey_;
    }
    int32_t rowSize = fixedRowSize_ +
        (iter->normalizedKeysLeft > 0 ? sizeof(normalized_key_t) : 0);
    for (auto i = iter->allocationIndex; i < numAllocations; ++i) {
      auto allocation = rows_.allocationAt(i);
      auto numRuns = allocation->numRuns();
      for (auto runIndex = iter->runIndex; runIndex < numRuns; ++runIndex) {
        memory::MappedMemory::PageRun run = allocation->runAt(runIndex);
        auto data = run.data<char>();
        int64_t limit;
        if (i == numAllocations - 1 && runIndex == rows_.currentRunIndex()) {
          limit = rows_.currentOffset();
        } else {
          limit = run.numPages() * memory::MappedMemory::kPageSize;
        }
        auto row = iter->rowOffset;
        while (row + rowSize <= limit) {
          rows[count++] = data + row +
              (iter->normalizedKeysLeft > 0 ? sizeof(normalized_key_t) : 0);
          row += rowSize;
          if (--iter->normalizedKeysLeft == 0) {
            rowSize -= sizeof(normalized_key_t);
          }
          if (bits::isBitSet(rows[count - 1], freeFlagOffset_)) {
            --count;
            continue;
          }
          if constexpr (probeType == ProbeType::kNotProbed) {
            if (bits::isBitSet(rows[count - 1], probedFlagOffset_)) {
              --count;
              continue;
            }
          }
          if constexpr (probeType == ProbeType::kProbed) {
            if (not(bits::isBitSet(rows[count - 1], probedFlagOffset_))) {
              --count;
              continue;
            }
          }
          totalBytes += rowSize;
          if (rowSizeOffset_) {
            totalBytes += variableRowSize(rows[count - 1]);
          }
          if (count == maxRows || totalBytes > maxBytes) {
            iter->rowOffset = row;
            iter->runIndex = runIndex;
            iter->allocationIndex = i;
            return count;
          }
        }
        iter->rowOffset = 0;
      }
      iter->runIndex = 0;
    }
    iter->allocationIndex = std::numeric_limits<int32_t>::max();
    return count;
  }

  // Extracts up to 'maxRows' rows starting at the position of
  // 'iter'. A default constructed or reset iter starts at the
  // beginning. Returns the number of rows written to 'rows'. Returns
  // 0 when at end. Stops after the total size of returned rows exceeds
  // maxBytes.
  int32_t listRows(
      RowContainerIterator* FOLLY_NONNULL iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char* FOLLY_NONNULL* FOLLY_NONNULL rows) {
    return listRows<ProbeType::kAll>(iter, maxRows, maxBytes, rows);
  }

  int32_t listRows(
      RowContainerIterator* FOLLY_NONNULL iter,
      int32_t maxRows,
      char* FOLLY_NONNULL* FOLLY_NONNULL rows) {
    return listRows<ProbeType::kAll>(iter, maxRows, kUnlimited, rows);
  }

  /// Sets 'probed' flag for the specified rows. Used by the right and
  /// full join to mark build-side rows that matches join
  /// condition. 'rows' may contain duplicate entries for the cases
  /// where single probe row matched multiple build rows. In case of
  /// the full join, 'rows' may include null entries that correspond
  /// to probe rows with no match. No tsan because any thread can set
  /// this without synchronization. There is a barrier between setting
  /// and reading.
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
  __attribute__((__no_sanitize__("thread")))
#endif
#endif
  void
  setProbedFlag(char* FOLLY_NONNULL* FOLLY_NONNULL rows, int32_t numRows);

  // Returns true if 'row' at 'column' equals the value at 'index' in
  // 'decoded'. 'mayHaveNulls' specifies if nulls need to be checked. This is
  // a fast path for compare().
  template <bool mayHaveNulls>
  bool equals(
      const char* FOLLY_NONNULL row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index);

  // Compares the value at 'column' in 'row' with the value at 'index'
  // in 'decoded'. Returns 0 for equal, < 0 for 'row' < 'decoded', > 0
  // otherwise.
  int32_t compare(
      const char* FOLLY_NONNULL row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags = CompareFlags());

  // Compares the value at 'columnIndex' between 'left' and 'right'. Returns
  // 0 for equal, < 0 for left < right, > 0 otherwise.
  int32_t compare(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      int32_t columnIndex,
      CompareFlags flags = CompareFlags());

  // Allows get/set of the normalized key. If normalized keys are
  // used, they are stored in the word immediately below the hash
  // table row.
  static inline normalized_key_t& normalizedKey(char* FOLLY_NONNULL group) {
    return reinterpret_cast<normalized_key_t*>(group)[-1];
  }

  void disableNormalizedKeys() {
    normalizedKeySize_ = 0;
  }

  RowColumn columnAt(int32_t index) const {
    return rowColumns_[index];
  }

  // Bit offset of the probed flag for a full or right outer join  payload.
  // 0 if not applicable.
  int32_t probedFlagOffset() const {
    return probedFlagOffset_;
  }

  // Returns the offset of a uint32_t row size or 0 if the row has no
  // variable width fields or accumulators.
  int32_t rowSizeOffset() const {
    return rowSizeOffset_;
  }

  // For a hash join table with possible non-unique entries, the offset of
  // the pointer to the next row with the same key. 0 if keys are
  // guaranteed unique, e.g. for a group by or semijoin build.
  int32_t nextOffset() const {
    return nextOffset_;
  }

  // Hashes the values of 'columnIndex' for 'rows'.  If 'mix' is true,
  // mixes the hash with the existing value in 'result'.
  void hash(
      int32_t columnIndex,
      folly::Range<char**> rows,
      bool mix,
      uint64_t* FOLLY_NONNULL result);

  uint64_t allocatedBytes() const {
    return rows_.allocatedBytes() + stringAllocator_.retainedSize();
  }

  // Returns the number of fixed size rows that can be allocated
  // without growing the container and the number of unused bytes of
  // reserved storage for variable length data.
  std::pair<uint64_t, uint64_t> freeSpace() const {
    return std::make_pair<uint64_t, uint64_t>(
        rows_.availableInRun() / fixedRowSize_ + numFreeRows_,
        stringAllocator_.freeSpace());
  }

  // Returns a cap on  extra memory that may be needed when adding 'numRows'
  // and variableLengthBytes of out-of-line variable length data.
  int64_t sizeIncrement(vector_size_t numRows, int64_t variableLengthBytes)
      const;

  // Resets the state to be as after construction. Frees memory for payload.
  void clear();

  int32_t compareRows(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      const std::vector<CompareFlags>& flags = {}) {
    VELOX_DCHECK(flags.empty() || flags.size() == keyTypes_.size());
    for (auto i = 0; i < keyTypes_.size(); ++i) {
      auto result =
          compare(left, right, i, flags.empty() ? CompareFlags() : flags[i]);
      if (result) {
        return result;
      }
    }
    return 0;
  }

  // Returns estimated number of rows a batch can support for
  // the given batchSizeInBytes.
  // FIXME(venkatra): estimate num rows for variable length fields.
  int32_t estimatedNumRowsPerBatch(int32_t batchSizeInBytes) {
    return (batchSizeInBytes / fixedRowSize_) +
        ((batchSizeInBytes % fixedRowSize_) ? 1 : 0);
  }

  // Extract column values for 'rows' into 'result'.
  void extractRows(
      const std::vector<char * FOLLY_NONNULL>& rows,
      const RowVectorPtr& result) {
    VELOX_CHECK_EQ(rows.size(), result->size());
    if (rows.empty()) {
      return;
    }
    for (int i = 0; i < result->childrenSize(); ++i) {
      RowContainer::extractColumn(
          rows.data(), rows.size(), columnAt(i), result->childAt(i));
    }
  }

  memory::MappedMemory* FOLLY_NONNULL mappedMemory() const {
    return stringAllocator_.mappedMemory();
  }

  // Returns the types of all non-aggregate columns of 'this', keys first.
  const auto& columnTypes() const {
    return types_;
  }

  const auto& keyTypes() const {
    return keyTypes_;
  }

  const auto& aggregates() const {
    return aggregates_;
  }

  auto numFreeRows() const {
    return numFreeRows_;
  }

  const HashStringAllocator& stringAllocator() const {
    return stringAllocator_;
  }

  // Checks that row and free row counts match and that free list
  // membership is consistent with free flag.
  void checkConsistency();

  static inline bool
  isNullAt(const char* FOLLY_NONNULL row, int32_t nullByte, uint8_t nullMask) {
    return (row[nullByte] & nullMask) != 0;
  }

  /// Retrieves rows from 'iterator' whose partition equals
  /// 'partition'. Writes up to 'maxRows' pointers to the rows in
  /// 'result'. Returns the number of rows retrieved, 0 when no more
  /// rows are found. 'iterator' is expected to be in initial state
  /// on first call.
  int32_t listPartitionRows(
      RowContainerIterator& iterator,
      uint8_t partition,
      int32_t maxRows,
      char* FOLLY_NONNULL* FOLLY_NONNULL result);

  /// Returns a container with a partition number for each row. This
  /// is created on first use. The caller is responsible for filling
  /// this.
  RowPartitions& partitions();

  /// Advances 'iterator' by 'numRows'. The current row after skip is
  /// in iter.currentRow(). This is null if past end. Public for testing.
  void skip(RowContainerIterator& iterator, int32_t numRows);

 private:
  // Offset of the pointer to the next free row on a free row.
  static constexpr int32_t kNextFreeOffset = 0;

  template <typename T>
  static inline T valueAt(const char* FOLLY_NONNULL group, int32_t offset) {
    return *reinterpret_cast<const T*>(group + offset);
  }

  template <typename T>
  static inline T& valueAt(char* FOLLY_NONNULL group, int32_t offset) {
    return *reinterpret_cast<T*>(group + offset);
  }

  template <TypeKind Kind>
  static void extractColumnTyped(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      int32_t resultOffset,
      VectorPtr& result) {
    if (rowNumbers.size() > 0) {
      extractColumnTypedInternal<true, Kind>(
          rows, rowNumbers, rowNumbers.size(), column, resultOffset, result);
    } else {
      extractColumnTypedInternal<false, Kind>(
          rows, rowNumbers, numRows, column, resultOffset, result);
    }
  }

  template <bool useRowNumbers, TypeKind Kind>
  static void extractColumnTypedInternal(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      int32_t resultOffset,
      VectorPtr& result) {
    // Resize the result vector before all copies.
    result->resize(numRows + resultOffset);

    if (Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      extractComplexType<useRowNumbers>(
          rows, rowNumbers, numRows, column, resultOffset, result);
      return;
    }
    using T = typename KindToFlatVector<Kind>::HashRowType;
    auto* flatResult = result->as<FlatVector<T>>();
    auto nullMask = column.nullMask();
    auto offset = column.offset();
    if (!nullMask) {
      extractValuesNoNulls<useRowNumbers, T>(
          rows, rowNumbers, numRows, offset, resultOffset, flatResult);
    } else {
      extractValuesWithNulls<useRowNumbers, T>(
          rows,
          rowNumbers,
          numRows,
          offset,
          column.nullByte(),
          nullMask,
          resultOffset,
          flatResult);
    }
  }

  char* FOLLY_NULLABLE& nextFree(char* FOLLY_NONNULL row) {
    return *reinterpret_cast<char**>(row + kNextFreeOffset);
  }

  uint32_t& variableRowSize(char* FOLLY_NONNULL row) {
    DCHECK(rowSizeOffset_);
    return *reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
  }

  template <TypeKind Kind>
  inline void storeWithNulls(
      const DecodedVector& decoded,
      vector_size_t index,
      char* FOLLY_NONNULL row,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask) {
    using T = typename TypeTraits<Kind>::NativeType;
    if (decoded.isNullAt(index)) {
      row[nullByte] |= nullMask;
      // Do not leave an uninitialized value in the case of a
      // null. This is an error with valgrind/asan.
      *reinterpret_cast<T*>(row + offset) = T();
      return;
    }
    *reinterpret_cast<T*>(row + offset) = decoded.valueAt<T>(index);
    if constexpr (std::is_same_v<T, StringView>) {
      RowSizeTracker tracker(row[rowSizeOffset_], stringAllocator_);
      stringAllocator_.copyMultipart(row, offset);
    }
  }

  template <TypeKind Kind>
  inline void storeNoNulls(
      const DecodedVector& decoded,
      vector_size_t index,
      char* FOLLY_NONNULL group,
      int32_t offset) {
    using T = typename TypeTraits<Kind>::NativeType;
    *reinterpret_cast<T*>(group + offset) = decoded.valueAt<T>(index);
    if constexpr (std::is_same_v<T, StringView>) {
      RowSizeTracker tracker(group[rowSizeOffset_], stringAllocator_);
      stringAllocator_.copyMultipart(group, offset);
    }
  }

  template <bool useRowNumbers, typename T>
  static void extractValuesWithNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result) {
    auto maxRows = numRows + resultOffset;
    VELOX_DCHECK_LE(maxRows, result->size());

    BufferPtr nullBuffer = result->mutableNulls(maxRows);
    auto nulls = nullBuffer->asMutable<uint64_t>();
    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        row = rows[rowNumbers[i]];
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (row == nullptr || isNullAt(row, nullByte, nullMask)) {
        bits::setNull(nulls, resultIndex, true);
      } else {
        bits::setNull(nulls, resultIndex, false);
        if constexpr (std::is_same_v<T, StringView>) {
          extractString(valueAt<StringView>(row, offset), result, resultIndex);
        } else {
          values[resultIndex] = valueAt<T>(row, offset);
        }
      }
    }
  }

  template <bool useRowNumbers, typename T>
  static void extractValuesNoNulls(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      int32_t offset,
      int32_t resultOffset,
      FlatVector<T>* FOLLY_NONNULL result) {
    auto maxRows = numRows + resultOffset;
    VELOX_DCHECK_LE(maxRows, result->size());
    BufferPtr valuesBuffer = result->mutableValues(maxRows);
    auto values = valuesBuffer->asMutableRange<T>();
    for (int32_t i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        row = rows[rowNumbers[i]];
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (row == nullptr) {
        result->setNull(resultIndex, true);
      } else {
        result->setNull(resultIndex, false);
        if constexpr (std::is_same_v<T, StringView>) {
          extractString(valueAt<StringView>(row, offset), result, resultIndex);
        } else {
          values[resultIndex] = valueAt<T>(row, offset);
        }
      }
    }
  }

  static void prepareRead(
      const char* FOLLY_NONNULL row,
      int32_t offset,
      ByteStream& stream);

  template <TypeKind Kind>
  void hashTyped(
      const Type* FOLLY_NONNULL type,
      RowColumn column,
      bool nullable,
      folly::Range<char**> rows,
      bool mix,
      uint64_t* FOLLY_NONNULL result);

  template <TypeKind Kind>
  inline bool equalsWithNulls(
      const char* FOLLY_NONNULL row,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      const DecodedVector& decoded,
      vector_size_t index) {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    bool rowIsNull = isNullAt(row, nullByte, nullMask);
    bool indexIsNull = decoded.isNullAt(index);
    if (rowIsNull || indexIsNull) {
      return rowIsNull == indexIsNull;
    }
    if (Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(row, offset, decoded, index) == 0;
    }
    if (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      return compareStringAsc(
                 valueAt<StringView>(row, offset), decoded, index) == 0;
    }
    return decoded.valueAt<T>(index) == valueAt<T>(row, offset);
  }

  template <TypeKind Kind>
  inline bool equalsNoNulls(
      const char* FOLLY_NONNULL row,
      int32_t offset,
      const DecodedVector& decoded,
      vector_size_t index) {
    using T = typename KindToFlatVector<Kind>::HashRowType;

    if (Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(row, offset, decoded, index) == 0;
    }
    if (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      return compareStringAsc(
                 valueAt<StringView>(row, offset), decoded, index) == 0;
    }

    return decoded.valueAt<T>(index) == valueAt<T>(row, offset);
  }

  template <TypeKind Kind>
  inline int compare(
      const char* FOLLY_NONNULL row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags) {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    bool rowIsNull = isNullAt(row, column.nullByte(), column.nullMask());
    bool indexIsNull = decoded.isNullAt(index);
    if (rowIsNull) {
      return indexIsNull ? 0 : flags.nullsFirst ? -1 : 1;
    }
    if (indexIsNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    if (Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(row, column.offset(), decoded, index);
    }
    if (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      auto result = compareStringAsc(
          valueAt<StringView>(row, column.offset()), decoded, index);
      return flags.ascending ? result : result * -1;
    }
    auto left = valueAt<T>(row, column.offset());
    auto right = decoded.valueAt<T>(index);
    auto result = comparePrimitiveAsc(left, right);
    return flags.ascending ? result : result * -1;
  }

  template <TypeKind Kind>
  inline int compare(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      const Type* FOLLY_NONNULL type,
      RowColumn column,
      CompareFlags flags) {
    using T = typename KindToFlatVector<Kind>::HashRowType;
    auto nullByte = column.nullByte();
    auto nullMask = column.nullMask();
    bool leftIsNull = isNullAt(left, nullByte, nullMask);
    bool rightIsNull = isNullAt(right, nullByte, nullMask);
    if (leftIsNull) {
      return rightIsNull ? 0 : flags.nullsFirst ? -1 : 1;
    }
    if (rightIsNull) {
      return flags.nullsFirst ? 1 : -1;
    }
    auto offset = column.offset();
    if (Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      return compareComplexType(left, right, type, offset, flags);
    }
    if (Kind == TypeKind::VARCHAR || Kind == TypeKind::VARBINARY) {
      auto leftValue = valueAt<StringView>(left, offset);
      auto rightValue = valueAt<StringView>(right, offset);
      auto result = compareStringAsc(leftValue, rightValue);
      return flags.ascending ? result : result * -1;
    }
    auto leftValue = valueAt<T>(left, offset);
    auto rightValue = valueAt<T>(right, offset);
    auto result = comparePrimitiveAsc(leftValue, rightValue);
    return flags.ascending ? result : result * -1;
  }

  template <typename T>
  static inline int comparePrimitiveAsc(const T& left, const T& right) {
    if constexpr (std::is_floating_point<T>::value) {
      bool isLeftNan = std::isnan(left);
      bool isRightNan = std::isnan(right);
      if (isLeftNan) {
        return isRightNan ? 0 : 1;
      }
      if (isRightNan) {
        return -1;
      }
    }
    return left < right ? -1 : left == right ? 0 : 1;
  }

  void storeComplexType(
      const DecodedVector& decoded,
      vector_size_t index,
      char* FOLLY_NONNULL row,
      int32_t offset,
      int32_t nullByte = 0,
      uint8_t nullMask = 0);

  template <bool useRowNumbers>
  static void extractComplexType(
      const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
      folly::Range<const vector_size_t*> rowNumbers,
      int32_t numRows,
      RowColumn column,
      int32_t resultOffset,
      VectorPtr& result) {
    ByteStream stream;
    auto nullByte = column.nullByte();
    auto nullMask = column.nullMask();
    auto offset = column.offset();

    VELOX_DCHECK_LE(numRows + resultOffset, result->size());
    for (int i = 0; i < numRows; ++i) {
      const char* row;
      if constexpr (useRowNumbers) {
        row = rows[rowNumbers[i]];
      } else {
        row = rows[i];
      }
      auto resultIndex = resultOffset + i;
      if (!row || isNullAt(row, nullByte, nullMask)) {
        result->setNull(resultIndex, true);
      } else {
        prepareRead(row, offset, stream);
        ContainerRowSerde::instance().deserialize(
            stream, resultIndex, result.get());
      }
    }
  }

  static void extractString(
      StringView value,
      FlatVector<StringView>* FOLLY_NONNULL values,
      vector_size_t index);

  static int32_t compareStringAsc(
      StringView left,
      const DecodedVector& decoded,
      vector_size_t index);

  static int32_t compareStringAsc(StringView left, StringView right);

  int32_t compareComplexType(
      const char* FOLLY_NONNULL row,
      int32_t offset,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags = CompareFlags());

  int32_t compareComplexType(
      const char* FOLLY_NONNULL left,
      const char* FOLLY_NONNULL right,
      const Type* FOLLY_NONNULL type,
      int32_t offset,
      CompareFlags flags);

  // Free any variable-width fields associated with the 'rows'.
  void freeVariableWidthFields(folly::Range<char**> rows);

  // Free any aggregates associated with the 'rows'.
  void freeAggregates(folly::Range<char**> rows);

  const std::vector<TypePtr> keyTypes_;
  const bool nullableKeys_;

  // Aggregates in payload. TODO: Separate out aggregate metadata
  // needed to manage memory of accumulators and the executable
  // aggregates. Store the metadata here.
  const std::vector<std::unique_ptr<Aggregate>>& aggregates_;
  bool usesExternalMemory_ = false;
  // Types of non-aggregate columns. Keys first. Corresponds pairwise
  // to 'typeKinds_' and 'rowColumns_'.
  std::vector<TypePtr> types_;
  std::vector<TypeKind> typeKinds_;
  const bool isJoinBuild_;
  int32_t nextOffset_ = 0;
  // Bit position of null bit  in the row. 0 if no null flag. Order is keys,
  // accumulators, dependent.
  std::vector<int32_t> nullOffsets_;
  // Position of field or accumulator. Corresponds 1:1 to 'nullOffset_'.
  std::vector<int32_t> offsets_;
  // Offset and null indicator offset of non-aggregate fields as a single word.
  // Corresponds pairwise to 'types_'.
  std::vector<RowColumn> rowColumns_;
  // Bit offset of the probed flag for a full or right outer join  payload. 0 if
  // not applicable.
  int32_t probedFlagOffset_ = 0;

  // Bit position of free bit.
  int32_t freeFlagOffset_ = 0;
  int32_t rowSizeOffset_ = 0;

  int32_t fixedRowSize_;
  // True if normalized keys are enabled in initial state.
  const bool hasNormalizedKeys_;
  // The count of entries that have an extra normalized_key_t before the
  // start.
  int64_t numRowsWithNormalizedKey_ = 0;
  // Extra bytes to reserve before  each added row for a normalized key. Set to
  // 0 after deciding not to use normalized keys.
  int8_t normalizedKeySize_ = sizeof(normalized_key_t);
  // Copied over the null bits of each row on initialization. Keys are
  // not null, aggregates are null.
  std::vector<uint8_t> initialNulls_;
  uint64_t numRows_ = 0;
  // Head of linked list of free rows.
  char* FOLLY_NULLABLE firstFreeRow_ = nullptr;
  uint64_t numFreeRows_ = 0;

  AllocationPool rows_;
  HashStringAllocator stringAllocator_;

  // Partition number for each row. Used only in parallel hash join build.
  std::unique_ptr<RowPartitions> partitions_;

  const RowSerde& serde_;
  // RowContainer requires a valid reference to a vector of aggregates. We use
  // a static constant to ensure the aggregates_ is valid throughout the
  // lifetime of the RowContainer.
  static const std::vector<std::unique_ptr<Aggregate>>& emptyAggregates() {
    static const std::vector<std::unique_ptr<Aggregate>> kEmptyAggregates;
    return kEmptyAggregates;
  }
};

template <>
inline void RowContainer::storeWithNulls<TypeKind::ROW>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ROW>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* FOLLY_NONNULL row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::extractColumnTyped<TypeKind::OPAQUE>(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL /*rows*/,
    folly::Range<const vector_size_t*> /*rowNumbers*/,
    int32_t /*numRows*/,
    RowColumn /*column*/,
    int32_t /*resultOffset*/,
    VectorPtr& /*result*/) {
  VELOX_UNSUPPORTED("RowContainer doesn't support values of type OPAQUE");
}

inline void RowContainer::extractColumn(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
    int32_t numRows,
    RowColumn column,
    int32_t resultOffset,
    VectorPtr& result) {
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      result->typeKind(),
      rows,
      {},
      numRows,
      column,
      resultOffset,
      result);
}

inline void RowContainer::extractColumn(
    const char* FOLLY_NONNULL const* FOLLY_NONNULL rows,
    folly::Range<const vector_size_t*> rowNumbers,
    RowColumn column,
    int32_t resultOffset,
    VectorPtr& result) {
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      result->typeKind(),
      rows,
      rowNumbers,
      rowNumbers.size(),
      column,
      resultOffset,
      result);
}

template <bool mayHaveNulls>
inline bool RowContainer::equals(
    const char* FOLLY_NONNULL row,
    RowColumn column,
    const DecodedVector& decoded,
    vector_size_t index) {
  if (!mayHaveNulls) {
    return VELOX_DYNAMIC_TYPE_DISPATCH(
        equalsNoNulls,
        decoded.base()->typeKind(),
        row,
        column.offset(),
        decoded,
        index);
  } else {
    return VELOX_DYNAMIC_TYPE_DISPATCH(
        equalsWithNulls,
        decoded.base()->typeKind(),
        row,
        column.offset(),
        column.nullByte(),
        column.nullMask(),
        decoded,
        index);
  }
}

inline int RowContainer::compare(
    const char* FOLLY_NONNULL row,
    RowColumn column,
    const DecodedVector& decoded,
    vector_size_t index,
    CompareFlags flags) {
  return VELOX_DYNAMIC_TYPE_DISPATCH(
      compare, decoded.base()->typeKind(), row, column, decoded, index, flags);
}

inline int RowContainer::compare(
    const char* FOLLY_NONNULL left,
    const char* FOLLY_NONNULL right,
    int columnIndex,
    CompareFlags flags) {
  auto type = types_[columnIndex].get();
  return VELOX_DYNAMIC_TYPE_DISPATCH(
      compare, type->kind(), left, right, type, columnAt(columnIndex), flags);
}

} // namespace facebook::velox::exec
