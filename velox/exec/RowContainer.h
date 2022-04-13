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
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"
namespace facebook::velox::exec {

struct RowContainerIterator {
  int32_t allocationIndex = 0;
  int32_t runIndex = 0;
  int32_t rowOffset = 0;
  // Number of unvisited entries that are prefixed by a uint64_t for
  // normalized key. Set in listRows() on first call.
  int64_t normalizedKeysLeft = 0;

  void reset() {
    allocationIndex = 0;
    runIndex = 0;
    rowOffset = 0;
    normalizedKeysLeft = 0;
  }
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

using normalized_key_t = uint64_t;

// Collection of rows for aggregation, hash join, order by
class RowContainer {
 public:
  static constexpr uint64_t kUnlimited = std::numeric_limits<uint64_t>::max();
  using Eraser = std::function<void(folly::Range<char**> rows)>;

  // 'keyTypes' gives the type of row and use 'mappedMemory' for bulk
  // allocation.
  RowContainer(
      const std::vector<TypePtr>& keyTypes,
      memory::MappedMemory* mappedMemory)
      : RowContainer(
            keyTypes,
            true, // nullableKeys
            emptyAggregates(),
            std::vector<TypePtr>(),
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
      memory::MappedMemory* mappedMemory,
      const RowSerde& serde);

  // Allocates a new row and initializes possible aggregates to null.
  char* newRow();

  uint32_t rowSize(const char* row) const {
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

  void incrementRowSize(char* row, uint64_t bytes) {
    uint32_t* ptr = reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
    uint64_t size = *ptr + bytes;
    *ptr = std::min<uint64_t>(size, std::numeric_limits<uint32_t>::max());
  }

  // Initialize row. 'reuse' specifies whether the 'row' is reused or
  // not. If it is reused, it will free memory associated with the row
  // elsewhere (such as in HashStringAllocator).
  char* initializeRow(char* row, bool reuse);

  // Stores the 'index'th value in 'decoded' into 'row' at
  // 'columnIndex'.
  void store(
      const DecodedVector& decoded,
      vector_size_t index,
      char* row,
      int32_t columnIndex);

  HashStringAllocator& stringAllocator() {
    return stringAllocator_;
  }

  // Returns the number of used rows in 'this'. This is the number of
  // rows a RowContainerIterator would access.
  int64_t numRows() const {
    return numRows_;
  }

  // Copies the values at 'col' into 'result' for the
  // 'numRows' rows pointed to by 'rows'. If an entry in 'rows' is null, sets
  // corresponding row in 'result' to null.
  static void extractColumn(
      const char* const* rows,
      int32_t numRows,
      RowColumn col,
      VectorPtr result);

  // Copies the values at 'columnIndex' into 'result' for the
  // 'numRows' rows pointed to by 'rows'. If an entry in 'rows' is null, sets
  //  corresponding row in 'result' to null.
  void extractColumn(
      const char* const* rows,
      int32_t numRows,
      int32_t columnIndex,
      VectorPtr result) {
    extractColumn(rows, numRows, columnAt(columnIndex), result);
  }

  static inline int32_t nullByte(int32_t nullOffset) {
    return nullOffset / 8;
  }

  static inline uint8_t nullMask(int32_t nullOffset) {
    return 1 << (nullOffset & 7);
  }

  // Extracts up to 'maxRows' rows starting at the position of
  // 'iter'. A default constructed or reset iter starts at the
  // beginning. Returns the number of rows written to 'rows'. Returns
  // 0 when at end. Stops after the total size of returned rows exceeds
  // maxBytes.
  int32_t listRows(
      RowContainerIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) {
    return listRows<false>(iter, maxRows, maxBytes, rows);
  }

  int32_t listRows(RowContainerIterator* iter, int32_t maxRows, char** rows) {
    return listRows<false>(iter, maxRows, kUnlimited, rows);
  }

  /// Sets 'probed' flag for the specified rows. Used by the right and full join
  /// to mark build-side rows that matches join condition. 'rows' may contain
  /// duplicate entries for the cases where single probe row matched multiple
  /// build rows. In case of the full join, 'rows' may include null entries that
  /// correspond to probe rows with no match.
  void setProbedFlag(char** rows, int32_t numRows);

  /// Returns rows with 'probed' flag unset. Used by the right and full join.
  int32_t listNotProbedRows(
      RowContainerIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) {
    return listRows<true>(iter, maxRows, maxBytes, rows);
  }

  // Returns true if 'row' at 'column' equals the value at 'index' in
  // 'decoded'. 'mayHaveNulls' specifies if nulls need to be checked. This is
  // a fast path for compare().
  template <bool mayHaveNulls>
  bool equals(
      const char* row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index);

  // Compares the value at 'column' in 'row' with the value at 'index'
  // in 'decoded'. Returns 0 for equal, < 0 for 'row' < 'decoded', > 0
  // otherwise.
  int32_t compare(
      const char* row,
      RowColumn column,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags = CompareFlags());

  // Compares the value at 'columnIndex' between 'left' and 'right'. Returns 0
  // for equal, < 0 for left < right, > 0 otherwise.
  int32_t compare(
      const char* left,
      const char* right,
      int32_t columnIndex,
      CompareFlags flags = CompareFlags());

  // Allows get/set of the normalized key. If normalized keys are
  // used, they are stored in the word immediately below the hash
  // table row.
  static inline normalized_key_t& normalizedKey(char* group) {
    return reinterpret_cast<normalized_key_t*>(group)[-1];
  }

  void disableNormalizedKeys() {
    normalizedKeySize_ = 0;
  }

  RowColumn columnAt(int32_t index) const {
    return rowColumns_[index];
  }

  // Bit offset of the probed flag for a full or right outer join  payload. 0 if
  // not applicable.
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
      uint64_t* result);

  uint64_t allocatedBytes() const {
    return rows_.allocatedBytes() + stringAllocator_.retainedSize();
  }

  // Resets the state to be as after construction. Frees memory for payload.
  void clear();

  int32_t compareRows(const char* left, const char* right) {
    for (auto i = 0; i < keyTypes_.size(); ++i) {
      auto result = compare(left, right, i);
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
  void extractRows(const std::vector<char*>& rows, const RowVectorPtr& result) {
    VELOX_CHECK_EQ(rows.size(), result->size());
    for (int i = 0; i < result->childrenSize(); ++i) {
      RowContainer::extractColumn(
          rows.data(), rows.size(), columnAt(i), result->childAt(i));
    }
  }

  memory::MappedMemory* mappedMemory() const {
    return stringAllocator_.mappedMemory();
  }

  // Checks that row and free row counts match and that free list
  // membership is consistent with free flag.
  void checkConsistency();

 private:
  // Offset of the pointer to the next free row on a free row.
  static constexpr int32_t kNextFreeOffset = 0;

  static inline bool
  isNullAt(const char* row, int32_t nullByte, uint8_t nullMask) {
    return (row[nullByte] & nullMask) != 0;
  }

  template <typename T>
  static inline T valueAt(const char* group, int32_t offset) {
    return *reinterpret_cast<const T*>(group + offset);
  }

  template <typename T>
  static inline T& valueAt(char* group, int32_t offset) {
    return *reinterpret_cast<T*>(group + offset);
  }

  template <TypeKind Kind>
  static void extractColumnTyped(
      const char* const* rows,
      int32_t numRows,
      RowColumn column,
      VectorPtr result) {
    if (Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
        Kind == TypeKind::MAP) {
      extractComplexType(rows, numRows, column, result);
      return;
    }
    using T = typename KindToFlatVector<Kind>::HashRowType;
    auto* flatResult = result->as<FlatVector<T>>();
    auto nullMask = column.nullMask();
    auto offset = column.offset();
    if (!nullMask) {
      extractValuesNoNulls<T>(rows, numRows, offset, flatResult);
    } else {
      extractValuesWithNulls<T>(
          rows, numRows, offset, column.nullByte(), nullMask, flatResult);
    }
  }

  char*& nextFree(char* row) {
    return *reinterpret_cast<char**>(row + kNextFreeOffset);
  }

  uint32_t& variableRowSize(char* row) {
    DCHECK(rowSizeOffset_);
    return *reinterpret_cast<uint32_t*>(row + rowSizeOffset_);
  }

  template <TypeKind Kind>
  inline void storeWithNulls(
      const DecodedVector& decoded,
      vector_size_t index,
      char* row,
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
    if constexpr (std::is_same<T, StringView>::value) {
      RowSizeTracker tracker(row[rowSizeOffset_], stringAllocator_);
      stringAllocator_.copyMultipart(row, offset);
    }
  }

  template <TypeKind Kind>
  inline void storeNoNulls(
      const DecodedVector& decoded,
      vector_size_t index,
      char* group,
      int32_t offset) {
    using T = typename TypeTraits<Kind>::NativeType;
    *reinterpret_cast<T*>(group + offset) = decoded.valueAt<T>(index);
    if constexpr (std::is_same<T, StringView>::value) {
      RowSizeTracker tracker(group[rowSizeOffset_], stringAllocator_);
      stringAllocator_.copyMultipart(group, offset);
    }
  }

  template <typename T>
  static void extractValuesWithNulls(
      const char* const* rows,
      int32_t numRows,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask,
      FlatVector<T>* result) {
    result->resize(numRows);
    BufferPtr nullBuffer = result->mutableNulls(numRows);
    auto nulls = nullBuffer->asMutable<uint64_t>();
    BufferPtr valuesBuffer = result->mutableValues(numRows);
    auto values = valuesBuffer->asMutableRange<T>();
    for (int32_t i = 0; i < numRows; ++i) {
      if (rows[i] == nullptr) {
        bits::setNull(nulls, i, true);
      } else {
        bits::setNull(nulls, i, isNullAt(rows[i], nullByte, nullMask));
        values[i] = valueAt<T>(rows[i], offset);
      }
    }
  }

  template <typename T>
  static void extractValuesNoNulls(
      const char* const* rows,
      int32_t numRows,
      int32_t offset,
      FlatVector<T>* result) {
    result->resize(numRows);
    BufferPtr valuesBuffer = result->mutableValues(numRows);
    auto values = valuesBuffer->asMutableRange<T>();
    for (int32_t i = 0; i < numRows; ++i) {
      if (rows[i] == nullptr) {
        result->setNull(i, true);
      } else {
        result->setNull(i, false);
        // Here a StringView will reference the hash table, not copy.
        values[i] = valueAt<T>(rows[i], offset);
      }
    }
  }

  static void prepareRead(const char* row, int32_t offset, ByteStream& stream);

  template <TypeKind Kind>
  void hashTyped(
      const Type* type,
      RowColumn column,
      bool nullable,
      folly::Range<char**> rows,
      bool mix,
      uint64_t* result);

  template <TypeKind Kind>
  inline bool equalsWithNulls(
      const char* row,
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
      const char* row,
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
      const char* row,
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
      const char* left,
      const char* right,
      const Type* type,
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
      char* row,
      int32_t offset,
      int32_t nullByte = 0,
      uint8_t nullMask = 0);

  template <bool nonProbedRowsOnly>
  int32_t listRows(
      RowContainerIterator* iter,
      int32_t maxRows,
      uint64_t maxBytes,
      char** rows) {
    int32_t count = 0;
    uint64_t totalBytes = 0;
    auto numAllocations = rows_.numAllocations();
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
          if constexpr (nonProbedRowsOnly) {
            if (bits::isBitSet(rows[count - 1], probedFlagOffset_)) {
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

  static void extractComplexType(
      const char* const* rows,
      int32_t numRows,
      RowColumn column,
      VectorPtr result);

  static void extractString(
      StringView value,
      FlatVector<StringView>* values,
      vector_size_t index);

  static int32_t compareStringAsc(
      StringView left,
      const DecodedVector& decoded,
      vector_size_t index);

  static int32_t compareStringAsc(StringView left, StringView right);

  int32_t compareComplexType(
      const char* row,
      int32_t offset,
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags flags = CompareFlags());

  int32_t compareComplexType(
      const char* left,
      const char* right,
      const Type* type,
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
  // Bit position of probed flag, 0 if none.
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
  char* firstFreeRow_ = nullptr;
  uint64_t numFreeRows_ = 0;

  AllocationPool rows_;
  HashStringAllocator stringAllocator_;
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
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ROW>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::ARRAY>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::storeWithNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  storeComplexType(decoded, index, row, offset, nullByte, nullMask);
}

template <>
inline void RowContainer::storeNoNulls<TypeKind::MAP>(
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    int32_t offset) {
  storeComplexType(decoded, index, row, offset);
}

template <>
inline void RowContainer::extractValuesNoNulls<StringView>(
    const char* const* rows,
    int32_t numRows,
    int32_t offset,
    FlatVector<StringView>* result) {
  result->resize(numRows);
  for (int32_t i = 0; i < numRows; ++i) {
    if (rows[i] == nullptr) {
      result->setNull(i, true);
    } else {
      result->setNull(i, false);
      extractString(valueAt<StringView>(rows[i], offset), result, i);
    }
  }
}

template <>
inline void RowContainer::extractValuesWithNulls<StringView>(
    const char* const* rows,
    int32_t numRows,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    FlatVector<StringView>* result) {
  result->resize(numRows);
  for (int32_t i = 0; i < numRows; ++i) {
    if (!rows[i] || isNullAt(rows[i], nullByte, nullMask)) {
      result->setNull(i, true);
    } else {
      extractString(valueAt<StringView>(rows[i], offset), result, i);
    }
  }
}

template <>
inline void RowContainer::extractColumnTyped<TypeKind::OPAQUE>(
    const char* const* /*rows*/,
    int32_t /*numRows*/,
    RowColumn /*column*/,
    VectorPtr /*result*/) {
  VELOX_UNSUPPORTED("RowContainer doesn't support values of type OPAQUE");
}

inline void RowContainer::extractColumn(
    const char* const* rows,
    int32_t numRows,
    RowColumn column,
    VectorPtr result) {
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped, result->typeKind(), rows, numRows, column, result);
}

template <bool mayHaveNulls>
inline bool RowContainer::equals(
    const char* row,
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
    const char* row,
    RowColumn column,
    const DecodedVector& decoded,
    vector_size_t index,
    CompareFlags flags) {
  return VELOX_DYNAMIC_TYPE_DISPATCH(
      compare, decoded.base()->typeKind(), row, column, decoded, index, flags);
}

inline int RowContainer::compare(
    const char* left,
    const char* right,
    int columnIndex,
    CompareFlags flags) {
  auto type = types_[columnIndex].get();
  return VELOX_DYNAMIC_TYPE_DISPATCH(
      compare, type->kind(), left, right, type, columnAt(columnIndex), flags);
}

} // namespace facebook::velox::exec
