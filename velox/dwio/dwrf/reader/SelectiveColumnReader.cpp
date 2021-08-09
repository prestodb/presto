/*
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

#include "velox/dwio/dwrf/reader/SelectiveColumnReader.h"

#include "velox/aggregates/AggregationHook.h"
#include "velox/common/base/Portability.h"
#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/dwrf/common/DirectDecoder.h"
#include "velox/dwio/dwrf/common/FloatingPointDecoder.h"
#include "velox/dwio/dwrf/common/RLEv1.h"
#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/DictionaryVector.h"
#include "velox/vector/FlatVector.h"

#include <numeric>

namespace facebook::velox::dwrf {

using common::FilterKind;
using dwio::common::TypeWithId;
using dwio::common::typeutils::CompatChecker;
using namespace facebook::velox::aggregate;
using V64 = simd::Vectors<int64_t>;
using V32 = simd::Vectors<int32_t>;
using V16 = simd::Vectors<int16_t>;
using V8 = simd::Vectors<int8_t>;

namespace {

class Timer {
 public:
  Timer() : startClocks_{folly::hardware_timestamp()} {}

  uint64_t elapsedClocks() const {
    return folly::hardware_timestamp() - startClocks_;
  }

 private:
  const uint64_t startClocks_;
};

// struct for grouping together global constants.
struct Filters {
  static common::AlwaysTrue alwaysTrue;
};

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    default:
      DWIO_RAISE("Unknown encoding in convertRleVersion");
  }
}
} // namespace

SelectiveColumnReader::SelectiveColumnReader(
    const EncodingKey& ek,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec,
    const TypePtr& type,
    bool loadIndex)
    : ColumnReader(ek, stripe),
      scanSpec_(scanSpec),
      type_{type},
      rowsPerRowGroup_{stripe.rowsPerRowGroup()} {
  if (scanSpec->filter() || loadIndex) {
    indexStream_ =
        stripe.getStream(ek.forKind(proto::Stream_Kind_ROW_INDEX), false);
  }
}

std::vector<uint32_t> SelectiveColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const StatsContext& context) const {
  ensureRowGroupIndex();
  auto filter = scanSpec_->filter();
  if (!index_ || !filter) {
    return ColumnReader::filterRowGroups(rowGroupSize, context);
  }

  std::vector<uint32_t> stridesToSkip;
  for (auto i = 0; i < index_->entry_size(); i++) {
    const auto& entry = index_->entry(i);
    auto columnStats = ColumnStatistics::fromProto(entry.statistics(), context);
    if (!testFilter(filter, columnStats.get(), rowGroupSize, type_)) {
      stridesToSkip.push_back(i); // Skipping stride based on column stats.
    }
  }
  return stridesToSkip;
}

void SelectiveColumnReader::seekTo(vector_size_t offset, bool readsNullsOnly) {
  if (offset == readOffset_) {
    return;
  }
  if (readOffset_ < offset) {
    if (readsNullsOnly) {
      ColumnReader::skip(offset - readOffset_);
    } else {
      skip(offset - readOffset_);
    }
    readOffset_ = offset;
  } else {
    VELOX_CHECK(false, "Seeking backward on a ColumnReader");
  }
}

template <typename T>
void SelectiveColumnReader::ensureValuesCapacity(vector_size_t numRows) {
  if (values_ &&
      values_->capacity() >=
          BaseVector::byteSize<T>(numRows) + simd::kPadding) {
    return;
  }
  values_ = AlignedBuffer::allocate<T>(
      numRows + (simd::kPadding / sizeof(T)), &memoryPool);
  rawValues_ = values_->asMutable<char>();
}

void SelectiveColumnReader::prepareNulls(RowSet rows, bool hasNulls) {
  if (!hasNulls) {
    anyNulls_ = false;
    return;
  }
  auto numRows = rows.size();
  if (useBulkPath()) {
    bool isDense = rows.back() == rows.size() - 1;
    if (!scanSpec_->filter()) {
      anyNulls_ = nullsInReadRange_ != nullptr;
      returnReaderNulls_ = anyNulls_ && isDense;
      // No need for null flags if fast path
      if (returnReaderNulls_) {
        return;
      }
    }
  }
  if (resultNulls_ && resultNulls_->unique() &&
      resultNulls_->capacity() >= bits::nbytes(numRows) + simd::kPadding) {
    // Clear whole capacity because future uses could hit
    // uncleared data between capacity() and 'numBytes'.
    simd::memset(rawResultNulls_, bits::kNotNullByte, resultNulls_->capacity());
    anyNulls_ = false;
    return;
  }

  anyNulls_ = false;
  resultNulls_ = AlignedBuffer::allocate<bool>(
      numRows + (simd::kPadding * 8), &memoryPool);
  rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
  simd::memset(rawResultNulls_, bits::kNotNullByte, resultNulls_->capacity());
}

template <typename T>
void SelectiveColumnReader::prepareRead(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  seekTo(offset, scanSpec_->readsNullsOnly());
  vector_size_t numRows = rows.back() + 1;
  readNulls(numRows, incomingNulls, nullptr, nullsInReadRange_);
  // We check for all nulls and no nulls. We expect both calls to
  // bits::isAllSet to fail early in the common case. We could do a
  // single traversal of null bits counting the bits and then compare
  // this to 0 and the total number of rows but this would end up
  // reading more in the mixed case and would not be better in the all
  // (non)-null case.
  allNull_ = nullsInReadRange_ &&
      bits::isAllSet(
                 nullsInReadRange_->as<uint64_t>(), 0, numRows, bits::kNull);
  if (nullsInReadRange_ &&
      bits::isAllSet(
          nullsInReadRange_->as<uint64_t>(), 0, numRows, bits::kNotNull)) {
    nullsInReadRange_ = nullptr;
  }
  innerNonNullRows_.clear();
  outerNonNullRows_.clear();
  outputRows_.clear();
  // is part of read() and after read returns getValues may be called.
  mayGetValues_ = true;
  numOutConfirmed_ = 0;
  numValues_ = 0;
  valueSize_ = sizeof(T);
  inputRows_ = rows;
  if (scanSpec_->filter()) {
    outputRows_.reserve(rows.size());
  }
  ensureValuesCapacity<T>(rows.size());
  if (scanSpec_->keepValues() && !scanSpec_->valueHook()) {
    // Can't re-use if someone else has a reference
    if (values_ && !values_->unique()) {
      values_.reset();
    }
    ensureValuesCapacity<T>(rows.size());
    valueRows_.clear();
    prepareNulls(rows, nullsInReadRange_ != nullptr);
  }
}

template <typename T, typename TVector>
void SelectiveColumnReader::getFlatValues(
    RowSet rows,
    VectorPtr* result,
    const TypePtr& type,
    bool isFinal) {
  VELOX_CHECK(valueSize_ != kNoValueSize);
  VELOX_CHECK(mayGetValues_);
  if (isFinal) {
    mayGetValues_ = false;
  }
  if (allNull_) {
    *result = std::make_shared<ConstantVector<TVector>>(
        &memoryPool,
        rows.size(),
        true,
        type,
        T(),
        cdvi::EMPTY_METADATA,
        sizeof(TVector) * rows.size());
    return;
  }
  if (valueSize_ == sizeof(TVector)) {
    compactScalarValues<TVector, TVector>(rows, isFinal);
  } else if (sizeof(T) >= sizeof(TVector)) {
    compactScalarValues<T, TVector>(rows, isFinal);
  } else {
    upcastScalarValues<T, TVector>(rows);
  }
  valueSize_ = sizeof(TVector);
  BufferPtr nulls = anyNulls_
      ? (returnReaderNulls_ ? nullsInReadRange_ : resultNulls_)
      : nullptr;
  *result = std::make_shared<FlatVector<TVector>>(
      &memoryPool, type, nulls, numValues_, values_, std::move(stringBuffers_));
}

template <>
void SelectiveColumnReader::getFlatValues<int8_t, bool>(
    RowSet rows,
    VectorPtr* result,
    const TypePtr& type,
    bool isFinal) {
  constexpr int32_t kWidth = V8::VSize;
  static_assert(kWidth == 32);
  VELOX_CHECK(valueSize_ == sizeof(int8_t));
  compactScalarValues<int8_t, int8_t>(rows, isFinal);
  auto boolValues =
      AlignedBuffer::allocate<bool>(numValues_, &memoryPool, false);
  auto rawBits = boolValues->asMutable<uint32_t>();
  auto rawBytes = values_->as<int8_t>();
  auto zero = V8::setAll(0);
  for (auto i = 0; i < numValues_; i += kWidth) {
    rawBits[i / kWidth] = ~V8::compareBitMask(
        V8::compareResult(V8::compareEq(zero, V8::load(rawBytes + i))));
  }
  BufferPtr nulls = anyNulls_
      ? (returnReaderNulls_ ? nullsInReadRange_ : resultNulls_)
      : nullptr;
  *result = std::make_shared<FlatVector<bool>>(
      &memoryPool,
      type,
      nulls,
      numValues_,
      std::move(boolValues),
      std::move(stringBuffers_));
}

template <typename T, typename TVector>
void SelectiveColumnReader::upcastScalarValues(RowSet rows) {
  VELOX_CHECK(rows.size() <= numValues_);
  VELOX_CHECK(!rows.empty());
  if (!values_) {
    return;
  }
  VELOX_CHECK(sizeof(TVector) > sizeof(T));
  // Since upcast is not going to be a common path, allocate buffer to copy
  // upcasted values to and then copy back to the values buffer.
  std::vector<TVector> buf;
  buf.resize(rows.size());
  T* typedSourceValues = reinterpret_cast<T*>(rawValues_);
  RowSet sourceRows;
  // The row numbers corresponding to elements in 'values_' are in
  // 'valueRows_' if values have been accessed before. Otherwise
  // they are in 'outputRows_' if these are non-empty (there is a
  // filter) and in 'inputRows_' otherwise.
  if (!valueRows_.empty()) {
    sourceRows = valueRows_;
  } else if (!outputRows_.empty()) {
    sourceRows = outputRows_;
  } else {
    sourceRows = inputRows_;
  }
  if (valueRows_.empty()) {
    valueRows_.resize(rows.size());
  }
  vector_size_t rowIndex = 0;
  auto nextRow = rows[rowIndex];
  for (size_t i = 0; i < numValues_; i++) {
    if (sourceRows[i] < nextRow) {
      continue;
    }

    VELOX_DCHECK(sourceRows[i] == nextRow);
    buf[rowIndex] = typedSourceValues[i];
    if (resultNulls_) {
      bits::setBit(
          rawResultNulls_, rowIndex, bits::isBitSet(rawResultNulls_, i));
    }
    valueRows_[rowIndex] = nextRow;
    rowIndex++;
    if (rowIndex >= rows.size()) {
      break;
    }
    nextRow = rows[rowIndex];
  }
  ensureValuesCapacity<TVector>(rows.size());
  std::memcpy(rawValues_, buf.data(), rows.size() * sizeof(TVector));
  numValues_ = rows.size();
  valueRows_.resize(numValues_);
  values_->setSize(numValues_ * sizeof(TVector));
}

template <typename T, typename TVector>
void SelectiveColumnReader::compactScalarValues(RowSet rows, bool isFinal) {
  VELOX_CHECK(rows.size() <= numValues_);
  VELOX_CHECK(!rows.empty());
  if (!values_ || (rows.size() == numValues_ && sizeof(T) == sizeof(TVector))) {
    if (values_) {
      values_->setSize(numValues_ * sizeof(T));
    }
    return;
  }
  VELOX_CHECK(sizeof(TVector) <= sizeof(T));
  T* typedSourceValues = reinterpret_cast<T*>(rawValues_);
  TVector* typedDestValues = reinterpret_cast<TVector*>(rawValues_);
  RowSet sourceRows;
  // The row numbers corresponding to elements in 'values_' are in
  // 'valueRows_' if values have been accessed before. Otherwise
  // they are in 'outputRows_' if these are non-empty (there is a
  // filter) and in 'inputRows_' otherwise.
  if (!valueRows_.empty()) {
    sourceRows = valueRows_;
  } else if (!outputRows_.empty()) {
    sourceRows = outputRows_;
  } else {
    sourceRows = inputRows_;
  }
  if (valueRows_.empty()) {
    valueRows_.resize(rows.size());
  }
  vector_size_t rowIndex = 0;
  auto nextRow = rows[rowIndex];
  for (size_t i = 0; i < numValues_; i++) {
    if (sourceRows[i] < nextRow) {
      continue;
    }

    VELOX_DCHECK(sourceRows[i] == nextRow);
    typedDestValues[rowIndex] = typedSourceValues[i];
    if (resultNulls_) {
      bits::setBit(
          rawResultNulls_, rowIndex, bits::isBitSet(rawResultNulls_, i));
    }
    if (!isFinal) {
      valueRows_[rowIndex] = nextRow;
    }
    rowIndex++;
    if (rowIndex >= rows.size()) {
      break;
    }
    nextRow = rows[rowIndex];
  }
  numValues_ = rows.size();
  valueRows_.resize(numValues_);
  values_->setSize(numValues_ * sizeof(TVector));
}

template <>
void SelectiveColumnReader::compactScalarValues<bool, bool>(
    RowSet rows,
    bool isFinal) {
  if (!values_ || rows.size() == numValues_) {
    if (values_) {
      values_->setSize(bits::nbytes(numValues_));
    }
    return;
  }
  auto rawBits = reinterpret_cast<uint64_t*>(rawValues_);
  vector_size_t rowIndex = 0;
  auto nextRow = rows[rowIndex];
  for (size_t i = 0; i < numValues_; i++) {
    if (outputRows_[i] < nextRow) {
      continue;
    }

    VELOX_DCHECK(outputRows_[i] == nextRow);

    bits::setBit(rawBits, rowIndex, bits::isBitSet(rawBits, i));
    if (resultNulls_) {
      bits::setBit(
          rawResultNulls_, rowIndex, bits::isBitSet(rawResultNulls_, i));
    }
    if (!isFinal) {
      outputRows_[rowIndex] = nextRow;
    }
    rowIndex++;
    if (rowIndex >= rows.size()) {
      break;
    }
    nextRow = rows[rowIndex];
  }
  numValues_ = rows.size();
  outputRows_.resize(numValues_);
  values_->setSize(bits::nbytes(numValues_));
}
namespace {
int32_t sizeOfIntKind(TypeKind kind) {
  switch (kind) {
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
      return 4;
    case TypeKind::BIGINT:
      return 8;
    default:
      VELOX_FAIL("Not an integer TypeKind");
  }
}
} // namespace

void SelectiveColumnReader::getIntValues(
    RowSet rows,
    const Type* requestedType,
    VectorPtr* result) {
  switch (requestedType->kind()) {
    case TypeKind::SMALLINT: {
      switch (valueSize_) {
        case 8:
          getFlatValues<int64_t, int16_t>(rows, result);
          break;
        case 4:
          getFlatValues<int32_t, int16_t>(rows, result);
          break;
        case 2:
          getFlatValues<int16_t, int16_t>(rows, result);
          break;
        default:
          VELOX_FAIL("Unsupported value size");
      }
      break;
      case TypeKind::INTEGER:
        switch (valueSize_) {
          case 8:
            getFlatValues<int64_t, int32_t>(rows, result);
            break;
          case 4:
            getFlatValues<int32_t, int32_t>(rows, result);
            break;
          case 2:
            getFlatValues<int16_t, int32_t>(rows, result);
            break;
          default:
            VELOX_FAIL("Unsupported value size");
        }
        break;
      case TypeKind::BIGINT:
        switch (valueSize_) {
          case 8:
            getFlatValues<int64_t, int64_t>(rows, result);
            break;
          case 4:
            getFlatValues<int32_t, int64_t>(rows, result);
            break;
          case 2:
            getFlatValues<int16_t, int64_t>(rows, result);
            break;
          default:
            VELOX_FAIL("Unsupported value size");
        }
        break;
      default:
        VELOX_FAIL(
            "Not a valid type for integer reader: {}",
            requestedType->toString());
    }
  }
}

template <typename T>
void SelectiveColumnReader::filterNulls(
    RowSet rows,
    bool isNull,
    bool extractValues) {
  if (!notNullDecoder) {
    if (isNull) {
      // The whole stripe will be empty. We do not update
      // 'readOffset' since nothing is read from either nulls or data.
      return;
    }
    // All pass. We do not update 'readOffset' because neither nulls
    // or data will be read for the whole stripe.
    setOutputRows(rows);
    return;
  }
  bool isDense = rows.back() == rows.size() - 1;
  auto rawNulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  if (isNull) {
    if (!rawNulls) {
      // The stripe has nulls but the current range does not. Nothing matches.
    } else if (isDense) {
      bits::forEachUnsetBit(
          rawNulls, 0, rows.back() + 1, [&](vector_size_t row) {
            addOutputRow(row);
            if (extractValues) {
              addNull<T>();
            }
          });
    } else {
      for (auto row : rows) {
        if (bits::isBitNull(rawNulls, row)) {
          addOutputRow(row);
          if (extractValues) {
            addNull<T>();
          }
        }
      }
    }
    readOffset_ += rows.back() + 1;
    return;
  }

  VELOX_CHECK(
      !extractValues,
      "filterNulls for not null only applies to filter-only case");
  if (!rawNulls) {
    // All pass.
    for (auto row : rows) {
      addOutputRow(row);
    }
  } else if (isDense) {
    bits::forEachSetBit(rawNulls, 0, rows.back() + 1, [&](vector_size_t row) {
      addOutputRow(row);
    });
  } else {
    for (auto row : rows) {
      if (!bits::isBitNull(rawNulls, row)) {
        addOutputRow(row);
      }
    }
  }
  readOffset_ += rows.back() + 1;
}

common::AlwaysTrue Filters::alwaysTrue;

char* SelectiveColumnReader::copyStringValue(folly::StringPiece value) {
  uint64_t size = value.size();
  if (stringBuffers_.empty() || rawStringUsed_ + size > rawStringSize_) {
    if (!stringBuffers_.empty()) {
      stringBuffers_.back()->setSize(rawStringUsed_);
    }
    auto bytes = std::max(size, kStringBufferSize);
    BufferPtr buffer = AlignedBuffer::allocate<char>(bytes, &memoryPool);
    stringBuffers_.push_back(buffer);
    rawStringBuffer_ = buffer->asMutable<char>();
    rawStringUsed_ = 0;
    // Adjust the size downward so that the last store can take place
    // at full width.
    rawStringSize_ = buffer->capacity() - simd::kPadding;
  }
  memcpy(rawStringBuffer_ + rawStringUsed_, value.data(), size);
  auto start = rawStringUsed_;
  rawStringUsed_ += size;
  return rawStringBuffer_ + start;
}

void SelectiveColumnReader::addStringValue(folly::StringPiece value) {
  auto copy = copyStringValue(value);
  reinterpret_cast<StringView*>(rawValues_)[numValues_++] =
      StringView(copy, value.size());
}

std::vector<uint64_t> toPositions(const proto::RowIndexEntry& entry) {
  return std::vector<uint64_t>(
      entry.positions().begin(), entry.positions().end());
}

// structs for extractValues in ColumnVisitor.

// Represents values not being retained after filter evaluation. Must
// be in real namespace because referenced by name in templates in
// decoders.
struct DropValues {
  static constexpr bool kSkipNulls = false;
  using HookType = NoHook;

  bool acceptsNulls() const {
    return true;
  }

  template <typename V>
  void addValue(vector_size_t /*rowIndex*/, V /*value*/) {}

  void addNull(vector_size_t /*rowIndex*/) {}

  HookType& hook() {
    static NoHook hook;
    return hook;
  }
};

namespace {

template <typename TReader>
struct ExtractToReader {
  using HookType = NoHook;
  static constexpr bool kSkipNulls = false;
  explicit ExtractToReader(TReader* readerIn) : reader(readerIn) {}

  bool acceptsNulls() const {
    return true;
  }

  void addNull(vector_size_t rowIndex);

  template <typename V>
  void addValue(vector_size_t /*rowIndex*/, V value) {
    reader->addValue(value);
  }

  TReader* reader;

  NoHook& hook() {
    static NoHook noHook;
    return noHook;
  }
};

template <typename THook>
class ExtractToHook {
 public:
  using HookType = THook;
  static constexpr bool kSkipNulls = THook::kSkipNulls;

  explicit ExtractToHook(ValueHook* hook)
      : hook_(*reinterpret_cast<THook*>(hook)) {}

  bool acceptsNulls() {
    return hook_.acceptsNulls();
  }

  void addNull(vector_size_t rowIndex) {
    hook_.addNull(rowIndex);
  }

  template <typename V>
  void addValue(vector_size_t rowIndex, V value) {
    hook_.addValue(rowIndex, &value);
  }

  auto& hook() {
    return hook_;
  }

 private:
  THook hook_;
};

class ExtractToGenericHook {
 public:
  using HookType = ValueHook;
  static constexpr bool kSkipNulls = false;

  explicit ExtractToGenericHook(ValueHook* hook) : hook_(hook) {}

  bool acceptsNulls() const {
    return hook_->acceptsNulls();
  }

  void addNull(vector_size_t rowIndex) {
    hook_->addNull(rowIndex);
  }

  template <typename V>
  void addValue(vector_size_t rowIndex, V value) {
    hook_->addValue(rowIndex, &value);
  }

  ValueHook& hook() {
    return *hook_;
  }

 private:
  ValueHook* hook_;
};

// Template parameter for controlling filtering and action on a set of rows.
template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class ColumnVisitor {
 public:
  using FilterType = TFilter;
  using Extract = ExtractValues;
  using HookType = typename Extract::HookType;
  using DataType = T;
  static constexpr bool dense = isDense;
  static constexpr bool kHasBulkPath = true;
  ColumnVisitor(
      TFilter& filter,
      SelectiveColumnReader* reader,
      const RowSet& rows,
      ExtractValues values)
      : filter_(filter),
        reader_(reader),
        allowNulls_(!TFilter::deterministic || filter.testNull()),
        rows_(&rows[0]),
        numRows_(rows.size()),
        rowIndex_(0),
        values_(values) {}

  bool allowNulls() {
    if (ExtractValues::kSkipNulls && TFilter::deterministic) {
      return false;
    }
    return allowNulls_ && values_.acceptsNulls();
  }

  vector_size_t start() {
    return isDense ? 0 : rowAt(0);
  }

  // Tests for a null value and processes it. If the value is not
  // null, returns 0 and has no effect. If the value is null, advances
  // to the next non-null value in 'rows_'. Returns the number of
  // values (not including nulls) to skip to get to the next non-null.
  // If there is no next non-null in 'rows_', sets 'atEnd'. If 'atEnd'
  // is set and a non-zero skip is returned, the caller must perform
  // the skip before returning.
  FOLLY_ALWAYS_INLINE vector_size_t checkAndSkipNulls(
      const uint64_t* nulls,
      vector_size_t& current,
      bool& atEnd) {
    auto testRow = currentRow();
    // Check that the caller and the visitor are in sync about current row.
    VELOX_DCHECK(current == testRow);
    uint32_t nullIndex = testRow >> 6;
    uint64_t nullWord = nulls[nullIndex];
    if (nullWord == bits::kNotNull64) {
      return 0;
    }
    uint8_t nullBit = testRow & 63;
    if ((nullWord & (1UL << nullBit))) {
      return 0;
    }
    // We have a null. We find the next non-null.
    if (++rowIndex_ >= numRows_) {
      atEnd = true;
      return 0;
    }
    auto rowOfNullWord = testRow - nullBit;
    if (isDense) {
      if (nullBit == 63) {
        nullBit = 0;
        rowOfNullWord += 64;
        nullWord = nulls[++nullIndex];
      } else {
        ++nullBit;
        // set all the bits below the row to null.
        nullWord &= ~velox::bits::lowMask(nullBit);
      }
      for (;;) {
        auto nextNonNull = count_trailing_zeros(nullWord);
        if (rowOfNullWord + nextNonNull >= numRows_) {
          // Nulls all the way to the end.
          atEnd = true;
          return 0;
        }
        if (nextNonNull < 64) {
          VELOX_CHECK(rowIndex_ <= rowOfNullWord + nextNonNull);
          rowIndex_ = rowOfNullWord + nextNonNull;
          current = currentRow();
          return 0;
        }
        rowOfNullWord += 64;
        nullWord = nulls[++nullIndex];
      }
    } else {
      // Sparse row numbers. We find the first non-null and count
      // how many non-nulls on rows not in 'rows_' we skipped.
      int32_t toSkip = 0;
      nullWord &= ~velox::bits::lowMask(nullBit);
      for (;;) {
        testRow = currentRow();
        while (testRow >= rowOfNullWord + 64) {
          toSkip += __builtin_popcountll(nullWord);
          nullWord = nulls[++nullIndex];
          rowOfNullWord += 64;
        }
        // testRow is inside nullWord. See if non-null.
        nullBit = testRow & 63;
        if ((nullWord & (1UL << nullBit))) {
          toSkip +=
              __builtin_popcountll(nullWord & velox::bits::lowMask(nullBit));
          current = testRow;
          return toSkip;
        }
        if (++rowIndex_ >= numRows_) {
          // We end with a null. Add the non-nulls below the final null.
          toSkip += __builtin_popcountll(
              nullWord & velox::bits::lowMask(testRow - rowOfNullWord));
          atEnd = true;
          return toSkip;
        }
      }
    }
  }

  vector_size_t processNull(bool& atEnd) {
    vector_size_t previous = currentRow();
    if (filter_.testNull()) {
      filterPassedForNull();
    } else {
      filterFailed();
    }
    if (++rowIndex_ >= numRows_) {
      atEnd = true;
      return rows_[numRows_ - 1] - previous;
    }
    if (TFilter::deterministic && isDense) {
      return 0;
    }
    return currentRow() - previous - 1;
  }

  // Check if a string value doesn't pass the filter based on length.
  // Return unset optional if length is not sufficient to determine
  // whether the value passes or not. In this case, the caller must
  // call "process" for the actual string.
  FOLLY_ALWAYS_INLINE std::optional<vector_size_t> processLength(
      int32_t length,
      bool& atEnd) {
    if (!TFilter::deterministic) {
      return std::nullopt;
    }

    if (filter_.testLength(length)) {
      return std::nullopt;
    }

    filterFailed();

    if (++rowIndex_ >= numRows_) {
      atEnd = true;
      return 0;
    }
    if (isDense) {
      return 0;
    }
    return currentRow() - rows_[rowIndex_ - 1] - 1;
  }

  FOLLY_ALWAYS_INLINE vector_size_t process(T value, bool& atEnd) {
    if (!TFilter::deterministic) {
      auto previous = currentRow();
      if (common::applyFilter(filter_, value)) {
        filterPassed(value);
      } else {
        filterFailed();
      }
      if (++rowIndex_ >= numRows_) {
        atEnd = true;
        return rows_[numRows_ - 1] - previous;
      }
      return currentRow() - previous - 1;
    }
    // The filter passes or fails and we go to the next row if any.
    if (common::applyFilter(filter_, value)) {
      filterPassed(value);
    } else {
      filterFailed();
    }
    if (++rowIndex_ >= numRows_) {
      atEnd = true;
      return 0;
    }
    if (isDense) {
      return 0;
    }
    return currentRow() - rows_[rowIndex_ - 1] - 1;
  }

  // Returns space for 'size' items of T for a scan to fill. The scan
  // calls addResults and related to mark which elements are part of
  // the result.
  inline T* mutableValues(int32_t size) {
    return reader_->mutableValues<T>(size);
  }

  inline vector_size_t rowAt(vector_size_t index) {
    if (isDense) {
      return index;
    }
    return rows_[index];
  }

  bool atEnd() {
    return rowIndex_ >= numRows_;
  }

  vector_size_t currentRow() {
    if (isDense) {
      return rowIndex_;
    }
    return rows_[rowIndex_];
  }

  const vector_size_t* rows() const {
    return rows_;
  }

  vector_size_t numRows() {
    return numRows_;
  }

  void filterPassed(T value) {
    addResult(value);
    if (!std::is_same<TFilter, common::AlwaysTrue>::value) {
      addOutputRow(currentRow());
    }
  }

  inline void filterPassedForNull() {
    addNull();
    if (!std::is_same<TFilter, common::AlwaysTrue>::value) {
      addOutputRow(currentRow());
    }
  }

  FOLLY_ALWAYS_INLINE void filterFailed();
  inline void addResult(T value);
  inline void addNull();
  inline void addOutputRow(vector_size_t row);

  TFilter& filter() {
    return filter_;
  }

  int32_t* outputRows(int32_t size) {
    return reader_->mutableOutputRows(size);
  }

  void setNumValues(int32_t size) {
    reader_->setNumValues(size);
    if (!std::is_same<TFilter, common::AlwaysTrue>::value) {
      reader_->setNumRows(size);
    }
  }

  HookType& hook() {
    return values_.hook();
  }

  T* rawValues(int32_t size) {
    return reader_->mutableValues<T>(size);
  }

  uint64_t* rawNulls(int32_t size) {
    return reader_->mutableNulls(size);
  }

  void setHasNulls() {
    reader_->setHasNulls();
  }

  void setAllNull(int32_t numValues) {
    reader_->setNumValues(numValues);
    reader_->setAllNull();
  }

  auto& innerNonNullRows() {
    return reader_->innerNonNullRows();
  }

  auto& outerNonNullRows() {
    return reader_->outerNonNullRows();
  }

 protected:
  TFilter& filter_;
  SelectiveColumnReader* reader_;
  const bool allowNulls_;
  const vector_size_t* rows_;
  vector_size_t numRows_;
  vector_size_t rowIndex_;
  ExtractValues values_;
};

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
FOLLY_ALWAYS_INLINE void
ColumnVisitor<T, TFilter, ExtractValues, isDense>::filterFailed() {
  auto preceding = filter_.getPrecedingPositionsToFail();
  auto succeeding = filter_.getSucceedingPositionsToFail();
  if (preceding) {
    reader_->dropResults(preceding);
  }
  if (succeeding) {
    rowIndex_ += succeeding;
  }
}

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
inline void ColumnVisitor<T, TFilter, ExtractValues, isDense>::addResult(
    T value) {
  values_.addValue(rowIndex_, value);
}

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
inline void ColumnVisitor<T, TFilter, ExtractValues, isDense>::addNull() {
  values_.addNull(rowIndex_);
}

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
inline void ColumnVisitor<T, TFilter, ExtractValues, isDense>::addOutputRow(
    vector_size_t row) {
  reader_->addOutputRow(row);
}

template <typename TReader>
void ExtractToReader<TReader>::addNull(vector_size_t /*rowIndex*/) {
  reader->template addNull<typename TReader::ValueType>();
}

class SelectiveByteRleColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = int8_t;

  SelectiveByteRleColumnReader(
      const EncodingKey& ek,
      const std::shared_ptr<const TypeWithId>& requestedType,
      const std::shared_ptr<const TypeWithId>& dataType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec,
      bool isBool)
      : SelectiveColumnReader(ek, stripe, scanSpec, dataType->type, true),
        requestedType_(requestedType->type) {
    if (isBool) {
      boolRle_ = createBooleanRleDecoder(
          stripe.getStream(ek.forKind(proto::Stream_Kind_DATA), true), ek);
    } else {
      byteRle_ = createByteRleDecoder(
          stripe.getStream(ek.forKind(proto::Stream_Kind_DATA), true), ek);
    }
  }

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder) {
      notNullDecoder->seekToRowGroup(positionsProvider);
    }

    if (boolRle_) {
      boolRle_->seekToRowGroup(positionsProvider);
    } else {
      byteRle_->seekToRowGroup(positionsProvider);
    }

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* nulls) override;

  void getValues(RowSet rows, VectorPtr* result) override {
    switch (requestedType_->kind()) {
      case TypeKind::BOOLEAN:
        getFlatValues<int8_t, bool>(rows, result);
        break;
      case TypeKind::TINYINT:
        getFlatValues<int8_t, int8_t>(rows, result);
        break;
      case TypeKind::SMALLINT:
        getFlatValues<int8_t, int16_t>(rows, result);
        break;
      case TypeKind::INTEGER:
        getFlatValues<int8_t, int32_t>(rows, result);
        break;
      case TypeKind::BIGINT:
        getFlatValues<int8_t, int64_t>(rows, result);
        break;
      default:
        VELOX_CHECK(
            false,
            "Result type {} not supported in ByteRLE encoding",
            requestedType_->toString());
    }
  }

  bool useBulkPath() const override {
    return false;
  }

 private:
  template <typename ColumnVisitor>
  void readWithVisitor(RowSet rows, ColumnVisitor visitor);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      ExtractValues extractValues,
      RowSet rows);

  template <bool isDence>
  void processValueHook(RowSet rows, ValueHook* hook);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void
  readHelper(common::Filter* filter, RowSet rows, ExtractValues extractValues);

  std::unique_ptr<ByteRleDecoder> byteRle_;
  std::unique_ptr<BooleanRleDecoder> boolRle_;
  const TypePtr requestedType_;
};

uint64_t SelectiveByteRleColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  if (byteRle_) {
    byteRle_->skip(numValues);
  } else {
    boolRle_->skip(numValues);
  }
  return numValues;
}

template <typename ColumnVisitor>
void SelectiveByteRleColumnReader::readWithVisitor(
    RowSet rows,
    ColumnVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  if (boolRle_) {
    if (nullsInReadRange_) {
      boolRle_->readWithVisitor<true>(
          nullsInReadRange_->as<uint64_t>(), visitor);
    } else {
      boolRle_->readWithVisitor<false>(nullptr, visitor);
    }
  } else {
    if (nullsInReadRange_) {
      byteRle_->readWithVisitor<true>(
          nullsInReadRange_->as<uint64_t>(), visitor);
    } else {
      byteRle_->readWithVisitor<false>(nullptr, visitor);
    }
  }
  readOffset_ += numRows;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveByteRleColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  readWithVisitor(
      rows,
      ColumnVisitor<int8_t, TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <bool isDense, typename ExtractValues>
void SelectiveByteRleColumnReader::processFilter(
    common::Filter* filter,
    ExtractValues extractValues,
    RowSet rows) {
  switch (filter ? filter->kind() : FilterKind::kAlwaysTrue) {
    case FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case FilterKind::kIsNull:
      filterNulls<int8_t>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<int8_t>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case FilterKind::kBigintRange:
      readHelper<common::BigintRange, isDense>(filter, rows, extractValues);
      break;
    case FilterKind::kBigintValuesUsingBitmask:
      readHelper<common::BigintValuesUsingBitmask, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

template <bool isDense>
void SelectiveByteRleColumnReader::processValueHook(
    RowSet rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case AggregationHook::kSumBigintToBigint:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<SumHook<int64_t, int64_t>>(hook));
      break;
    default:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue, rows, ExtractToGenericHook(hook));
  }
}

void SelectiveByteRleColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead<int8_t>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
  common::Filter* filter =
      scanSpec_->filter() ? scanSpec_->filter() : &Filters::alwaysTrue;
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<false>(rows, scanSpec_->valueHook());
      }
      return;
    }
    if (isDense) {
      processFilter<true>(filter, ExtractToReader(this), rows);
    } else {
      processFilter<false>(filter, ExtractToReader(this), rows);
    }
  } else {
    if (isDense) {
      processFilter<true>(filter, DropValues(), rows);
    } else {
      processFilter<false>(filter, DropValues(), rows);
    }
  }
}

class SelectiveIntegerDirectColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = int64_t;

  SelectiveIntegerDirectColumnReader(
      const EncodingKey& ek,
      const std::shared_ptr<const TypeWithId>& requestedType,
      const std::shared_ptr<const TypeWithId>& dataType,
      StripeStreams& stripe,
      uint32_t numBytes,
      common::ScanSpec* scanSpec)
      : SelectiveColumnReader(ek, stripe, scanSpec, dataType->type, true),
        requestedType_(requestedType->type) {
    auto data = ek.forKind(proto::Stream_Kind_DATA);
    bool dataVInts = stripe.getUseVInts(data);
    auto decoder = IntDecoder</*isSigned*/ true>::createDirect(
        stripe.getStream(data, true), dataVInts, numBytes);
    auto rawDecoder = decoder.release();
    auto directDecoder = dynamic_cast<DirectDecoder<true>*>(rawDecoder);
    ints.reset(directDecoder);
  }

  bool hasBulkPath() const override {
    return true;
  }

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder) {
      notNullDecoder->seekToRowGroup(positionsProvider);
    }

    ints->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

 private:
  template <typename ColumnVisitor>
  void readWithVisitor(RowSet rows, ColumnVisitor visitor);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      ExtractValues extractValues,
      RowSet rows);

  template <bool isDence>
  void processValueHook(RowSet rows, ValueHook* hook);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void
  readHelper(common::Filter* filter, RowSet rows, ExtractValues extractValues);

  void getValues(RowSet rows, VectorPtr* result) override {
    getIntValues(rows, requestedType_.get(), result);
  }

  std::unique_ptr<DirectDecoder</*isSigned*/ true>> ints;
  const TypePtr requestedType_;
};

uint64_t SelectiveIntegerDirectColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  ints->skip(numValues);
  return numValues;
}

template <typename ColumnVisitor>
void SelectiveIntegerDirectColumnReader::readWithVisitor(
    RowSet rows,
    ColumnVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  if (nullsInReadRange_) {
    ints->readWithVisitor<true>(nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    ints->readWithVisitor<false>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveIntegerDirectColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (valueSize_) {
    case 2:
      readWithVisitor(
          rows,
          ColumnVisitor<int16_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    case 4:
      readWithVisitor(
          rows,
          ColumnVisitor<int32_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));

      break;

    case 8:
      readWithVisitor(
          rows,
          ColumnVisitor<int64_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;
    default:
      VELOX_FAIL("Unsupported valueSize_ {}", valueSize_);
  }
}

template <bool isDense, typename ExtractValues>
void SelectiveIntegerDirectColumnReader::processFilter(
    common::Filter* filter,
    ExtractValues extractValues,
    RowSet rows) {
  switch (filter ? filter->kind() : FilterKind::kAlwaysTrue) {
    case FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case FilterKind::kIsNull:
      filterNulls<int64_t>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<int64_t>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case FilterKind::kBigintRange:
      readHelper<common::BigintRange, isDense>(filter, rows, extractValues);
      break;
    case FilterKind::kBigintValuesUsingHashTable:
      readHelper<common::BigintValuesUsingHashTable, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kBigintValuesUsingBitmask:
      readHelper<common::BigintValuesUsingBitmask, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

template <bool isDense>
void SelectiveIntegerDirectColumnReader::processValueHook(
    RowSet rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case AggregationHook::kSumBigintToBigint:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<SumHook<int64_t, int64_t>>(hook));
      break;
    case AggregationHook::kBigintMax:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<MinMaxHook<int64_t, false>>(hook));
      break;
    case AggregationHook::kBigintMin:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<MinMaxHook<int64_t, true>>(hook));
      break;
    default:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue, rows, ExtractToGenericHook(hook));
  }
}

void SelectiveIntegerDirectColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  VELOX_WIDTH_DISPATCH(
      sizeOfIntKind(type_->kind()), prepareRead, offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
  common::Filter* filter =
      scanSpec_->filter() ? scanSpec_->filter() : &Filters::alwaysTrue;
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<false>(rows, scanSpec_->valueHook());
      }
      return;
    }
    if (isDense) {
      processFilter<true>(filter, ExtractToReader(this), rows);
    } else {
      processFilter<false>(filter, ExtractToReader(this), rows);
    }
  } else {
    if (isDense) {
      processFilter<true>(filter, DropValues(), rows);
    } else {
      processFilter<false>(filter, DropValues(), rows);
    }
  }
}

enum FilterResult { kUnknown = 0x40, kSuccess = 0x80, kFailure = 0 };

template <typename T>
inline __m256si load8Indices(const T* /*input*/) {
  VELOX_FAIL("Unsupported dictionary index type");
}

template <>
inline __m256si load8Indices(const int32_t* input) {
  return V32::load(input);
}

template <>
inline __m256si load8Indices(const int16_t* input) {
  return V16::as8x32u(*reinterpret_cast<const __m128hi_u*>(input));
}

template <>
inline __m256si load8Indices(const int64_t* input) {
  static __m256si iota = {0, 1, 2, 3, 4, 5, 6, 7};
  return V32::gather32<8>(input, V32::load(&iota));
}

// Copies from 'input' to 'values' and translates  via 'dict'. Only elements
// where 'dictMask' is true at the element's index are translated, else they are
// passed as is. The elements of input that are copied to values with or without
// translation are given by the first 'numBits' elements of 'selected'. There is
// a generic and a V32 specialization of this template. The V32 specialization
// has 'indices' holding the data to translate, which is loaded from input +
// inputIndex.
template <typename T>
inline void storeTranslatePermute(
    const T* input,
    int32_t inputIndex,
    __m256si /*indices*/,
    __m256si selected,
    __m256si dictMask,
    int8_t numBits,
    const T* dict,
    T* values) {
  auto selectedAsInts = reinterpret_cast<int32_t*>(&selected);
  auto inDict = reinterpret_cast<int32_t*>(&dictMask);
  for (auto i = 0; i < numBits; ++i) {
    auto value = input[inputIndex + selectedAsInts[i]];
    if (inDict[selected[i]]) {
      value = dict[value];
    }
    values[i] = value;
  }
}

template <>
inline void storeTranslatePermute(
    const int32_t* /*input*/,
    int32_t /*inputIndex*/,
    __m256si indices,
    __m256si selected,
    __m256si dictMask,
    int8_t /*numBits*/,
    const int32_t* dict,
    int32_t* values) {
  auto translated = V32::maskGather32(indices, dictMask, dict, indices);
  simd::storePermute(values, translated, selected);
}

// Stores 8 elements starting at 'input' + 'inputIndex' into
// 'values'. The values are translated via 'dict' for the positions
// that are true in 'dictMask'.
template <typename T>
inline void storeTranslate(
    const T* input,
    int32_t inputIndex,
    __m256si /*indices*/,
    __m256si dictMask,
    const T* dict,
    T* values) {
  int32_t* inDict = reinterpret_cast<int32_t*>(&dictMask);
  for (auto i = 0; i < 8; ++i) {
    auto value = input[inputIndex + i];
    if (inDict[i]) {
      value = dict[value];
    }
    values[i] = value;
  }
}

template <>
inline void storeTranslate(
    const int32_t* /*input*/,
    int32_t /*inputIndex*/,
    __m256si indices,
    __m256si dictMask,
    const int32_t* dict,
    int32_t* values) {
  V32::store(values, V32::maskGather32(indices, dictMask, dict, indices));
}

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class DictionaryColumnVisitor
    : public ColumnVisitor<T, TFilter, ExtractValues, isDense> {
  using super = ColumnVisitor<T, TFilter, ExtractValues, isDense>;

 public:
  DictionaryColumnVisitor(
      TFilter& filter,
      SelectiveColumnReader* reader,
      RowSet rows,
      ExtractValues values,
      const T* dict,
      const uint64_t* inDict,
      uint8_t* filterCache)
      : ColumnVisitor<T, TFilter, ExtractValues, isDense>(
            filter,
            reader,
            rows,
            values),
        dict_(dict),
        inDict_(inDict),
        filterCache_(filterCache),
        width_(
            reader->type()->kind() == TypeKind::BIGINT        ? 8
                : reader->type()->kind() == TypeKind::INTEGER ? 4
                                                              : 2) {}

  FOLLY_ALWAYS_INLINE bool isInDict() {
    if (inDict_) {
      return bits::isBitSet(inDict_, super::currentRow());
    }
    return true;
  }

  FOLLY_ALWAYS_INLINE vector_size_t process(T value, bool& atEnd) {
    if (!isInDict()) {
      // If reading fixed width values, the not in dictionary value will be read
      // as unsigned at the width of the type. Integer columns are signed, so
      // sign extend the value here.
      if (LIKELY(width_ == 8)) {
        // No action. This should be the most common case.
      } else if (width_ == 4) {
        value = static_cast<int32_t>(value);
      } else {
        value = static_cast<int16_t>(value);
      }
      return super::process(value, atEnd);
    }
    vector_size_t previous =
        isDense && TFilter::deterministic ? 0 : super::currentRow();
    T valueInDictionary = dict_[value];
    if (std::is_same<TFilter, common::AlwaysTrue>::value) {
      super::filterPassed(valueInDictionary);
    } else {
      // check the dictionary cache
      if (TFilter::deterministic &&
          filterCache_[value] == FilterResult::kSuccess) {
        super::filterPassed(valueInDictionary);
      } else if (
          TFilter::deterministic &&
          filterCache_[value] == FilterResult::kFailure) {
        super::filterFailed();
      } else {
        if (super::filter_.testInt64(valueInDictionary)) {
          super::filterPassed(valueInDictionary);
          if (TFilter::deterministic) {
            filterCache_[value] = FilterResult::kSuccess;
          }
        } else {
          super::filterFailed();
          if (TFilter::deterministic) {
            filterCache_[value] = FilterResult::kFailure;
          }
        }
      }
    }
    if (++super::rowIndex_ >= super::numRows_) {
      atEnd = true;
      return (isDense && TFilter::deterministic)
          ? 0
          : super::rowAt(super::numRows_ - 1) - previous;
    }
    if (isDense && TFilter::deterministic) {
      return 0;
    }
    return super::currentRow() - previous - 1;
  }

  // Use for replacing all rows with non-null rows for fast path with
  // processRun and processRle.
  void setRows(folly::Range<const int32_t*> newRows) {
    super::rows_ = newRows.data();
    super::numRows_ = newRows.size();
  }

  // Processes 'numInput' dictionary indices in 'input'. Sets 'values'
  // and 'numValues'' to the resulting values. If hasFilter is true,
  // only values passing filter are put in 'values' and the indices of
  // the passing rows are put in the corresponding position in
  // 'filterHits'. 'scatterRows' may be non-null if there is no filter and the
  // decoded values should be scattered into values with gaps in between so as
  // to leave gaps  for nulls. If scatterRows is given, the ith value goes to
  // values[scatterRows[i]], else it goes to 'values[i]'. If 'hasFilter' is
  // true, the passing values are written to consecutive places in 'values'.
  template <bool hasFilter, bool hasHook, bool scatter>
  void processRun(
      const T* input,
      int32_t numInput,
      const int32_t* scatterRows,
      int32_t* filterHits,
      T* values,
      int32_t& numValues) {
    DCHECK_EQ(input, values + numValues);
    if (!hasFilter) {
      if (hasHook) {
        translateByDict(input, numInput, values);
        super::values_.hook().addValues(
            scatter ? scatterRows + super::rowIndex_
                    : velox::iota(super::numRows_, super::innerNonNullRows()) +
                    super::rowIndex_,
            values,
            numInput,
            sizeof(T));
        super::rowIndex_ += numInput;
        return;
      }
      if (inDict_) {
        translateScatter<true, scatter>(
            input, numInput, scatterRows, numValues, values);
      } else {
        translateScatter<false, scatter>(
            input, numInput, scatterRows, numValues, values);
      }
      super::rowIndex_ += numInput;
      numValues = scatter ? scatterRows[super::rowIndex_ - 1] + 1
                          : numValues + numInput;
      return;
    }
    // The filter path optionally extracts values but always sets
    // filterHits. It first loads a vector of indices. It translates
    // those indices that refer to dictionary via the dictionary in
    // bulk. It checks the dictionary filter cache 8 values at a
    // time. It calls the scalar filter for the indices that were not
    // found in the cache. It gets a bitmask of up to 8 filter
    // results. It stores these in filterHits. If values are to be
    // written, the passing bitmap is used to load a permute mask to
    // permute the passing values to the left of a vector register and
    // write  the whole register to the end of 'values'
    constexpr bool kFilterOnly =
        std::is_same<typename super::Extract, DropValues>::value;
    constexpr int32_t kWidth = V32::VSize;
    int32_t last = numInput & ~(kWidth - 1);
    for (auto i = 0; i < numInput; i += kWidth) {
      int8_t width = UNLIKELY(i == last) ? numInput - last : 8;
      auto indices = load8Indices(input + i);
      __m256si dictMask;
      if (inDict_) {
        if (simd::isDense(super::rows_ + super::rowIndex_ + i, width)) {
          dictMask = load8MaskDense(
              inDict_, super::rows_[super::rowIndex_ + i], width);
        } else {
          dictMask = load8MaskSparse(
              inDict_, super::rows_ + super::rowIndex_ + i, width);
        }
      } else {
        dictMask = V32::leadingMask(width);
      }

      // Load 8 filter cache values. Defaults the extra to values to 0 if
      // loading less than 8.
      V32::TV cache = V32::maskGather32<1>(
          V32::setAll(0), dictMask, filterCache_ - 3, indices);
      auto unknowns = V32::compareResult((cache & (kUnknown << 24)) << 1);
      auto passed = V32::compareBitMask(V32::compareResult(cache));
      if (UNLIKELY(unknowns)) {
        // Ranges only over inputs that are in dictionary, the not in dictionary
        // were masked off in 'dictMask'.
        uint16_t bits = V32::compareBitMask(unknowns);
        while (bits) {
          int index = bits::getAndClearLastSetBit(bits);
          auto value = input[i + index];
          if (applyFilter(super::filter_, dict_[value])) {
            filterCache_[value] = FilterResult::kSuccess;
            passed |= 1 << index;
          } else {
            filterCache_[value] = FilterResult::kFailure;
          }
        }
      }
      // Were there values not in dictionary?
      if (inDict_) {
        auto mask = V32::compareResult(dictMask);
        if (mask != V32::kAllTrue) {
          uint16_t bits = (~V32::compareBitMask(mask)) & bits::lowMask(kWidth);
          while (bits) {
            auto index = bits::getAndClearLastSetBit(bits);
            if (i + index >= numInput) {
              break;
            }
            if (common::applyFilter(super::filter_, input[i + index])) {
              passed |= 1 << index;
            }
          }
        }
      }
      // We know 8 compare results. If all false, process next batch.
      if (!passed) {
        continue;
      } else if (passed == (1 << V32::VSize) - 1) {
        // All passed, no need to shuffle the indices or values, write then to
        // 'values' and 'filterHits'.
        V32::store(
            filterHits + numValues,
            V32::load(
                (scatter ? scatterRows : super::rows_) + super::rowIndex_ + i));
        if (!kFilterOnly) {
          storeTranslate(
              input, i, indices, dictMask, dict_, values + numValues);
        }
        numValues += kWidth;
      } else {
        // Some passed. Permute  the passing row numbers and values to the left
        // of the SIMD vector and store.
        int8_t numBits = __builtin_popcount(passed);
        auto setBits = V32::load(&V32::byteSetBits()[passed]);
        simd::storePermute(
            filterHits + numValues,
            V32::load(
                (scatter ? scatterRows : super::rows_) + super::rowIndex_ + i),
            setBits);
        if (!kFilterOnly) {
          storeTranslatePermute(
              input,
              i,
              indices,
              setBits,
              dictMask,
              numBits,
              dict_,
              values + numValues);
        }
        numValues += numBits;
      }
    }
    super::rowIndex_ += numInput;
  }

  template <bool hasFilter, bool hasHook, bool scatter>
  void processRle(
      T value,
      T delta,
      int32_t numRows,
      int32_t currentRow,
      const int32_t* scatterRows,
      int32_t* filterHits,
      T* values,
      int32_t& numValues) {
    if (sizeof(T) == 8) {
      constexpr int32_t kWidth = V64::VSize;
      for (auto i = 0; i < numRows; i += kWidth) {
        auto numbers =
            V64::from32u(
                V64::loadGather32Indices(super::rows_ + super::rowIndex_ + i) -
                currentRow) *
                delta +
            value;
        V64::store(values + numValues + i, numbers);
      }
    } else if (sizeof(T) == 4) {
      constexpr int32_t kWidth = V32::VSize;
      for (auto i = 0; i < numRows; i += kWidth) {
        auto numbers =
            (V32::load(super::rows_ + super::rowIndex_ + i) - currentRow) *
                static_cast<int32_t>(delta) +
            static_cast<int32_t>(value);
        V32::store(values + numValues + i, numbers);
      }
    } else {
      for (auto i = 0; i < numRows; ++i) {
        values[numValues + i] =
            (super::rows_[super::rowIndex_ + i] - currentRow) * delta + value;
      }
    }

    processRun<hasFilter, hasHook, scatter>(
        values + numValues,
        numRows,
        scatterRows,
        filterHits,
        values,
        numValues);
  }

 private:
  template <bool hasInDict, bool scatter>
  void translateScatter(
      const T* input,
      int32_t numInput,
      const int32_t* scatterRows,
      int32_t numValues,
      T* values) {
    for (int32_t i = numInput - 1; i >= 0; --i) {
      using U = typename std::make_unsigned<T>::type;
      T value = input[i];
      if (hasInDict) {
        if (bits::isBitSet(inDict_, super::rows_[super::rowIndex_ + i])) {
          value = dict_[static_cast<U>(value)];
        } else if (!scatter) {
          continue;
        }
      } else {
        value = dict_[static_cast<U>(value)];
      }
      if (scatter) {
        values[scatterRows[super::rowIndex_ + i]] = value;
      } else {
        values[numValues + i] = value;
      }
    }
  }

  // Returns 'numBits' bits starting at bit 'index' in 'bits' as a
  // 8x32 mask. This is used as a mask for maskGather to load selected
  // lanes from a dictionary.
  __m256si load8MaskDense(const uint64_t* bits, int32_t index, int8_t numBits) {
    uint8_t shift = index & 7;
    uint32_t byte = index >> 3;
    auto asBytes = reinterpret_cast<const uint8_t*>(bits);
    auto mask = (*reinterpret_cast<const int16_t*>(asBytes + byte) >> shift) &
        bits::lowMask(numBits);
    return V32::mask(mask);
  }

  // Returns 'numBits' bits at bit offsets in 'rows' from 'bits' as a
  // 8x32 mask for use in maskGather.
  __m256si
  load8MaskSparse(const uint64_t* bits, const int32_t* rows, int8_t numRows) {
    // Computes 8 byte addresses, and 8 bit masks. The low bits of the
    // row select the bit mask, the rest of the bits are the byte
    // offset. There is an AND wich will be zero if the bit is not
    // set. This is finally converted to a mask with a negated SIMD
    // comparison with 0. The negate is a xor with -1.
    static __m256si byteBits = {1, 2, 4, 8, 16, 32, 64, 128};
    auto zero = V32::setAll(0);
    auto indicesV = V32::load(rows);
    auto loadMask = V32::leadingMask(numRows);
    auto maskV = (__m256si)_mm256_permutevar8x32_epi32(
        (__m256i)byteBits, (__m256i)(indicesV & 7));
    auto data = V32::maskGather32<1>(zero, loadMask, bits, indicesV >> 3);
    return V32::compareEq(data & maskV, V32::setAll(0)) ^ -1;
  }

  void translateByDict(const T* values, int numValues, T* out) {
    if (!inDict_) {
      for (auto i = 0; i < numValues; ++i) {
        out[i] = dict_[values[i]];
      }
    } else if (super::dense) {
      bits::forEachSetBit(
          inDict_,
          super::rowIndex_,
          super::rowIndex_ + numValues,
          [&](int row) {
            auto valueIndex = row - super::rowIndex_;
            out[valueIndex] = dict_[values[valueIndex]];
            return true;
          });
    } else {
      for (auto i = 0; i < numValues; ++i) {
        if (bits::isBitSet(inDict_, super::rows_[super::rowIndex_ + i])) {
          out[i] = dict_[values[i]];
        }
      }
    }
  }

 protected:
  const T* const dict_;
  const uint64_t* const inDict_;
  uint8_t* filterCache_;
  vector_size_t nullCount_ = 0;
  const uint8_t width_;
};

class SelectiveIntegerDictionaryColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = int64_t;

  SelectiveIntegerDictionaryColumnReader(
      const EncodingKey& ek,
      const std::shared_ptr<const TypeWithId>& requestedType,
      const std::shared_ptr<const TypeWithId>& dataType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec,
      uint32_t numBytes);

  bool hasBulkPath() const override {
    return true;
  }

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder) {
      notNullDecoder->seekToRowGroup(positionsProvider);
    }

    if (inDictionaryReader_) {
      inDictionaryReader_->seekToRowGroup(positionsProvider);
    }

    dataReader_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override {
    getIntValues(rows, requestedType_.get(), result);
  }

 private:
  template <typename ColumnVisitor>
  void readWithVisitor(RowSet rows, ColumnVisitor visitor);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      ExtractValues extractValues,
      RowSet rows);

  template <bool isDence>
  void processValueHook(RowSet rows, ValueHook* hook);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void
  readHelper(common::Filter* filter, RowSet rows, ExtractValues extractValues);

  void ensureInitialized();

  BufferPtr dictionary_;
  BufferPtr inDictionary_;
  std::unique_ptr<ByteRleDecoder> inDictionaryReader_;
  std::unique_ptr<IntDecoder</* isSigned = */ false>> dataReader_;
  uint64_t dictionarySize_;
  std::unique_ptr<IntDecoder</* isSigned = */ true>> dictReader_;
  std::function<BufferPtr()> dictInit_;
  raw_vector<uint8_t> filterCache_;
  RleVersion rleVersion_;
  const TypePtr requestedType_;
  bool initialized_{false};
};

SelectiveIntegerDictionaryColumnReader::SelectiveIntegerDictionaryColumnReader(
    const EncodingKey& ek,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec,
    uint32_t numBytes)
    : SelectiveColumnReader(ek, stripe, scanSpec, dataType->type, true),
      requestedType_(requestedType->type) {
  auto encoding = stripe.getEncoding(ek);
  dictionarySize_ = encoding.dictionarysize();
  rleVersion_ = convertRleVersion(encoding.kind());
  auto data = ek.forKind(proto::Stream_Kind_DATA);
  bool dataVInts = stripe.getUseVInts(data);
  dataReader_ = IntDecoder</* isSigned = */ false>::createRle(
      stripe.getStream(data, true),
      rleVersion_,
      memoryPool,
      dataVInts,
      numBytes);

  // make a lazy dictionary initializer
  dictInit_ = stripe.getIntDictionaryInitializerForNode(ek, numBytes, numBytes);

  auto inDictStream =
      stripe.getStream(ek.forKind(proto::Stream_Kind_IN_DICTIONARY), false);
  if (inDictStream) {
    inDictionaryReader_ = createBooleanRleDecoder(std::move(inDictStream), ek);
  }
}

uint64_t SelectiveIntegerDictionaryColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  dataReader_->skip(numValues);
  if (inDictionaryReader_) {
    inDictionaryReader_->skip(numValues);
  }
  return numValues;
}

template <typename ColumnVisitor>
void SelectiveIntegerDictionaryColumnReader::readWithVisitor(
    RowSet rows,
    ColumnVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  VELOX_CHECK(rleVersion_ == RleVersion_1);
  auto reader = reinterpret_cast<RleDecoderV1<false>*>(dataReader_.get());
  if (nullsInReadRange_) {
    reader->readWithVisitor<true>(nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    reader->readWithVisitor<false>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveIntegerDictionaryColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (valueSize_) {
    case 2:
      readWithVisitor(
          rows,
          DictionaryColumnVisitor<int16_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter),
              this,
              rows,
              extractValues,
              dictionary_->as<int16_t>(),
              inDictionary_ ? inDictionary_->as<uint64_t>() : nullptr,
              filterCache_.empty() ? nullptr : filterCache_.data()));
      break;
    case 4:
      readWithVisitor(
          rows,
          DictionaryColumnVisitor<int32_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter),
              this,
              rows,
              extractValues,
              dictionary_->as<int32_t>(),
              inDictionary_ ? inDictionary_->as<uint64_t>() : nullptr,
              filterCache_.empty() ? nullptr : filterCache_.data()));
      break;

    case 8:
      readWithVisitor(
          rows,
          DictionaryColumnVisitor<int64_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter),
              this,
              rows,
              extractValues,
              dictionary_->as<int64_t>(),
              inDictionary_ ? inDictionary_->as<uint64_t>() : nullptr,
              filterCache_.empty() ? nullptr : filterCache_.data()));
      break;

    default:
      VELOX_FAIL("Unsupported valueSize_ {}", valueSize_);
  }
}

template <bool isDense, typename ExtractValues>
void SelectiveIntegerDictionaryColumnReader::processFilter(
    common::Filter* filter,
    ExtractValues extractValues,
    RowSet rows) {
  switch (filter ? filter->kind() : FilterKind::kAlwaysTrue) {
    case FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case FilterKind::kIsNull:
      filterNulls<int64_t>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<int64_t>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case FilterKind::kBigintRange:
      readHelper<common::BigintRange, isDense>(filter, rows, extractValues);
      break;
    case FilterKind::kBigintValuesUsingHashTable:
      readHelper<common::BigintValuesUsingHashTable, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kBigintValuesUsingBitmask:
      readHelper<common::BigintValuesUsingBitmask, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

template <bool isDense>
void SelectiveIntegerDictionaryColumnReader::processValueHook(
    RowSet rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case AggregationHook::kSumBigintToBigint:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<SumHook<int64_t, int64_t>>(hook));
      break;
    default:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue, rows, ExtractToGenericHook(hook));
  }
}

void SelectiveIntegerDictionaryColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  VELOX_WIDTH_DISPATCH(
      sizeOfIntKind(type_->kind()), prepareRead, offset, rows, incomingNulls);
  auto end = rows.back() + 1;
  const auto* rawNulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;

  // read the stream of booleans indicating whether a given data entry
  // is an offset or a literal value.
  if (inDictionaryReader_) {
    bool isBulk = useBulkPath();
    int32_t numFlags = (isBulk && nullsInReadRange_)
        ? bits::countNonNulls(nullsInReadRange_->as<uint64_t>(), 0, end)
        : end;
    ensureCapacity<uint64_t>(
        inDictionary_, bits::nwords(numFlags), &memoryPool);
    inDictionaryReader_->next(
        inDictionary_->asMutable<char>(),
        numFlags,
        isBulk ? nullptr : rawNulls);
  }

  // lazy load dictionary only when it's needed
  ensureInitialized();

  bool isDense = rows.back() == rows.size() - 1;
  common::Filter* filter = scanSpec_->filter();
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<false>(rows, scanSpec_->valueHook());
      }
      return;
    }
    if (isDense) {
      processFilter<true>(filter, ExtractToReader(this), rows);
    } else {
      processFilter<false>(filter, ExtractToReader(this), rows);
    }
  } else {
    if (isDense) {
      processFilter<true>(filter, DropValues(), rows);
    } else {
      processFilter<false>(filter, DropValues(), rows);
    }
  }
}

void SelectiveIntegerDictionaryColumnReader::ensureInitialized() {
  if (LIKELY(initialized_)) {
    return;
  }

  Timer timer;
  dictionary_ = dictInit_();

  filterCache_.resize(dictionarySize_);
  simd::memset(filterCache_.data(), FilterResult::kUnknown, dictionarySize_);
  initialized_ = true;
  initTimeClocks_ = timer.elapsedClocks();
}

template <typename TData, typename TRequested>
class SelectiveFloatingPointColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = TRequested;
  SelectiveFloatingPointColumnReader(
      const EncodingKey& ek,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec);

  // Offers fast path only if data and result widths match.
  bool hasBulkPath() const override {
    return std::is_same<TData, TRequested>::value;
  }

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder) {
      notNullDecoder->seekToRowGroup(positionsProvider);
    }

    decoder_.seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;
  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override {
    getFlatValues<TRequested, TRequested>(rows, result);
  }

 private:
  template <typename TVisitor>
  void readWithVisitor(RowSet rows, TVisitor visitor);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void readHelper(common::Filter* filter, RowSet rows, ExtractValues values);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);

  template <bool isDense>
  void processValueHook(RowSet rows, ValueHook* hook);

  FloatingPointDecoder<TData, TRequested> decoder_;
};

template <typename TData, typename TRequested>
SelectiveFloatingPointColumnReader<TData, TRequested>::
    SelectiveFloatingPointColumnReader(
        const EncodingKey& ek,
        StripeStreams& stripe,
        common::ScanSpec* scanSpec)
    : SelectiveColumnReader(
          ek,
          stripe,
          scanSpec,
          CppToType<TData>::create(),
          true),
      decoder_(stripe.getStream(ek.forKind(proto::Stream_Kind_DATA), true)) {}

template <typename TData, typename TRequested>
uint64_t SelectiveFloatingPointColumnReader<TData, TRequested>::skip(
    uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  decoder_.skip(numValues);
  return numValues;
}

template <typename TData, typename TRequested>
template <typename TVisitor>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readWithVisitor(
    RowSet rows,
    TVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  if (nullsInReadRange_) {
    decoder_.template readWithVisitor<true, TVisitor>(
        SelectiveColumnReader::nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    decoder_.template readWithVisitor<false, TVisitor>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TData, typename TRequested>
template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveFloatingPointColumnReader<TData, TRequested>::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  readWithVisitor(
      rows,
      ColumnVisitor<TRequested, TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <typename TData, typename TRequested>
template <bool isDense, typename ExtractValues>
void SelectiveFloatingPointColumnReader<TData, TRequested>::processFilter(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : FilterKind::kAlwaysTrue) {
    case FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case FilterKind::kIsNull:
      filterNulls<TRequested>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<TRequested>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case FilterKind::kDoubleRange:
    case FilterKind::kFloatRange:
      readHelper<common::FloatingPointRange<TData>, isDense>(
          filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

template <typename TData, typename TRequested>
template <bool isDense>
void SelectiveFloatingPointColumnReader<TData, TRequested>::processValueHook(
    RowSet rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case AggregationHook::kSumFloatToDouble:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<SumHook<float, double>>(hook));
      break;
    case AggregationHook::kSumDoubleToDouble:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<SumHook<double, double>>(hook));
      break;
    case AggregationHook::kFloatMax:
    case AggregationHook::kDoubleMax:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<MinMaxHook<TRequested, false>>(hook));
      break;
    case AggregationHook::kFloatMin:
    case AggregationHook::kDoubleMin:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue,
          rows,
          ExtractToHook<MinMaxHook<TRequested, true>>(hook));
      break;
    default:
      readHelper<common::AlwaysTrue, isDense>(
          &Filters::alwaysTrue, rows, ExtractToGenericHook(hook));
  }
}

template <typename TData, typename TRequested>
void SelectiveFloatingPointColumnReader<TData, TRequested>::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead<TRequested>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<false>(rows, scanSpec_->valueHook());
      }
      return;
    }
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, ExtractToReader(this));
    } else {
      processFilter<false>(scanSpec_->filter(), rows, ExtractToReader(this));
    }
  } else {
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, DropValues());
    } else {
      processFilter<false>(scanSpec_->filter(), rows, DropValues());
    }
  }
}

class SelectiveStringDirectColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = StringView;
  SelectiveStringDirectColumnReader(
      const EncodingKey& ek,
      const std::shared_ptr<const TypeWithId>& type,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec);

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder) {
      notNullDecoder->seekToRowGroup(positionsProvider);
    }

    blobStream_->seekToRowGroup(positionsProvider);
    lengthDecoder_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());

    bytesToSkip_ = 0;
    bufferStart_ = bufferEnd_;
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override {
    rawStringBuffer_ = nullptr;
    rawStringSize_ = 0;
    rawStringUsed_ = 0;
    getFlatValues<StringView, StringView>(rows, result, type_);
  }

 private:
  template <bool hasNulls>
  void skipInDecode(int32_t numValues, int32_t current, const uint64_t* nulls);

  folly::StringPiece readValue(int32_t length);

  template <bool hasNulls, typename Visitor>
  void decode(const uint64_t* nulls, Visitor visitor);

  template <typename TVisitor>
  void readWithVisitor(RowSet rows, TVisitor visitor);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void readHelper(common::Filter* filter, RowSet rows, ExtractValues values);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);

  void extractCrossBuffers(
      const int32_t* lengths,
      const int32_t* starts,
      int32_t rowIndex,
      int32_t numValues);

  inline void makeSparseStarts(
      int32_t startRow,
      const int32_t* rows,
      int32_t numRows,
      int32_t* starts);

  inline void extractNSparse(const int32_t* rows, int32_t row, int numRows);

  void extractSparse(const int32_t* rows, int32_t numRows);

  template <bool scatter, bool skip>
  bool try8Consecutive(int32_t start, const int32_t* rows, int32_t row);

  std::unique_ptr<IntDecoder</*isSigned*/ false>> lengthDecoder_;
  std::unique_ptr<SeekableInputStream> blobStream_;
  const char* bufferStart_ = nullptr;
  const char* bufferEnd_ = nullptr;
  BufferPtr lengths_;
  int32_t lengthIndex_ = 0;
  const uint32_t* rawLengths_ = nullptr;
  int64_t bytesToSkip_ = 0;
  // Storage for a string straddling a buffer boundary. Needed for calling
  // the filter.
  std::string tempString_;
};

SelectiveStringDirectColumnReader::SelectiveStringDirectColumnReader(
    const EncodingKey& ek,
    const std::shared_ptr<const TypeWithId>& type,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec)
    : SelectiveColumnReader(ek, stripe, scanSpec, type->type, true) {
  RleVersion rleVersion = convertRleVersion(stripe.getEncoding(ek).kind());
  auto lenId = ek.forKind(proto::Stream_Kind_LENGTH);
  bool lenVInts = stripe.getUseVInts(lenId);
  lengthDecoder_ = IntDecoder</*isSigned*/ false>::createRle(
      stripe.getStream(lenId, true),
      rleVersion,
      memoryPool,
      lenVInts,
      INT_BYTE_SIZE);
  blobStream_ = stripe.getStream(ek.forKind(proto::Stream_Kind_DATA), true);
}

uint64_t SelectiveStringDirectColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  ensureCapacity<int64_t>(lengths_, numValues, &memoryPool);
  lengthDecoder_->nextLengths(lengths_->asMutable<int32_t>(), numValues);
  rawLengths_ = lengths_->as<uint32_t>();
  for (auto i = 0; i < numValues; ++i) {
    bytesToSkip_ += rawLengths_[i];
  }
  skipBytes(bytesToSkip_, blobStream_.get(), bufferStart_, bufferEnd_);
  bytesToSkip_ = 0;
  return numValues;
}

void SelectiveStringDirectColumnReader::extractCrossBuffers(
    const int32_t* lengths,
    const int32_t* starts,
    int32_t rowIndex,
    int32_t numValues) {
  int32_t current = 0;
  bool scatter = !outerNonNullRows_.empty();
  for (auto i = 0; i < numValues; ++i) {
    auto gap = starts[i] - current;
    bytesToSkip_ += gap;
    auto size = lengths[i];
    auto value = readValue(size);
    current += size + gap;
    if (!scatter) {
      addValue(value);
    } else {
      auto index = outerNonNullRows_[rowIndex + i];
      if (size <= StringView::kInlineSize) {
        reinterpret_cast<StringView*>(rawValues_)[index] =
            StringView(value.data(), size);
      } else {
        auto copy = copyStringValue(value);
        reinterpret_cast<StringView*>(rawValues_)[index] =
            StringView(copy, size);
      }
    }
  }
  skipBytes(bytesToSkip_, blobStream_.get(), bufferStart_, bufferEnd_);
  bytesToSkip_ = 0;
  if (scatter) {
    numValues_ = outerNonNullRows_[rowIndex + numValues - 1] + 1;
  }
}

inline int32_t
rangeSum(const uint32_t* rows, int32_t start, int32_t begin, int32_t end) {
  for (auto i = begin; i < end; ++i) {
    start += rows[i];
  }
  return start;
}

inline void SelectiveStringDirectColumnReader::makeSparseStarts(
    int32_t startRow,
    const int32_t* rows,
    int32_t numRows,
    int32_t* starts) {
  auto previousRow = lengthIndex_;
  int32_t i = 0;
  int32_t startOffset = 0;
  for (; i < numRows; ++i) {
    int targetRow = rows[startRow + i];
    startOffset = rangeSum(rawLengths_, startOffset, previousRow, targetRow);
    starts[i] = startOffset;
    previousRow = targetRow + 1;
    startOffset += rawLengths_[targetRow];
  }
}

void SelectiveStringDirectColumnReader::extractNSparse(
    const int32_t* rows,
    int32_t row,
    int32_t numValues) {
  int32_t starts[8];
  if (numValues == 8 &&
      (outerNonNullRows_.empty() ? try8Consecutive<false, true>(0, rows, row)
                                 : try8Consecutive<true, true>(0, rows, row))) {
    return;
  }
  int32_t lengths[8];
  for (auto i = 0; i < numValues; ++i) {
    lengths[i] = rawLengths_[rows[row + i]];
  }
  makeSparseStarts(row, rows, numValues, starts);
  extractCrossBuffers(lengths, starts, row, numValues);
  lengthIndex_ = rows[row + numValues - 1] + 1;
}

template <bool scatter, bool sparse>
inline bool SelectiveStringDirectColumnReader::try8Consecutive(
    int32_t start,
    const int32_t* rows,
    int32_t row) {
  const char* data = bufferStart_ + start + bytesToSkip_;
  if (!data || bufferEnd_ - data < start + 8 * 12) {
    return false;
  }
  int32_t* result = reinterpret_cast<int32_t*>(rawValues_);
  int32_t resultIndex = numValues_ * 4 - 4;
  auto rawUsed = rawStringUsed_;
  auto previousRow = sparse ? lengthIndex_ : 0;
  auto endRow = row + 8;
  for (auto i = row; i < endRow; ++i) {
    if (scatter) {
      resultIndex = outerNonNullRows_[i] * 4;
    } else {
      resultIndex += 4;
    }
    if (sparse) {
      auto targetRow = rows[i];
      data += rangeSum(rawLengths_, 0, previousRow, rows[i]);
      previousRow = targetRow + 1;
    }
    auto length = rawLengths_[rows[i]];
    if (sparse && data + length > bufferEnd_) {
      return false;
    }
    result[resultIndex] = length;
    auto first16 = *reinterpret_cast<const __m128_u*>(data);
    *reinterpret_cast<__m128_u*>(result + resultIndex + 1) = first16;
    if (length <= 12) {
      data += length;
      *reinterpret_cast<int64_t*>(
          reinterpret_cast<char*>(result + resultIndex + 1) + length) = 0;
      continue;
    }
    if (!rawStringBuffer_ || rawUsed + length > rawStringSize_ ||
        bufferEnd_ - data < length + 8 * 12) {
      // Slow path if no space in raw strings or less than length and 8
      // inlined left in buffer.
      return false;
    }
    *reinterpret_cast<char**>(result + resultIndex + 2) =
        rawStringBuffer_ + rawUsed;
    *reinterpret_cast<__m128_u*>(rawStringBuffer_ + rawUsed) = first16;
    if (length > 16) {
      simd::memcpy(
          rawStringBuffer_ + rawUsed + 16,
          data + 16,
          bits::roundUp(length - 16, 16));
    }
    rawUsed += length;
    data += length;
  }
  // Update the data members only after successful completion.
  bufferStart_ = data;
  bytesToSkip_ = 0;
  rawStringUsed_ = rawUsed;
  numValues_ = scatter ? outerNonNullRows_[row + 7] + 1 : numValues_ + 8;
  lengthIndex_ = sparse ? rows[row + 7] + 1 : lengthIndex_ + 8;
  return true;
}

void SelectiveStringDirectColumnReader::extractSparse(
    const int32_t* rows,
    int32_t numRows) {
  rowLoop(
      rows,
      0,
      numRows,
      8,
      [&](int32_t row) {
        int32_t start = rangeSum(rawLengths_, 0, lengthIndex_, rows[row]);
        lengthIndex_ = rows[row];
        auto lengths =
            reinterpret_cast<const int32_t*>(rawLengths_) + lengthIndex_;

        if (outerNonNullRows_.empty()
                ? try8Consecutive<false, false>(start, rows, row)
                : try8Consecutive<true, false>(start, rows, row)) {
          return;
        }
        int32_t starts[8];
        for (auto i = 0; i < 8; ++i) {
          starts[i] = start;
          start += lengths[i];
        }
        lengthIndex_ += 8;
        extractCrossBuffers(lengths, starts, row, 8);
      },
      [&](int32_t row) { extractNSparse(rows, row, 8); },
      [&](int32_t row, int32_t numRows) {
        extractNSparse(rows, row, numRows);
      });
}

template <bool hasNulls>
void SelectiveStringDirectColumnReader::skipInDecode(
    int32_t numValues,
    int32_t current,
    const uint64_t* nulls) {
  if (hasNulls) {
    numValues = bits::countNonNulls(nulls, current, current + numValues);
  }
  for (size_t i = lengthIndex_; i < lengthIndex_ + numValues; ++i) {
    bytesToSkip_ += rawLengths_[i];
  }
  lengthIndex_ += numValues;
}

folly::StringPiece SelectiveStringDirectColumnReader::readValue(
    int32_t length) {
  skipBytes(bytesToSkip_, blobStream_.get(), bufferStart_, bufferEnd_);
  bytesToSkip_ = 0;
  if (bufferStart_ + length <= bufferEnd_) {
    bytesToSkip_ = length;
    return folly::StringPiece(bufferStart_, length);
  }
  tempString_.resize(length);
  readBytes(
      length, blobStream_.get(), tempString_.data(), bufferStart_, bufferEnd_);
  return folly::StringPiece(tempString_);
}

template <bool hasNulls, typename Visitor>
void SelectiveStringDirectColumnReader::decode(
    const uint64_t* nulls,
    Visitor visitor) {
  int32_t current = visitor.start();
  bool atEnd = false;
  bool allowNulls = hasNulls && visitor.allowNulls();
  for (;;) {
    int32_t toSkip;
    if (hasNulls && allowNulls && bits::isBitNull(nulls, current)) {
      toSkip = visitor.processNull(atEnd);
    } else {
      if (hasNulls && !allowNulls) {
        toSkip = visitor.checkAndSkipNulls(nulls, current, atEnd);
        if (!Visitor::dense) {
          skipInDecode<false>(toSkip, current, nullptr);
        }
        if (atEnd) {
          return;
        }
      }

      // Check if length passes the filter first. Don't read the value if length
      // doesn't pass.
      auto length = rawLengths_[lengthIndex_++];
      auto toSkipOptional = visitor.processLength(length, atEnd);
      if (toSkipOptional.has_value()) {
        bytesToSkip_ += length;
        toSkip = toSkipOptional.value();
      } else {
        toSkip = visitor.process(readValue(length), atEnd);
      }
    }
    ++current;
    if (toSkip) {
      skipInDecode<hasNulls>(toSkip, current, nulls);
      current += toSkip;
    }
    if (atEnd) {
      return;
    }
  }
}

template <typename TVisitor>
void SelectiveStringDirectColumnReader::readWithVisitor(
    RowSet rows,
    TVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  int32_t current = visitor.start();
  constexpr bool isExtract =
      std::is_same<typename TVisitor::FilterType, common::AlwaysTrue>::value &&
      std::is_same<
          typename TVisitor::Extract,
          ExtractToReader<SelectiveStringDirectColumnReader>>::value;
  auto nulls = nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;

  if (process::hasAvx2() && isExtract) {
    if (nullsInReadRange_) {
      if (TVisitor::dense) {
        returnReaderNulls_ = true;
        nonNullRowsFromDense(nulls, rows.size(), outerNonNullRows_);
        extractSparse(rows.data(), outerNonNullRows_.size());
      } else {
        int32_t tailSkip = -1;
        anyNulls_ = nonNullRowsFromSparse<false, true>(
            nulls,
            rows,
            innerNonNullRows_,
            outerNonNullRows_,
            rawResultNulls_,
            tailSkip);
        extractSparse(innerNonNullRows_.data(), innerNonNullRows_.size());
        skipInDecode<false>(tailSkip, 0, nullptr);
      }
    } else {
      extractSparse(rows.data(), rows.size());
    }
    numValues_ = rows.size();
    readOffset_ += numRows;
    return;
  }

  if (nulls) {
    skipInDecode<true>(current, 0, nulls);
  } else {
    skipInDecode<false>(current, 0, nulls);
  }
  if (nulls) {
    decode<true, TVisitor>(nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    decode<false, TVisitor>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveStringDirectColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  readWithVisitor(
      rows,
      ColumnVisitor<folly::StringPiece, TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <bool isDense, typename ExtractValues>
void SelectiveStringDirectColumnReader::processFilter(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : FilterKind::kAlwaysTrue) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<StringView>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<StringView>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case common::FilterKind::kBytesRange:
      readHelper<common::BytesRange, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kBytesValues:
      readHelper<common::BytesValues, isDense>(filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

void SelectiveStringDirectColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  prepareRead<folly::StringPiece>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;

  auto end = rows.back() + 1;
  auto numNulls =
      nullsInReadRange_ ? BaseVector::countNulls(nullsInReadRange_, 0, end) : 0;
  ensureCapacity<int32_t>(lengths_, end - numNulls, &memoryPool);
  lengthDecoder_->nextLengths(lengths_->asMutable<int32_t>(), end - numNulls);
  rawLengths_ = lengths_->as<uint32_t>();
  lengthIndex_ = 0;
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        readHelper<common::AlwaysTrue, true>(
            &Filters::alwaysTrue,
            rows,
            ExtractToGenericHook(scanSpec_->valueHook()));
      } else {
        readHelper<common::AlwaysTrue, false>(
            &Filters::alwaysTrue,
            rows,
            ExtractToGenericHook(scanSpec_->valueHook()));
      }
      return;
    }
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, ExtractToReader(this));
    } else {
      processFilter<false>(scanSpec_->filter(), rows, ExtractToReader(this));
    }
  } else {
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, DropValues());
    } else {
      processFilter<false>(scanSpec_->filter(), rows, DropValues());
    }
  }
}

class SelectiveStringDictionaryColumnReader : public SelectiveColumnReader {
 public:
  using ValueType = int32_t;

  SelectiveStringDictionaryColumnReader(
      const EncodingKey& ek,
      const std::shared_ptr<const TypeWithId>& type,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec);

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder) {
      notNullDecoder->seekToRowGroup(positionsProvider);
    }

    if (strideDictStream_) {
      strideDictStream_->seekToRowGroup(positionsProvider);
      strideDictLengthDecoder_->seekToRowGroup(positionsProvider);
      // skip row group dictionary size
      positionsProvider.next();
    }

    dictIndex_->seekToRowGroup(positionsProvider);

    if (inDictionaryReader_) {
      inDictionaryReader_->seekToRowGroup(positionsProvider);
    }

    VELOX_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

 private:
  void loadStrideDictionary();
  void makeDictionaryBaseVector();

  template <typename TVisitor>
  void readWithVisitor(RowSet rows, TVisitor visitor);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void readHelper(common::Filter* filter, RowSet rows, ExtractValues values);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);

  BufferPtr loadDictionary(
      uint64_t count,
      SeekableInputStream& data,
      IntDecoder</*isSigned*/ false>& lengthDecoder,
      BufferPtr& offsets);

  void ensureInitialized();

  BufferPtr dictionaryBlob_;
  BufferPtr dictionaryOffset_;
  BufferPtr inDict_;
  BufferPtr strideDict_;
  BufferPtr strideDictOffset_;
  std::unique_ptr<IntDecoder</*isSigned*/ false>> dictIndex_;
  std::unique_ptr<ByteRleDecoder> inDictionaryReader_;
  std::unique_ptr<SeekableInputStream> strideDictStream_;
  std::unique_ptr<IntDecoder</*isSigned*/ false>> strideDictLengthDecoder_;

  FlatVectorPtr<StringView> dictionaryValues_;

  uint64_t dictionaryCount_;
  uint64_t strideDictCount_{0};
  int64_t lastStrideIndex_;
  size_t positionOffset_;
  size_t strideDictSizeOffset_;

  const StrideIndexProvider& provider_;

  // lazy load the dictionary
  std::unique_ptr<IntDecoder</*isSigned*/ false>> lengthDecoder_;
  std::unique_ptr<SeekableInputStream> blobStream_;
  raw_vector<uint8_t> filterCache_;
  bool initialized_{false};
};

SelectiveStringDictionaryColumnReader::SelectiveStringDictionaryColumnReader(
    const EncodingKey& ek,
    const std::shared_ptr<const TypeWithId>& type,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec)
    : SelectiveColumnReader(ek, stripe, scanSpec, type->type, true),
      lastStrideIndex_(-1),
      provider_(stripe.getStrideIndexProvider()) {
  RleVersion rleVersion = convertRleVersion(stripe.getEncoding(ek).kind());
  dictionaryCount_ = stripe.getEncoding(ek).dictionarysize();

  const auto dataId = ek.forKind(proto::Stream_Kind_DATA);
  bool dictVInts = stripe.getUseVInts(dataId);
  dictIndex_ = IntDecoder</*isSigned*/ false>::createRle(
      stripe.getStream(dataId, true),
      rleVersion,
      memoryPool,
      dictVInts,
      INT_BYTE_SIZE);

  const auto lenId = ek.forKind(proto::Stream_Kind_LENGTH);
  bool lenVInts = stripe.getUseVInts(lenId);
  lengthDecoder_ = IntDecoder</*isSigned*/ false>::createRle(
      stripe.getStream(lenId, false),
      rleVersion,
      memoryPool,
      lenVInts,
      INT_BYTE_SIZE);

  blobStream_ =
      stripe.getStream(ek.forKind(proto::Stream_Kind_DICTIONARY_DATA), false);

  // handle in dictionary stream
  std::unique_ptr<SeekableInputStream> inDictStream =
      stripe.getStream(ek.forKind(proto::Stream_Kind_IN_DICTIONARY), false);
  if (inDictStream) {
    DWIO_ENSURE_NOT_NULL(indexStream_, "String index is missing");

    inDictionaryReader_ = createBooleanRleDecoder(std::move(inDictStream), ek);

    // stride dictionary only exists if in dictionary exists
    strideDictStream_ = stripe.getStream(
        ek.forKind(proto::Stream_Kind_STRIDE_DICTIONARY), true);
    DWIO_ENSURE_NOT_NULL(strideDictStream_, "Stride dictionary is missing");

    const auto strideDictLenId =
        ek.forKind(proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH);
    bool strideLenVInt = stripe.getUseVInts(strideDictLenId);
    strideDictLengthDecoder_ = IntDecoder</*isSigned*/ false>::createRle(
        stripe.getStream(strideDictLenId, true),
        rleVersion,
        memoryPool,
        strideLenVInt,
        INT_BYTE_SIZE);
  }
}

uint64_t SelectiveStringDictionaryColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  dictIndex_->skip(numValues);
  if (inDictionaryReader_) {
    inDictionaryReader_->skip(numValues);
  }
  return numValues;
}

BufferPtr SelectiveStringDictionaryColumnReader::loadDictionary(
    uint64_t count,
    SeekableInputStream& data,
    IntDecoder</*isSigned*/ false>& lengthDecoder,
    BufferPtr& offsets) {
  // read lengths from length reader
  auto* offsetsPtr = offsets->asMutable<int64_t>();
  offsetsPtr[0] = 0;
  lengthDecoder.next(offsetsPtr + 1, count, nullptr);

  // set up array that keeps offset of start positions of individual entries
  // in the dictionary
  for (uint64_t i = 1; i < count + 1; ++i) {
    offsetsPtr[i] += offsetsPtr[i - 1];
  }

  // read bytes from underlying string
  int64_t blobSize = offsetsPtr[count];
  BufferPtr dictionary = AlignedBuffer::allocate<char>(blobSize, &memoryPool);
  data.readFully(dictionary->asMutable<char>(), blobSize);
  return dictionary;
}

void SelectiveStringDictionaryColumnReader::loadStrideDictionary() {
  auto nextStride = provider_.getStrideIndex();
  if (nextStride == lastStrideIndex_) {
    return;
  }

  // get stride dictionary size and load it if needed
  auto& positions = index_->entry(nextStride).positions();
  strideDictCount_ = positions.Get(strideDictSizeOffset_);
  if (strideDictCount_ > 0) {
    // seek stride dictionary related streams
    std::vector<uint64_t> pos(
        positions.begin() + positionOffset_, positions.end());
    PositionProvider pp(pos);
    strideDictStream_->seekToRowGroup(pp);
    strideDictLengthDecoder_->seekToRowGroup(pp);

    ensureCapacity<int64_t>(
        strideDictOffset_, strideDictCount_ + 1, &memoryPool);
    strideDict_ = loadDictionary(
        strideDictCount_,
        *strideDictStream_,
        *strideDictLengthDecoder_,
        strideDictOffset_);
  } else {
    strideDict_.reset();
  }

  lastStrideIndex_ = nextStride;

  dictionaryValues_.reset();
  filterCache_.resize(dictionaryCount_ + strideDictCount_);
  simd::memset(
      filterCache_.data(),
      FilterResult::kUnknown,
      dictionaryCount_ + strideDictCount_);
}

void SelectiveStringDictionaryColumnReader::makeDictionaryBaseVector() {
  const auto* dictionaryBlob_Ptr = dictionaryBlob_->as<char>();
  const auto* dictionaryOffset_sPtr = dictionaryOffset_->as<int64_t>();
  if (strideDictCount_) {
    // TODO Reuse memory
    BufferPtr values = AlignedBuffer::allocate<StringView>(
        dictionaryCount_ + strideDictCount_, &memoryPool);
    auto* valuesPtr = values->asMutable<StringView>();
    for (size_t i = 0; i < dictionaryCount_; i++) {
      valuesPtr[i] = StringView(
          dictionaryBlob_Ptr + dictionaryOffset_sPtr[i],
          dictionaryOffset_sPtr[i + 1] - dictionaryOffset_sPtr[i]);
    }

    const auto* strideDictPtr = strideDict_->as<char>();
    const auto* strideDictOffset_Ptr = strideDictOffset_->as<int64_t>();
    for (size_t i = 0; i < strideDictCount_; i++) {
      valuesPtr[dictionaryCount_ + i] = StringView(
          strideDictPtr + strideDictOffset_Ptr[i],
          strideDictOffset_Ptr[i + 1] - strideDictOffset_Ptr[i]);
    }

    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &memoryPool,
        type_,
        BufferPtr(nullptr), // TODO nulls
        dictionaryCount_ + strideDictCount_ /*length*/,
        values,
        std::vector<BufferPtr>{dictionaryBlob_, strideDict_});
  } else {
    // TODO Reuse memory
    BufferPtr values =
        AlignedBuffer::allocate<StringView>(dictionaryCount_, &memoryPool);
    auto* valuesPtr = values->asMutable<StringView>();
    for (size_t i = 0; i < dictionaryCount_; i++) {
      valuesPtr[i] = StringView(
          dictionaryBlob_Ptr + dictionaryOffset_sPtr[i],
          dictionaryOffset_sPtr[i + 1] - dictionaryOffset_sPtr[i]);
    }

    dictionaryValues_ = std::make_shared<FlatVector<StringView>>(
        &memoryPool,
        type_,
        BufferPtr(nullptr), // TODO nulls
        dictionaryCount_ /*length*/,
        values,
        std::vector<BufferPtr>{dictionaryBlob_});
  }
}

template <typename TFilter, typename ExtractValues, bool isDense>
class StringDictionaryColumnVisitor
    : public DictionaryColumnVisitor<int32_t, TFilter, ExtractValues, isDense> {
  using super = ColumnVisitor<int32_t, TFilter, ExtractValues, isDense>;
  using DictSuper =
      DictionaryColumnVisitor<int32_t, TFilter, ExtractValues, isDense>;

 public:
  StringDictionaryColumnVisitor(
      TFilter& filter,
      SelectiveStringDictionaryColumnReader* reader,
      RowSet rows,
      ExtractValues values,
      const uint64_t* inDict,
      uint8_t* filterCache,
      const char* dictBlob,
      const uint64_t* dictOffset,
      vector_size_t baseDictSize,
      const char* strideDictBlob,
      const uint64_t* strideDictOffset)
      : DictionaryColumnVisitor<int32_t, TFilter, ExtractValues, isDense>(
            filter,
            reader,
            rows,
            values,
            nullptr,
            inDict,
            filterCache),
        dictBlob_(dictBlob),
        dictOffset_(dictOffset),
        baseDictSize_(baseDictSize),
        strideDictBlob_(strideDictBlob),
        strideDictOffset_(strideDictOffset) {}

  FOLLY_ALWAYS_INLINE vector_size_t process(int32_t value, bool& atEnd) {
    bool inStrideDict = !DictSuper::isInDict();
    auto index = value;
    if (inStrideDict) {
      index += baseDictSize_;
    }
    vector_size_t previous =
        isDense && TFilter::deterministic ? 0 : super::currentRow();
    if (std::is_same<TFilter, common::AlwaysTrue>::value) {
      super::filterPassed(index);
    } else {
      // check the dictionary cache
      if (TFilter::deterministic &&
          DictSuper::filterCache_[index] == FilterResult::kSuccess) {
        super::filterPassed(index);
      } else if (
          TFilter::deterministic &&
          DictSuper::filterCache_[index] == FilterResult::kFailure) {
        super::filterFailed();
      } else {
        if (common::applyFilter(
                super::filter_, valueInDictionary(value, inStrideDict))) {
          super::filterPassed(index);
          if (TFilter::deterministic) {
            DictSuper::filterCache_[index] = FilterResult::kSuccess;
          }
        } else {
          super::filterFailed();
          if (TFilter::deterministic) {
            DictSuper::filterCache_[index] = FilterResult::kFailure;
          }
        }
      }
    }
    if (++super::rowIndex_ >= super::numRows_) {
      atEnd = true;
      return (TFilter::deterministic && isDense)
          ? 0
          : super::rows_[super::numRows_ - 1] - previous;
    }
    if (isDense && TFilter::deterministic) {
      return 0;
    }
    return super::currentRow() - previous - 1;
  }

  // Feeds'numValues' items starting at 'values' to the result. If
  // projecting out do nothing. If hook, call hook. If filter, apply
  // and produce hits and if not filter only compact the values to
  // remove non-passing. Returns the number of values in the result
  // after processing.
  template <bool hasFilter, bool hasHook, bool scatter>
  void processRun(
      const int32_t* input,
      int32_t numInput,
      const int32_t* scatterRows,
      int32_t* filterHits,
      int32_t* values,
      int32_t& numValues) {
    setByInDict(values, numInput);
    if (!hasFilter) {
      if (hasHook) {
        for (auto i = 0; i < numInput; ++i) {
          auto value = input[i];
          super::values_.addValue(
              scatterRows ? scatterRows[super::rowIndex_ + i]
                          : super::rowIndex_ + i,
              value);
        }
      }
      DCHECK_EQ(input, values + numValues);
      if (scatter) {
        scatterDense(input, scatterRows + super::rowIndex_, numInput, values);
      }
      numValues = scatter ? scatterRows[super::rowIndex_ + numInput - 1] + 1
                          : numValues + numInput;
      super::rowIndex_ += numInput;
      return;
    }
    constexpr bool filterOnly =
        std::is_same<typename super::Extract, DropValues>::value;
    constexpr int32_t kWidth = V32::VSize;
    for (auto i = 0; i < numInput; i += kWidth) {
      auto indices = V32::load(input + i);
      V32::TV cache;
      if (i + kWidth > numInput) {
        cache = V32::maskGather32<1>(
            V32::setAll(0),
            V32::leadingMask(numInput - i),
            DictSuper::filterCache_ - 3,
            indices);
      } else {
        cache = V32::gather32<1>(DictSuper::filterCache_ - 3, indices);
      }
      auto unknowns = V32::compareResult((cache & (kUnknown << 24)) << 1);
      auto passed = V32::compareBitMask(V32::compareResult(cache));
      if (UNLIKELY(unknowns)) {
        uint16_t bits = V32::compareBitMask(unknowns);
        while (bits) {
          int index = bits::getAndClearLastSetBit(bits);
          int32_t value = input[i + index];
          bool result;
          if (value >= baseDictSize_) {
            result = applyFilter(
                super::filter_, valueInDictionary(value - baseDictSize_, true));
          } else {
            result =
                applyFilter(super::filter_, valueInDictionary(value, false));
          }
          if (result) {
            DictSuper::filterCache_[value] = FilterResult::kSuccess;
            passed |= 1 << index;
          } else {
            DictSuper::filterCache_[value] = FilterResult::kFailure;
          }
        }
      }
      if (!passed) {
        continue;
      } else if (passed == (1 << V32::VSize) - 1) {
        V32::store(
            filterHits + numValues,
            V32::load(
                (scatter ? scatterRows : super::rows_) + super::rowIndex_ + i));
        if (!filterOnly) {
          V32::store(values + numValues, indices);
        }
        numValues += kWidth;
      } else {
        int8_t numBits = __builtin_popcount(passed);
        auto setBits = V32::load(&V32::byteSetBits()[passed]);
        simd::storePermute(
            filterHits + numValues,
            V32::load(
                (scatter ? scatterRows : super::rows_) + super::rowIndex_ + i),
            setBits);
        if (!filterOnly) {
          simd::storePermute(values + numValues, indices, setBits);
        }
        numValues += numBits;
      }
    }
    super::rowIndex_ += numInput;
  }

  // Processes a run length run.
  // 'value' is the value for 'currentRow' and numRows is the number of
  // selected rows that fall in this RLE. If value is 10 and delta is 3
  // and rows is {20, 30}, then this processes a 25 at 20 and a 40 at
  // 30.
  template <bool hasFilter, bool hasHook, bool scatter>
  void processRle(
      int32_t value,
      int32_t delta,
      int32_t numRows,
      int32_t currentRow,
      const int32_t* scatterRows,
      int32_t* filterHits,
      int32_t* values,
      int32_t& numValues) {
    constexpr int32_t kWidth = V32::VSize;
    for (auto i = 0; i < numRows; i += kWidth) {
      V32::store(
          values + numValues + i,
          (V32::load(super::rows_ + super::rowIndex_ + i) - currentRow) *
                  delta +
              value);
    }

    processRun<hasFilter, hasHook, scatter>(
        values + numValues,
        numRows,
        scatterRows,
        filterHits,
        values,
        numValues);
  }

 private:
  void setByInDict(int32_t* values, int numValues) {
    if (DictSuper::inDict_) {
      auto current = super::rowIndex_;
      int32_t i = 0;
      for (; i < numValues; ++i) {
        if (!bits::isBitSet(DictSuper::inDict_, super::rows_[i + current])) {
          values[i] += baseDictSize_;
        }
      }
    }
  }

  folly::StringPiece valueInDictionary(int64_t index, bool inStrideDict) {
    if (inStrideDict) {
      auto start = strideDictOffset_[index];
      return folly::StringPiece(
          strideDictBlob_ + start, strideDictOffset_[index + 1] - start);
    }
    auto start = dictOffset_[index];
    return folly::StringPiece(
        dictBlob_ + start, dictOffset_[index + 1] - start);
  }

  const char* dictBlob_;
  const uint64_t* dictOffset_;
  const vector_size_t baseDictSize_;
  const char* const strideDictBlob_;
  const uint64_t* const strideDictOffset_;
};

class ExtractStringDictionaryToGenericHook {
 public:
  static constexpr bool kSkipNulls = true;
  using HookType = ValueHook;

  ExtractStringDictionaryToGenericHook(
      ValueHook* hook,
      RowSet rows,
      const uint64_t* inDict,
      const char* dictionaryBlob,
      const uint64_t* dictionaryOffset,
      int32_t dictionaryCount,
      const char* strideDictBlob,
      const uint64_t* strideDictOffset)
      : hook_(hook),
        rows_(rows),
        inDict_(inDict),
        dictBlob_(dictionaryBlob),
        dictOffset_(dictionaryOffset),
        baseDictSize_(dictionaryCount),
        strideDictBlob_(strideDictBlob),
        strideDictOffset_(strideDictOffset) {}

  bool acceptsNulls() {
    return hook_->acceptsNulls();
  }

  void addNull(vector_size_t rowIndex) {
    hook_->addNull(rowIndex);
  }

  void addValue(vector_size_t rowIndex, int32_t value) {
    if (!inDict_ || bits::isBitSet(inDict_, rows_[rowIndex])) {
      folly::StringPiece view(
          dictBlob_ + dictOffset_[value],
          dictOffset_[value + 1] - dictOffset_[value]);
      hook_->addValue(rowIndex, &view);
    } else {
      auto index = value - baseDictSize_;
      folly::StringPiece view(
          strideDictBlob_ + strideDictOffset_[index],
          strideDictOffset_[index + 1] - strideDictOffset_[index]);
      hook_->addValue(rowIndex, &view);
    }
  }

  ValueHook& hook() {
    return *hook_;
  }

 private:
  ValueHook* const hook_;
  RowSet const rows_;
  const uint64_t* const inDict_;
  const char* const dictBlob_;
  const uint64_t* const dictOffset_;
  const vector_size_t baseDictSize_;
  const char* const strideDictBlob_;
  const uint64_t* const strideDictOffset_;
};

template <typename TVisitor>
void SelectiveStringDictionaryColumnReader::readWithVisitor(
    RowSet rows,
    TVisitor visitor) {
  vector_size_t numRows = rows.back() + 1;
  auto decoder = dynamic_cast<RleDecoderV1<false>*>(dictIndex_.get());
  VELOX_CHECK(decoder, "Only RLEv1 is supported");
  if (nullsInReadRange_) {
    decoder->readWithVisitor<true, TVisitor>(
        nullsInReadRange_->as<uint64_t>(), visitor);
  } else {
    decoder->readWithVisitor<false, TVisitor>(nullptr, visitor);
  }
  readOffset_ += numRows;
}

template <typename TFilter, bool isDense, typename ExtractValues>
void SelectiveStringDictionaryColumnReader::readHelper(
    common::Filter* filter,
    RowSet rows,
    ExtractValues values) {
  readWithVisitor(
      rows,
      StringDictionaryColumnVisitor<TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter),
          this,
          rows,
          values,
          (strideDict_ && inDict_) ? inDict_->as<uint64_t>() : nullptr,
          filterCache_.empty() ? nullptr : filterCache_.data(),
          dictionaryBlob_->as<char>(),
          dictionaryOffset_->as<uint64_t>(),
          dictionaryCount_,
          strideDict_ ? strideDict_->as<char>() : nullptr,
          strideDictOffset_ ? strideDictOffset_->as<uint64_t>() : nullptr));
}

template <bool isDense, typename ExtractValues>
void SelectiveStringDictionaryColumnReader::processFilter(
    common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  switch (filter ? filter->kind() : FilterKind::kAlwaysTrue) {
    case common::FilterKind::kAlwaysTrue:
      readHelper<common::AlwaysTrue, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kIsNull:
      filterNulls<int32_t>(
          rows,
          true,
          !std::is_same<decltype(extractValues), DropValues>::value);
      break;
    case common::FilterKind::kIsNotNull:
      if (std::is_same<decltype(extractValues), DropValues>::value) {
        filterNulls<int32_t>(rows, false, false);
      } else {
        readHelper<common::IsNotNull, isDense>(filter, rows, extractValues);
      }
      break;
    case common::FilterKind::kBytesRange:
      readHelper<common::BytesRange, isDense>(filter, rows, extractValues);
      break;
    case common::FilterKind::kBytesValues:
      readHelper<common::BytesValues, isDense>(filter, rows, extractValues);
      break;
    default:
      readHelper<common::Filter, isDense>(filter, rows, extractValues);
      break;
  }
}

void SelectiveStringDictionaryColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  static std::array<char, 1> EMPTY_DICT;

  prepareRead<int32_t>(offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
  const auto* nullsPtr =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  vector_size_t numRows = rows.back() + 1;
  // lazy loading dictionary data when first hit
  ensureInitialized();

  if (inDictionaryReader_) {
    auto end = rows.back() + 1;
    bool isBulk = useBulkPath();
    int32_t numFlags = (isBulk && nullsInReadRange_)
        ? bits::countNonNulls(nullsInReadRange_->as<uint64_t>(), 0, end)
        : end;
    ensureCapacity<uint64_t>(inDict_, bits::nwords(numFlags), &memoryPool);
    inDictionaryReader_->next(
        inDict_->asMutable<char>(), numFlags, isBulk ? nullptr : nullsPtr);
    loadStrideDictionary();
    if (strideDict_) {
      DWIO_ENSURE_NOT_NULL(strideDictOffset_);

      // It's possible strideDictBlob is nullptr when stride dictionary only
      // contains empty string. In that case, we need to make sure
      // strideDictBlob points to some valid address, and the last entry of
      // strideDictOffset_ have value 0.
      auto strideDictBlob = strideDict_->as<char>();
      if (!strideDictBlob) {
        strideDictBlob = EMPTY_DICT.data();
        DWIO_ENSURE_EQ(strideDictOffset_->as<int64_t>()[strideDictCount_], 0);
      }
    }
  }
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        readHelper<common::AlwaysTrue, true>(
            &Filters::alwaysTrue,
            rows,
            ExtractStringDictionaryToGenericHook(
                scanSpec_->valueHook(),
                rows,
                (strideDict_ && inDict_) ? inDict_->as<uint64_t>() : nullptr,
                dictionaryBlob_->as<char>(),
                dictionaryOffset_->as<uint64_t>(),
                dictionaryCount_,
                strideDict_ ? strideDict_->as<char>() : nullptr,
                strideDictOffset_ ? strideDictOffset_->as<uint64_t>()
                                  : nullptr));
      } else {
        readHelper<common::AlwaysTrue, false>(
            &Filters::alwaysTrue,
            rows,
            ExtractStringDictionaryToGenericHook(
                scanSpec_->valueHook(),
                rows,
                (strideDict_ && inDict_) ? inDict_->as<uint64_t>() : nullptr,
                dictionaryBlob_->as<char>(),
                dictionaryOffset_->as<uint64_t>(),
                dictionaryCount_,
                strideDict_ ? strideDict_->as<char>() : nullptr,
                strideDictOffset_ ? strideDictOffset_->as<uint64_t>()
                                  : nullptr));
      }
      return;
    }
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, ExtractToReader(this));
    } else {
      processFilter<false>(scanSpec_->filter(), rows, ExtractToReader(this));
    }
  } else {
    if (isDense) {
      processFilter<true>(scanSpec_->filter(), rows, DropValues());
    } else {
      processFilter<false>(scanSpec_->filter(), rows, DropValues());
    }
  }
}

void SelectiveStringDictionaryColumnReader::getValues(
    RowSet rows,
    VectorPtr* result) {
  if (!dictionaryValues_) {
    makeDictionaryBaseVector();
  }
  compactScalarValues<int32_t, int32_t>(rows, false);

  *result = std::make_shared<DictionaryVector<StringView>>(
      &memoryPool,
      !anyNulls_               ? nullptr
          : returnReaderNulls_ ? nullsInReadRange_
                               : resultNulls_,
      numValues_,
      dictionaryValues_,
      TypeKind::INTEGER,
      values_);

  if (scanSpec_->makeFlat()) {
    BaseVector::ensureWritable(
        SelectivityVector::empty(), (*result)->type(), &memoryPool, result);
  }
}

void SelectiveStringDictionaryColumnReader::ensureInitialized() {
  if (LIKELY(initialized_)) {
    return;
  }

  Timer timer;

  ensureCapacity<int64_t>(dictionaryOffset_, dictionaryCount_ + 1, &memoryPool);
  dictionaryBlob_ = loadDictionary(
      dictionaryCount_, *blobStream_, *lengthDecoder_, dictionaryOffset_);
  dictionaryValues_.reset();
  filterCache_.resize(dictionaryCount_);
  simd::memset(filterCache_.data(), FilterResult::kUnknown, dictionaryCount_);

  // handle in dictionary stream
  if (inDictionaryReader_) {
    ensureRowGroupIndex();
    // load stride dictionary offsets
    positionOffset_ =
        notNullDecoder ? notNullDecoder->loadIndices(*index_, 0) : 0;
    size_t offset = strideDictStream_->loadIndices(*index_, positionOffset_);
    strideDictSizeOffset_ =
        strideDictLengthDecoder_->loadIndices(*index_, offset);
  }
  initialized_ = true;
  initTimeClocks_ = timer.elapsedClocks();
}

class SelectiveStructColumnReader : public SelectiveColumnReader {
 public:
  SelectiveStructColumnReader(
      const EncodingKey& ek,
      const std::shared_ptr<const TypeWithId>& requestedType,
      const std::shared_ptr<const TypeWithId>& dataType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec);

  void seekToRowGroup(uint32_t index) override {
    for (auto& child : children_) {
      child->seekToRowGroup(index);
      child->setReadOffset(index * rowsPerRowGroup_);
    }

    setReadOffset(index * rowsPerRowGroup_);
  }

  uint64_t skip(uint64_t numValues) override;

  void next(
      uint64_t numValues,
      VectorPtr& result,
      const uint64_t* incomingNulls) override;

  std::vector<uint32_t> filterRowGroups(
      uint64_t rowGroupSize,
      const StatsContext& context) const override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

  uint64_t numReads() const {
    return numReads_;
  }

  vector_size_t lazyVectorReadOffset() const {
    return lazyVectorReadOffset_;
  }

  /// Advance field reader to the row group closest to specified offset by
  /// calling seekToRowGroup.
  void advanceFieldReader(SelectiveColumnReader* reader, vector_size_t offset) {
    auto rowGroup = reader->readOffset() / rowsPerRowGroup_;
    auto nextRowGroup = offset / rowsPerRowGroup_;
    if (nextRowGroup > rowGroup) {
      reader->seekToRowGroup(nextRowGroup);
      reader->setReadOffset(nextRowGroup * rowsPerRowGroup_);
    }
  }

  // Returns the nulls bitmap from reading this. Used in LazyVector loaders.
  const uint64_t* nulls() const {
    return nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  }

 private:
  std::vector<std::unique_ptr<SelectiveColumnReader>> children_;
  // Sequence number of output batch. Checked against ColumnLoaders
  // created by 'this' to verify they are still valid at load.
  uint64_t numReads_ = 0;
  vector_size_t lazyVectorReadOffset_;

  // Dense set of rows to read in next().
  raw_vector<vector_size_t> rows_;
};

SelectiveStructColumnReader::SelectiveStructColumnReader(
    const EncodingKey& ek,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec)
    : SelectiveColumnReader(ek, stripe, scanSpec, dataType->type) {
  DWIO_ENSURE_EQ(ek.node, dataType->id, "working on the same node");
  auto encoding = static_cast<int64_t>(stripe.getEncoding(encodingKey).kind());
  DWIO_ENSURE_EQ(
      encoding,
      proto::ColumnEncoding_Kind_DIRECT,
      "Unknown encoding for StructColumnReader");

  const auto& cs = stripe.getColumnSelector();
  auto& childSpecs = scanSpec->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto childSpec = childSpecs[i].get();
    if (childSpec->isConstant()) {
      continue;
    }
    auto childDataType = dataType->childByName(childSpec->fieldName());
    auto childRequestedType =
        requestedType->childByName(childSpec->fieldName());
    VELOX_CHECK(cs.shouldReadNode(childDataType->id));
    children_.push_back(SelectiveColumnReader::build(
        childRequestedType, childDataType, stripe, childSpec, ek.sequence));
    childSpec->setSubscript(children_.size() - 1);
  }
}

std::vector<uint32_t> SelectiveStructColumnReader::filterRowGroups(
    uint64_t rowGroupSize,
    const StatsContext& context) const {
  auto stridesToSkip =
      SelectiveColumnReader::filterRowGroups(rowGroupSize, context);
  for (const auto& child : children_) {
    auto childStridesToSkip = child->filterRowGroups(rowGroupSize, context);
    if (stridesToSkip.empty()) {
      stridesToSkip = std::move(childStridesToSkip);
    } else {
      std::vector<uint32_t> merged;
      merged.reserve(childStridesToSkip.size() + stridesToSkip.size());
      std::merge(
          childStridesToSkip.begin(),
          childStridesToSkip.end(),
          stridesToSkip.begin(),
          stridesToSkip.end(),
          std::back_inserter(merged));
      stridesToSkip = std::move(merged);
    }
  }
  return stridesToSkip;
}

uint64_t SelectiveStructColumnReader::skip(uint64_t numValues) {
  auto numNonNulls = ColumnReader::skip(numValues);
  // 'readOffset_' of struct child readers is aligned with
  // 'readOffset_' of the struct. The child readers may have fewer
  // values since there is no value in children where the struct is
  // null. But because struct nulls are injected as nulls in child
  // readers, it is practical to keep the row numbers in terms of the
  // enclosing struct.
  for (auto& child : children_) {
    if (child) {
      child->skip(numNonNulls);
      child->setReadOffset(child->readOffset() + numValues);
    }
  }
  return numValues;
}

void SelectiveStructColumnReader::next(
    uint64_t numValues,
    VectorPtr& result,
    const uint64_t* incomingNulls) {
  VELOX_CHECK(!incomingNulls, "next may only be called for the root reader.");
  if (children_.empty()) {
    // no readers
    // This can be either count(*) query or a query that select only
    // constant columns (partition keys or columns missing from an old file
    // due to schema evolution)
    result->resize(numValues);

    auto resultRowVector = std::dynamic_pointer_cast<RowVector>(result);
    auto& childSpecs = scanSpec_->children();
    for (auto& childSpec : childSpecs) {
      VELOX_CHECK(childSpec->isConstant());
      auto channel = childSpec->channel();
      resultRowVector->childAt(channel) = childSpec->constantValue();
    }
    return;
  }
  auto oldSize = rows_.size();
  rows_.resize(numValues);
  if (numValues > oldSize) {
    std::iota(&rows_[oldSize], &rows_[rows_.size()], oldSize);
  }
  read(readOffset_, rows_, nullptr);
  getValues(outputRows(), &result);
}

class ColumnLoader : public velox::VectorLoader {
 public:
  ColumnLoader(
      SelectiveStructColumnReader* structReader,
      SelectiveColumnReader* fieldReader,
      uint64_t version)
      : structReader_(structReader),
        fieldReader_(fieldReader),
        version_(version) {}

  void load(RowSet rows, ValueHook* hook, VectorPtr* result) override;

 private:
  SelectiveStructColumnReader* structReader_;
  SelectiveColumnReader* fieldReader_;
  // This is checked against the version of 'structReader' on load. If
  // these differ, 'structReader' has been advanced since the creation
  // of 'this' and 'this' is no longer loadable.
  const uint64_t version_;
};

// Wraps '*result' in a dictionary to make the contiguous values
// appear at the indices i 'rows'. Used when loading a LazyVector for
// a sparse set of rows in conditional exprs.
static void scatter(RowSet rows, VectorPtr* result) {
  auto end = rows.back() + 1;
  // Initialize the indices to 0 to make the dictionary safely
  // readable also for uninitialized positions.
  auto indices =
      AlignedBuffer::allocate<vector_size_t>(end, (*result)->pool(), 0);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (int32_t i = 0; i < rows.size(); ++i) {
    rawIndices[rows[i]] = i;
  }
  *result =
      BaseVector::wrapInDictionary(BufferPtr(nullptr), indices, end, *result);
}

void ColumnLoader::load(RowSet rows, ValueHook* hook, VectorPtr* result) {
  VELOX_CHECK(
      version_ == structReader_->numReads(),
      "Loading LazyVector after the enclosing reader has moved");
  auto offset = structReader_->lazyVectorReadOffset();
  auto incomingNulls = structReader_->nulls();
  auto outputRows = structReader_->outputRows();
  raw_vector<vector_size_t> selectedRows;
  RowSet effectiveRows;
  if (rows.size() == outputRows.size()) {
    // All the rows planned at creation are accessed.
    effectiveRows = outputRows;
  } else {
    // rows is a set of indices into outputRows. There has been a
    // selection between creation and loading.
    selectedRows.resize(rows.size());
    assert(!selectedRows.empty());
    for (auto i = 0; i < rows.size(); ++i) {
      selectedRows[i] = outputRows[rows[i]];
    }
    effectiveRows = RowSet(selectedRows);
  }

  structReader_->advanceFieldReader(fieldReader_, offset);
  fieldReader_->scanSpec()->setValueHook(hook);
  fieldReader_->read(offset, effectiveRows, incomingNulls);
  if (!hook) {
    fieldReader_->getValues(effectiveRows, result);
    if (rows.size() != outputRows.size()) {
      // We read sparsely. The values that were read should appear
      // at the indices in the result vector that were given by
      // 'rows'.
      scatter(rows, result);
    }
  }
}

void SelectiveStructColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  numReads_ = scanSpec_->newRead();
  prepareRead<char>(offset, rows, incomingNulls);
  RowSet activeRows = rows;
  auto& childSpecs = scanSpec_->children();
  const uint64_t* structNulls =
      nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  bool hasFilter = false;
  assert(!children_.empty());
  for (size_t i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (childSpec->isConstant()) {
      continue;
    }
    if (childSpec->projectOut() && !childSpec->filter() &&
        !childSpec->extractValues()) {
      // Will make a LazyVector.
      continue;
    }
    auto fieldIndex = childSpec->subscript();
    auto reader = children_[fieldIndex].get();
    advanceFieldReader(reader, offset);
    if (childSpec->filter()) {
      hasFilter = true;
      {
        SelectivityTimer timer(childSpec->selectivity(), activeRows.size());

        reader->resetInitTimeClocks();
        reader->read(offset, activeRows, structNulls);

        // Exclude initialization time.
        timer.subtract(reader->initTimeClocks());

        activeRows = reader->outputRows();
        childSpec->selectivity().addOutput(activeRows.size());
      }
      if (activeRows.empty()) {
        break;
      }
    } else {
      reader->read(offset, activeRows, structNulls);
    }
  }
  if (hasFilter) {
    setOutputRows(activeRows);
  }
  lazyVectorReadOffset_ = offset;
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveStructColumnReader::getValues(RowSet rows, VectorPtr* result) {
  assert(!children_.empty());
  VELOX_CHECK(
      *result != nullptr,
      "SelectiveStructColumnReader expects a non-null result");
  RowVector* resultRow = dynamic_cast<RowVector*>(result->get());
  VELOX_CHECK(resultRow, "Struct reader expects a result of type ROW.");
  resultRow->resize(rows.size());
  if (!rows.size()) {
    return;
  }
  if (nullsInReadRange_) {
    auto readerNulls = nullsInReadRange_->as<uint64_t>();
    auto nulls = resultRow->mutableNulls(rows.size())->asMutable<uint64_t>();
    for (size_t i = 0; i < rows.size(); ++i) {
      bits::setBit(nulls, i, bits::isBitSet(readerNulls, rows[i]));
    }
  } else {
    resultRow->clearNulls(0, rows.size());
  }
  bool lazyPrepared = false;
  auto& childSpecs = scanSpec_->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto& childSpec = childSpecs[i];
    if (!childSpec->projectOut()) {
      continue;
    }
    auto index = childSpec->subscript();
    auto channel = childSpec->channel();
    if (childSpec->isConstant()) {
      resultRow->childAt(channel) = childSpec->constantValue();
    } else {
      if (!childSpec->extractValues() && !childSpec->filter()) {
        // LazyVector result.
        if (!lazyPrepared) {
          if (rows.size() != outputRows_.size()) {
            setOutputRows(rows);
          }
          lazyPrepared = true;
        }
        resultRow->childAt(channel) = std::make_shared<LazyVector>(
            &memoryPool,
            resultRow->type()->childAt(channel),
            rows.size(),
            std::make_unique<ColumnLoader>(
                this, children_[index].get(), numReads_));
      } else {
        children_[index]->getValues(rows, &resultRow->childAt(channel));
      }
    }
  }
}

// Abstract superclass for list and map readers. Encapsulates common
// logic for dealing with mapping between enclosing and nested rows.
class SelectiveRepeatedColumnReader : public SelectiveColumnReader {
 public:
  bool useBulkPath() const override {
    return false;
  }

 protected:
  // Buffer size for reading length stream
  static constexpr size_t kBufferSize = 1024;

  SelectiveRepeatedColumnReader(
      const EncodingKey& ek,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec,
      const TypePtr& type)
      : SelectiveColumnReader(ek, stripe, scanSpec, type, true) {
    auto rleVersion = convertRleVersion(stripe.getEncoding(encodingKey).kind());
    auto lenId = encodingKey.forKind(proto::Stream_Kind_LENGTH);
    bool lenVints = stripe.getUseVInts(lenId);
    length_ = IntDecoder</*isSigned*/ false>::createRle(
        stripe.getStream(lenId, true),
        rleVersion,
        memoryPool,
        lenVints,
        INT_BYTE_SIZE);
  }

  void makeNestedRowSet(RowSet rows) {
    allLengths_.resize(rows.back() + 1);
    assert(!allLengths_.empty()); // for lint only.
    auto nulls =
        nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
    // Reads the lengths, leaves an uninitialized gap for a null
    // map/list. Reading these checks the null nask.
    length_->next(allLengths_.data(), rows.back() + 1, nulls);
    ensureCapacity<vector_size_t>(offsets_, rows.size(), &memoryPool);
    ensureCapacity<vector_size_t>(sizes_, rows.size(), &memoryPool);
    auto rawOffsets = offsets_->asMutable<vector_size_t>();
    auto rawSizes = sizes_->asMutable<vector_size_t>();
    vector_size_t nestedLength = 0;
    for (auto row : rows) {
      if (!nulls || !bits::isBitNull(nulls, row)) {
        nestedLength += allLengths_[row];
      }
    }
    nestedRows_.resize(nestedLength);
    vector_size_t currentRow = 0;
    vector_size_t nestedRow = 0;
    vector_size_t nestedOffset = 0;
    for (auto rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
      auto row = rows[rowIndex];
      // Add up the lengths of non-null rows skipped since the last
      // non-null.
      for (auto i = currentRow; i < row; ++i) {
        if (!nulls || !bits::isBitNull(nulls, i)) {
          nestedOffset += allLengths_[i];
        }
      }
      currentRow = row + 1;
      // Check if parent is null after adding up the lengths leading
      // up to this. If null, add a null to the result and keep
      // looping. If the null is last, the lengths will all have been
      // added up.
      if (nulls && bits::isBitNull(nulls, row)) {
        rawOffsets[rowIndex] = 0;
        rawSizes[rowIndex] = 0;
        bits::setNull(rawResultNulls_, rowIndex);
        anyNulls_ = true;
        continue;
      }

      auto lengthAtRow = allLengths_[row];
      std::iota(
          &nestedRows_[nestedRow],
          &nestedRows_[nestedRow + lengthAtRow],
          nestedOffset);
      rawOffsets[rowIndex] = nestedRow;
      rawSizes[rowIndex] = lengthAtRow;
      nestedRow += lengthAtRow;
      nestedOffset += lengthAtRow;
    }
    childTargetReadOffset_ += nestedOffset;
  }

  void compactOffsets(RowSet rows) {
    auto rawOffsets = offsets_->asMutable<vector_size_t>();
    auto rawSizes = sizes_->asMutable<vector_size_t>();
    auto nulls =
        nullsInReadRange_ ? nullsInReadRange_->asMutable<uint64_t>() : nullptr;
    VELOX_CHECK(
        outputRows_.empty(), "Repeated reader does not support filters");
    RowSet rowsToCompact;
    if (valueRows_.empty()) {
      valueRows_.resize(rows.size());
      rowsToCompact = inputRows_;
    } else {
      rowsToCompact = valueRows_;
    }
    if (rows.size() == rowsToCompact.size()) {
      return;
    }

    int32_t current = 0;
    for (int i = 0; i < rows.size(); ++i) {
      auto row = rows[i];
      while (rowsToCompact[current] < row) {
        ++current;
      }
      VELOX_CHECK(rowsToCompact[current] == row);
      valueRows_[i] = row;
      rawOffsets[i] = rawOffsets[current];
      rawSizes[i] = rawSizes[current];
      if (nulls) {
        bits::setBit(
            rawResultNulls_, i, bits::isBitSet(rawResultNulls_, current));
      }
    }
    numValues_ = rows.size();
    valueRows_.resize(numValues_);
    offsets_->setSize(numValues_ * sizeof(vector_size_t));
    sizes_->setSize(numValues_ * sizeof(vector_size_t));
  }

  std::vector<int64_t> allLengths_;
  raw_vector<vector_size_t> nestedRows_;
  BufferPtr offsets_;
  BufferPtr sizes_;
  // The position in the child readers that corresponds to the
  // position in the length stream. The child readers can be behind if
  // the last parents were null, so that the child stream was only
  // read up to the last position corresponding to
  // the last non-null parent.
  vector_size_t childTargetReadOffset_ = 0;
  std::unique_ptr<IntDecoder</*isSigned*/ false>> length_;
};

class SelectiveListColumnReader : public SelectiveRepeatedColumnReader {
 public:
  SelectiveListColumnReader(
      const EncodingKey& ek,
      const std::shared_ptr<const TypeWithId>& requestedType,
      const std::shared_ptr<const TypeWithId>& dataType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec);

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder) {
      notNullDecoder->seekToRowGroup(positionsProvider);
    }

    length_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());

    child_->seekToRowGroup(index);
    child_->setReadOffset(0);
    childTargetReadOffset_ = 0;
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

 private:
  std::unique_ptr<SelectiveColumnReader> child_;
  const TypePtr requestedType_;
};

SelectiveListColumnReader::SelectiveListColumnReader(
    const EncodingKey& ek,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec)
    : SelectiveRepeatedColumnReader(ek, stripe, scanSpec, dataType->type),
      requestedType_(requestedType->type) {
  DWIO_ENSURE_EQ(ek.node, dataType->id, "working on the same node");
  // count the number of selected sub-columns
  const auto& cs = stripe.getColumnSelector();
  auto& childType = requestedType->childAt(0);
  VELOX_CHECK(
      cs.shouldReadNode(childType->id),
      "SelectiveListColumnReader must select the values stream");
  if (scanSpec_->children().empty()) {
    common::ScanSpec* childSpec =
        scanSpec->getOrCreateChild(common::Subfield("elements"));
    childSpec->setProjectOut(true);
    childSpec->setExtractValues(true);
  }
  child_ = SelectiveColumnReader::build(
      childType,
      dataType->childAt(0),
      stripe,
      scanSpec_->children()[0].get(),
      ek.sequence);
}

uint64_t SelectiveListColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  if (child_) {
    std::array<int64_t, kBufferSize> buffer;
    uint64_t childElements = 0;
    uint64_t lengthsRead = 0;
    while (lengthsRead < numValues) {
      uint64_t chunk =
          std::min(numValues - lengthsRead, static_cast<uint64_t>(kBufferSize));
      length_->next(buffer.data(), chunk, nullptr);
      for (size_t i = 0; i < chunk; ++i) {
        childElements += static_cast<size_t>(buffer[i]);
      }
      lengthsRead += chunk;
    }
    child_->skip(childElements);
    childTargetReadOffset_ += childElements;
    child_->setReadOffset(child_->readOffset() + childElements);
  } else {
    length_->skip(numValues);
  }
  return numValues;
}

void SelectiveListColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  // Catch up if the child is behind the length stream.
  child_->seekTo(childTargetReadOffset_, false);
  prepareRead<char>(offset, rows, incomingNulls);
  makeNestedRowSet(rows);
  if (child_ && !nestedRows_.empty()) {
    child_->read(child_->readOffset(), nestedRows_, nullptr);
  }
  numValues_ = rows.size();
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveListColumnReader::getValues(RowSet rows, VectorPtr* result) {
  compactOffsets(rows);
  VectorPtr elements;
  if (child_ && !nestedRows_.empty()) {
    child_->getValues(nestedRows_, &elements);
  }
  *result = std::make_shared<ArrayVector>(
      &memoryPool,
      requestedType_,
      anyNulls_ ? resultNulls_ : nullptr,
      rows.size(),
      offsets_,
      sizes_,
      elements);
}

class SelectiveMapColumnReader : public SelectiveRepeatedColumnReader {
 public:
  SelectiveMapColumnReader(
      const EncodingKey& ek,
      const std::shared_ptr<const TypeWithId>& requestedType,
      const std::shared_ptr<const TypeWithId>& dataType,
      StripeStreams& stripe,
      common::ScanSpec* scanSpec);

  void seekToRowGroup(uint32_t index) override {
    ensureRowGroupIndex();

    auto positions = toPositions(index_->entry(index));
    PositionProvider positionsProvider(positions);

    if (notNullDecoder) {
      notNullDecoder->seekToRowGroup(positionsProvider);
    }

    length_->seekToRowGroup(positionsProvider);

    VELOX_CHECK(!positionsProvider.hasNext());

    keyReader_->seekToRowGroup(index);
    keyReader_->setReadOffset(0);
    elementReader_->seekToRowGroup(index);
    elementReader_->setReadOffset(0);
    childTargetReadOffset_ = 0;
  }

  uint64_t skip(uint64_t numValues) override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

 private:
  std::unique_ptr<SelectiveColumnReader> keyReader_;
  std::unique_ptr<SelectiveColumnReader> elementReader_;
  const TypePtr requestedType_;
};

SelectiveMapColumnReader::SelectiveMapColumnReader(
    const EncodingKey& ek,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec)
    : SelectiveRepeatedColumnReader(ek, stripe, scanSpec, dataType->type),
      requestedType_(requestedType->type) {
  DWIO_ENSURE_EQ(ek.node, dataType->id, "working on the same node");
  if (scanSpec_->children().empty()) {
    common::ScanSpec* keySpec =
        scanSpec->getOrCreateChild(common::Subfield("keys"));
    keySpec->setProjectOut(true);
    keySpec->setExtractValues(true);
    common::ScanSpec* valueSpec =
        scanSpec->getOrCreateChild(common::Subfield("elements"));
    valueSpec->setProjectOut(true);
    valueSpec->setExtractValues(true);
  }
  const auto& cs = stripe.getColumnSelector();
  auto& keyType = requestedType->childAt(0);
  VELOX_CHECK(
      cs.shouldReadNode(keyType->id),
      "Map key must be selected in SelectiveMapColumnReader");
  keyReader_ = SelectiveColumnReader::build(
      keyType,
      dataType->childAt(0),
      stripe,
      scanSpec_->children()[0].get(),
      ek.sequence);

  auto& valueType = requestedType->childAt(1);
  VELOX_CHECK(
      cs.shouldReadNode(valueType->id),
      "Map Values must be selected in SelectiveMapColumnReader");
  elementReader_ = SelectiveColumnReader::build(
      valueType,
      dataType->childAt(1),
      stripe,
      scanSpec_->children()[0].get(),
      ek.sequence);

  VLOG(1) << "[Map] Initialized map column reader for node " << dataType->id;
}

uint64_t SelectiveMapColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  if (keyReader_ || elementReader_) {
    std::array<int64_t, kBufferSize> buffer;
    uint64_t childElements = 0;
    uint64_t lengthsRead = 0;
    while (lengthsRead < numValues) {
      uint64_t chunk =
          std::min(numValues - lengthsRead, static_cast<uint64_t>(kBufferSize));
      length_->next(buffer.data(), chunk, nullptr);
      for (size_t i = 0; i < chunk; ++i) {
        childElements += buffer[i];
      }
      lengthsRead += chunk;
    }
    if (keyReader_) {
      keyReader_->skip(childElements);
      keyReader_->setReadOffset(keyReader_->readOffset() + childElements);
    }
    if (elementReader_) {
      elementReader_->skip(childElements);
      elementReader_->setReadOffset(
          elementReader_->readOffset() + childElements);
    }
    childTargetReadOffset_ += childElements;

  } else {
    length_->skip(numValues);
  }
  return numValues;
}

void SelectiveMapColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  // Catch up if child readers are behind the length stream.
  if (keyReader_) {
    keyReader_->seekTo(childTargetReadOffset_, false);
  }
  if (elementReader_) {
    elementReader_->seekTo(childTargetReadOffset_, false);
  }

  prepareRead<char>(offset, rows, incomingNulls);
  makeNestedRowSet(rows);
  if (keyReader_ && elementReader_ && !nestedRows_.empty()) {
    keyReader_->read(keyReader_->readOffset(), nestedRows_, nullptr);
    elementReader_->read(elementReader_->readOffset(), nestedRows_, nullptr);
  }
  numValues_ = rows.size();
  readOffset_ = offset + rows.back() + 1;
}

void SelectiveMapColumnReader::getValues(RowSet rows, VectorPtr* result) {
  compactOffsets(rows);
  VectorPtr keys;
  VectorPtr values;
  VELOX_CHECK(
      keyReader_ && elementReader_,
      "keyReader_ and elementReaer_ must exist in "
      "SelectiveMapColumnReader::getValues");
  if (!nestedRows_.empty()) {
    keyReader_->getValues(nestedRows_, &keys);
    elementReader_->getValues(nestedRows_, &values);
  }
  *result = std::make_shared<MapVector>(
      &memoryPool,
      requestedType_,
      anyNulls_ ? resultNulls_ : nullptr,
      rows.size(),
      offsets_,
      sizes_,
      keys,
      values);
}

} // namespace

std::unique_ptr<SelectiveColumnReader> buildIntegerReader(
    const EncodingKey& ek,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    uint32_t numBytes,
    common::ScanSpec* scanSpec) {
  switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return std::make_unique<SelectiveIntegerDictionaryColumnReader>(
          ek, requestedType, dataType, stripe, scanSpec, numBytes);
    case proto::ColumnEncoding_Kind_DIRECT:
      return std::make_unique<SelectiveIntegerDirectColumnReader>(
          ek, requestedType, dataType, stripe, numBytes, scanSpec);
    default:
      DWIO_RAISE("buildReader unhandled integer encoding");
  }
}

std::unique_ptr<SelectiveColumnReader> SelectiveColumnReader::build(
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& dataType,
    StripeStreams& stripe,
    common::ScanSpec* scanSpec,
    uint32_t sequence) {
  CompatChecker::check(*dataType->type, *requestedType->type);
  EncodingKey ek(dataType->id, sequence);

  switch (dataType->type->kind()) {
    case TypeKind::INTEGER:
      return buildIntegerReader(
          ek, requestedType, dataType, stripe, INT_BYTE_SIZE, scanSpec);
    case TypeKind::BIGINT:
      return buildIntegerReader(
          ek, requestedType, dataType, stripe, LONG_BYTE_SIZE, scanSpec);
    case TypeKind::SMALLINT:
      return buildIntegerReader(
          ek, requestedType, dataType, stripe, SHORT_BYTE_SIZE, scanSpec);
    case TypeKind::ARRAY:
      return std::make_unique<SelectiveListColumnReader>(
          ek, requestedType, dataType, stripe, scanSpec);
    case TypeKind::MAP:
      if (stripe.getEncoding(ek).kind() ==
          proto::ColumnEncoding_Kind_MAP_FLAT) {
        VELOX_CHECK(false, "SelectiveColumnReader does not support flat maps");
      }
      return std::make_unique<SelectiveMapColumnReader>(
          ek, requestedType, dataType, stripe, scanSpec);
    case TypeKind::REAL:
      if (requestedType->type->kind() == TypeKind::REAL) {
        return std::make_unique<
            SelectiveFloatingPointColumnReader<float, float>>(
            ek, stripe, scanSpec);
      } else {
        return std::make_unique<
            SelectiveFloatingPointColumnReader<float, double>>(
            ek, stripe, scanSpec);
      }
    case TypeKind::DOUBLE:
      return std::make_unique<
          SelectiveFloatingPointColumnReader<double, double>>(
          ek, stripe, scanSpec);
    case TypeKind::ROW:
      return std::make_unique<SelectiveStructColumnReader>(
          ek, requestedType, dataType, stripe, scanSpec);
    case TypeKind::BOOLEAN:
      return std::make_unique<SelectiveByteRleColumnReader>(
          ek, requestedType, dataType, stripe, scanSpec, true);
    case TypeKind::TINYINT:
      return std::make_unique<SelectiveByteRleColumnReader>(
          ek, requestedType, dataType, stripe, scanSpec, false);
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      switch (static_cast<int64_t>(stripe.getEncoding(ek).kind())) {
        case proto::ColumnEncoding_Kind_DIRECT:
          return std::make_unique<SelectiveStringDirectColumnReader>(
              ek, dataType, stripe, scanSpec);
        case proto::ColumnEncoding_Kind_DICTIONARY:
          return std::make_unique<SelectiveStringDictionaryColumnReader>(
              ek, dataType, stripe, scanSpec);
        default:
          DWIO_RAISE("buildReader string unknown encoding");
      }
    default:
      DWIO_RAISE(
          "buildReader unhandled type: " +
          mapTypeKindToName(dataType->type->kind()));
  }
}

} // namespace facebook::velox::dwrf
