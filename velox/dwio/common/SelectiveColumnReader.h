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
#include "velox/common/base/RawVector.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/process/ProcessBase.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/FormatData.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/type/Filter.h"

namespace facebook::velox::dwio::common {

// Generalized representation of a set of distinct values for dictionary
// encodings.
struct DictionaryValues {
  // Array of values for dictionary. StringViews for string values.
  BufferPtr values;

  // For a string dictionary, holds the characters that are pointed to by
  // StringViews in 'values'.
  BufferPtr strings;

  // Number of valid elements in 'values'.
  int32_t numValues{0};

  // True if values are in ascending order.
  bool sorted{false};

  void clear() {
    values = nullptr;
    strings = nullptr;
    numValues = 0;
    sorted = false;
  }
};

struct RawDictionaryState {
  const void* values{nullptr};
  int32_t numValues{0};
};

// Dictionary and other scan state. Trivially copiable, to be passed by value
// when constructing a visitor.
struct RawScanState {
  RawDictionaryState dictionary;

  // See comment in  ScanState below.
  RawDictionaryState dictionary2;
  const uint64_t* __restrict inDictionary{nullptr};
  uint8_t* __restrict filterCache;
};

// Maintains state for encoding between calls to readWithVisitor of
// individual readers. DWRF sets up encoding information at the
// start of a stripe and dictionaries at the start of stripes and
// optionally row groups. Other encodings can set dictionaries and
// encoding types at any time during processing a stripe.
//
//  This is the union of the state elements that the supported
//  encodings require for keeping state. This may be augmented when
//  adding formats. This is however inlined in the reader superclass
//  ad not for example nodeled as a class hierarchy with virtual
//  functions because this needs to be trivially and branchlessly
//  accessible.
struct ScanState {
  // Copies the owned values of 'this' into 'rawState'.
  void updateRawState();

  void clear() {
    dictionary.clear();
    dictionary2.clear();
    inDictionary = nullptr;
    updateRawState();
  }

  // Dictionary values when there s a dictionary in scope for decoding.
  DictionaryValues dictionary;

  // If the format, like ORC/DWRF has a base dictionary completed by
  // local delta dictionaries over the furst one, this represents the
  // local values, e.g. row group dictionary in ORC. TBD: If there is
  // a pattern of dictionaries completed by more dictionaries in other
  // formats, this will be modeled as an vector of n DictionaryValues.
  DictionaryValues dictionary2;

  // Bits selecting between dictionary and dictionary2 or dictionary and
  // literal. OR/DWRFC only.
  BufferPtr inDictionary;

  // Copy of Visitor::rows_ adjusted to start at the current encoding
  // run (e.g. Parquet Page). 0 means the first value of the current
  // encoding entry.
  raw_vector<vector_size_t> rowsCopy;

  // Cached results of filter application subscripted by dictionary
  // index. The visitor needs to reset this if the dictionary changes
  // in mid scan.
  raw_vector<uint8_t> filterCache;

  // The above as raw pointers.
  RawScanState rawState;
};

class SelectiveColumnReader {
 public:
  static constexpr uint64_t kStringBufferSize = 16 * 1024;

  SelectiveColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> requestedType,
      dwio::common::FormatParams& params,
      velox::common::ScanSpec& scanSpec,
      const TypePtr& type);

  virtual ~SelectiveColumnReader() = default;

  dwio::common::FormatData& formatData() const {
    return *formatData_;
  }

  /**
   * Read the next group of values into a RowVector.
   * @param numValues the number of values to read
   * @param vector to read into
   */
  virtual void next(
      uint64_t /*numValues*/,
      VectorPtr& /*result*/,
      const uint64_t* /*incomingNulls*/ = nullptr) {
    VELOX_UNSUPPORTED("next() is only defined in SelectiveStructColumnReader");
  }

  // Called when filters in ScanSpec change, e.g. a new filter is pushed down
  // from a downstream operator.
  virtual void resetFilterCaches();

  // Seeks to offset and reads the rows in 'rows' and applies
  // filters and value processing as given by 'scanSpec supplied at
  // construction. 'offset' is relative to start of stripe. 'rows' are
  // relative to 'offset', so that row 0 is the 'offset'th row from
  // start of stripe. 'rows' is expected to stay constant
  // between this and the next call to read.
  virtual void
  read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls) = 0;

  virtual uint64_t skip(uint64_t numValues) {
    return formatData_->skip(numValues);
  }

  // Extracts the values at 'rows' into '*result'. May rewrite or
  // reallocate '*result'. 'rows' must be the same set or a subset of
  // 'rows' passed to the last 'read().
  virtual void getValues(RowSet rows, VectorPtr* result) = 0;

  // Returns the rows that were selected/visited by the last
  // read(). If 'this' has no filter, returns 'rows' passed to last
  // read().
  const RowSet outputRows() const {
    if (scanSpec_->hasFilter()) {
      return outputRows_;
    }
    return inputRows_;
  }

  // Advances to 'offset', so that the next item to be read is the
  // offset-th from the start of stripe.
  void seekTo(vector_size_t offset, bool readsNullsOnly);

  // Positions this at the start of 'index'th row group. Interpretation of
  // 'index' depends on format.
  virtual void seekToRowGroup(uint32_t index) = 0;

  const TypePtr& type() const {
    return type_;
  }

  // The below functions are called from ColumnVisitor to fill the result set.
  inline void addOutputRow(vector_size_t row) {
    outputRows_.push_back(row);
  }

  // Returns a pointer to output rows  with at least 'size' elements available.
  vector_size_t* mutableOutputRows(int32_t size) {
    numOutConfirmed_ = outputRows_.size();
    outputRows_.resize(numOutConfirmed_ + size);
    return outputRows_.data() + numOutConfirmed_;
  }

  template <typename T>
  T* mutableValues(int32_t size) {
    DCHECK(values_->capacity() >= (numValues_ + size) * sizeof(T));
    return reinterpret_cast<T*>(rawValues_) + numValues_;
  }

  // Returns a mutable pointer to start of result nulls
  // bitmap. Ensures that this has at least 'numValues_' + 'size'
  // capacity and is unique. If extending existing buffer, preserves
  // previous contents.
  uint64_t* mutableNulls(int32_t size) {
    if (!resultNulls_->unique()) {
      resultNulls_ = AlignedBuffer::allocate<bool>(
          numValues_ + size, &memoryPool_, bits::kNotNull);
      rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
    }
    if (resultNulls_->capacity() * 8 < numValues_ + size) {
      // If a single read() spans many encoding runs then result nulls may
      // occasionally need extending.
      AlignedBuffer::reallocate<bool>(
          &resultNulls_,
          numValues_ + size + simd::kPadding * 8,
          bits::kNotNull);
      rawResultNulls_ = resultNulls_->asMutable<uint64_t>();
    }
    return rawResultNulls_;
  }

  // True if this reads contiguous rows starting at 0 and may have
  // nulls. If so, the nulls decoded from the nulls in encoded data
  // can be returned directly in the vector in getValues().
  bool returnReaderNulls() const {
    return returnReaderNulls_;
  }

  void setNumValues(vector_size_t size) {
    numValues_ = size;
  }

  // The number of passing after filtering.
  int32_t numRows() const {
    return outputRows_.size();
  }

  // The number of values copied into the results.
  int32_t numValues() const {
    return numValues_;
  }
  void setNumRows(vector_size_t size) {
    outputRows_.resize(size);
  }

  // Sets the result nulls to be returned in getValues(). This is used for
  // combining nulls from multiple encoding runs. nullptr means no nulls.
  void setNulls(BufferPtr resultNulls);

  // Adds 'bias' to outputt rows between 'firstRow' and end. Used
  // whenn combining data from multiple encoding runs, where the
  // output rows are first in terms of position in the encoding entry.
  void offsetOutputRows(int32_t firstRow, int32_t bias) {
    for (auto i = firstRow; i < outputRows_.size(); ++i) {
      outputRows_[i] += bias;
    }
  }

  void setHasNulls() {
    anyNulls_ = true;
  }

  void setAllNull() {
    allNull_ = true;
  }

  void incrementNumValues(vector_size_t size) {
    numValues_ += size;
  }

  template <typename T>
  inline void addNull() {
    VELOX_DCHECK_NE(valueSize_, kNoValueSize);
    VELOX_DCHECK_LE(
        rawResultNulls_ && rawValues_ && (numValues_ + 1) * valueSize_,
        values_->capacity());

    anyNulls_ = true;
    bits::setNull(rawResultNulls_, numValues_);
    // Set the default value at the nominal width of the reader but
    // calculate the index based on the actual width of the
    // data. These may differ for integer and dictionary readers.
    auto valuesAsChar = reinterpret_cast<char*>(rawValues_);
    *reinterpret_cast<T*>(valuesAsChar + valueSize_ * numValues_) = T();
    numValues_++;
  }

  template <typename T>
  inline void addValue(const T value) {
    // @lint-ignore-every HOWTOEVEN ConstantArgumentPassByValue
    static_assert(
        std::is_pod<T>::value,
        "General case of addValue is only for primitive types");
    VELOX_DCHECK_LE(
        rawValues_ && (numValues_ + 1) * sizeof(T), values_->capacity());
    reinterpret_cast<T*>(rawValues_)[numValues_] = value;
    numValues_++;
  }

  void dropResults(vector_size_t count) {
    outputRows_.resize(outputRows_.size() - count);
    numValues_ -= count;
  }

  velox::common::ScanSpec* scanSpec() const {
    return scanSpec_;
  }

  auto readOffset() const {
    return readOffset_;
  }

  void setReadOffset(vector_size_t readOffset) {
    readOffset_ = readOffset;
  }

  virtual void setReadOffsetRecursive(int32_t readOffset) {
    setReadOffset(readOffset);
  }

  // Recursively sets 'isTopLevel_'. Recurses down non-nullable structs,
  // otherwise only sets 'isTopLevel_' of 'this'
  virtual void setIsTopLevel() {
    isTopLevel_ = true;
  }

  bool isTopLevel() const {
    return isTopLevel_;
  }

  uint64_t initTimeClocks() const {
    return initTimeClocks_;
  }

  void resetInitTimeClocks() {
    initTimeClocks_ = 0;
  }

  virtual bool stripeMatches(uint32_t stripeIndex) const;

  virtual std::vector<uint32_t> filterRowGroups(
      uint64_t rowGroupSize,
      const dwio::common::StatsContext& context) const;

  raw_vector<int32_t>& innerNonNullRows() {
    return innerNonNullRows_;
  }

  raw_vector<int32_t>& outerNonNullRows() {
    return outerNonNullRows_;
  }

  BufferPtr& nullsInReadRange() {
    return nullsInReadRange_;
  }

  // Returns true if no filters or deterministic filters/hooks that
  // discard nulls. This is used at read prepare time. useFastPath()
  // in DecoderUtil.h is used at read time and is expected to produce
  // the same result.
  virtual bool useBulkPath() const {
    auto filter = scanSpec_->filter();
    return hasBulkPath() && process::hasAvx2() &&
        (!filter ||
         (filter->isDeterministic() &&
          (!nullsInReadRange_ || !filter->testNull()))) &&
        (!scanSpec_->valueHook() || !nullsInReadRange_ ||
         !scanSpec_->valueHook()->acceptsNulls());
  }

  // true if 'this' has a fast path.
  virtual bool hasBulkPath() const {
    return true;
  }

  // Used by decoders to set encoding-related data to be kept between calls to
  // read().
  ScanState& scanState() {
    return scanState_;
  }

  static constexpr int8_t kNoValueSize = -1;
  static constexpr uint32_t kRowGroupNotSet = ~0;

  // True if we have an is null filter and optionally return column
  // values or we have an is not null filter and do not return column
  // values. This means that only null flags need be accessed.
  bool readsNullsOnly() const;

  template <typename T>
  void ensureValuesCapacity(vector_size_t numRows);

  // Prepares the result buffer for nulls for reading 'rows'. Leaves
  // 'extraSpace' bits worth of space in the nulls buffer.
  void prepareNulls(RowSet rows, bool hasNulls, int32_t extraRows = 0);

  // Filters 'rows' according to 'is_null'. Only applies to cases where
  // readsNullsOnly() is true.
  template <typename T>
  void filterNulls(RowSet rows, bool isNull, bool extractValues);

  // If 'this' has values set for returning as dictionary-encoded,
  // converts these values to flat so that additional values can be
  // added without reference to dictionary. Resets dictionary info in
  // 'scanState_'. No-op for non-string readers. This is needed when
  // scanning a Parquet ColumnChunk that begins with dictionaries and
  // converts to direct in mid-read.
  virtual void dedictionarize() {}

 protected:
  template <typename T>
  void
  prepareRead(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls);

  void setOutputRows(RowSet rows) {
    outputRows_.resize(rows.size());
    if (!rows.size()) {
      return;
    }
    memcpy(outputRows_.data(), &rows[0], rows.size() * sizeof(vector_size_t));
  }

  // Returns integer values for 'rows' cast to the width of
  // 'requestedType' in '*result'.
  void getIntValues(RowSet rows, const Type* requestedType, VectorPtr* result);

  // Returns read values for 'rows' in 'vector'. This can be called
  // multiple times for consecutive subsets of 'rows'. If 'isFinal' is
  // true, this is free not to maintain the information mapping values
  // to rows. TODO: Consider isFinal as template parameter.
  template <typename T, typename TVector>
  void getFlatValues(
      RowSet rows,
      VectorPtr* result,
      const TypePtr& type = CppToType<TVector>::create(),
      bool isFinal = false);

  template <typename T, typename TVector>
  void compactScalarValues(RowSet rows, bool isFinal);

  template <typename T, typename TVector>
  void upcastScalarValues(RowSet rows);

  // Returns true if compactScalarValues and upcastScalarValues should
  // move null flags. Checks consistency of nulls-related state.
  bool shouldMoveNulls(RowSet rows);

  void addStringValue(folly::StringPiece value);

  // Copies 'value' to buffers owned by 'this' and returns the start of the
  // copy.
  char* copyStringValue(folly::StringPiece value);

  memory::MemoryPool& memoryPool_;

  std::shared_ptr<const dwio::common::TypeWithId> nodeType_;

  // Format specific state and functions.
  std::unique_ptr<dwio::common::FormatData> formatData_;

  // Specification of filters, value extraction, pruning etc. The
  // spec is assigned at construction and the contents may change at
  // run time based on adaptation. Owned by caller.
  velox::common::ScanSpec* const scanSpec_;
  TypePtr type_;

  // Row number after last read row, relative to stripe start.
  vector_size_t readOffset_ = 0;
  // The rows to process in read(). References memory supplied by
  // caller. The values must remain live until the next call to read().
  RowSet inputRows_;
  // Rows passing the filter in readWithVisitor. Must stay
  // constant between consecutive calls to read().
  raw_vector<vector_size_t> outputRows_;
  // Index of last set value in outputRows. Values between this and
  // size() can be used as scratchpad inside read().
  vector_size_t numOutConfirmed_;
  // The row number
  // corresponding to each element in 'values_'
  raw_vector<vector_size_t> valueRows_;
  // The set of all nulls in the range of read(). Created when first
  // needed and then reused. May be referenced by result if all rows are
  // selected.
  BufferPtr nullsInReadRange_;
  // Nulls buffer for readWithVisitor. Not set if no nulls. 'numValues'
  // is the index of the first non-set bit.
  BufferPtr resultNulls_;
  uint64_t* rawResultNulls_ = nullptr;
  // Buffer for gathering scalar values in readWithVisitor.
  BufferPtr values_;
  // Writable content in 'values'
  void* rawValues_ = nullptr;
  vector_size_t numValues_ = 0;
  // Size of fixed width value in 'rawValues'. For integers, values
  // are read at 64 bit width and can be compacted or extracted at a
  // different width.
  int8_t valueSize_ = kNoValueSize;

  // true if 'this' is in a state where gatValues can be called.
  bool mayGetValues_ = false;

  // True if row numbers of 'this' correspond 1:1 to row numbers in
  // the file. This is false inside lists, maps and nullable
  // structs. If true, a skip of n rows can use row group indices to
  // skip long distances. Lazy vectors will only be made for results
  // of top level readers.
  bool isTopLevel_{false};

  // Maps from position in non-null rows to a position in value
  // sequence with nulls included. Empty if no nulls.
  raw_vector<int32_t> outerNonNullRows_;
  // Rows of qualifying set in terms of non-null rows. Empty if no
  // nulls or if reading all rows.
  raw_vector<int32_t> innerNonNullRows_;
  // Buffers backing the StringViews in 'values' when reading strings.
  std::vector<BufferPtr> stringBuffers_;
  // Writable contents of 'stringBuffers_.back()'.
  char* rawStringBuffer_ = nullptr;
  // True if a vector can acquire a pin to a stream's buffer and refer
  // to that as its values.
  bool mayUseStreamBuffer_ = false;
  // True if nulls and everything selected, so that nullsInReadRange
  // can be returned as the null flags of the vector in getValues().
  bool returnReaderNulls_ = false;
  // Total writable bytes in 'rawStringBuffer_'.
  int32_t rawStringSize_ = 0;
  // Number of written bytes in 'rawStringBuffer_'.
  uint32_t rawStringUsed_ = 0;

  // True if last read() added any nulls.
  bool anyNulls_ = false;
  // True if all values in scope for last read() are null.
  bool allNull_ = false;

  // Number of clocks spent initializing.
  uint64_t initTimeClocks_{0};

  // Encoding-related state to keep between reads, e.g. dictionaries.
  ScanState scanState_;
};

template <>
inline void SelectiveColumnReader::addValue(const folly::StringPiece value) {
  const uint64_t size = value.size();
  if (size <= StringView::kInlineSize) {
    reinterpret_cast<StringView*>(rawValues_)[numValues_++] =
        StringView(value.data(), size);
    return;
  }
  if (rawStringBuffer_ && rawStringUsed_ + size <= rawStringSize_) {
    memcpy(rawStringBuffer_ + rawStringUsed_, value.data(), size);
    reinterpret_cast<StringView*>(rawValues_)[numValues_++] =
        StringView(rawStringBuffer_ + rawStringUsed_, size);
    rawStringUsed_ += size;
    return;
  }
  addStringValue(value);
}

} // namespace facebook::velox::dwio::common

namespace facebook::velox::dwio::common {
// Template parameter to indicate no hook in fast scan path. This is
// referenced in decoders, thus needs to be declared in a header.
struct NoHook : public ValueHook {
  void addValue(vector_size_t /*row*/, const void* /*value*/) override {}
};

} // namespace facebook::velox::dwio::common
