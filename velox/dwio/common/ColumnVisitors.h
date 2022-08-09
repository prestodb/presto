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
#include "velox/common/base/SimdUtil.h"
#include "velox/dwio/common/DecoderUtil.h"
#include "velox/dwio/common/SelectiveColumnReader.h"

namespace facebook::velox::dwio::common {

// structs for extractValues in ColumnVisitor.

dwio::common::NoHook& noHook();

// Represents values not being retained after filter evaluation.

struct DropValues {
  static constexpr bool kSkipNulls = false;
  using HookType = dwio::common::NoHook;

  bool acceptsNulls() const {
    return true;
  }

  template <typename V>
  void addValue(vector_size_t /*rowIndex*/, V /*value*/) {}

  template <typename T>
  void addNull(vector_size_t /*rowIndex*/) {}

  HookType& hook() {
    return noHook();
  }
};

template <typename TReader>
struct ExtractToReader {
  using HookType = dwio::common::NoHook;
  static constexpr bool kSkipNulls = false;
  explicit ExtractToReader(TReader* readerIn) : reader(readerIn) {}

  bool acceptsNulls() const {
    return true;
  }

  template <typename T>
  void addNull(vector_size_t rowIndex);

  template <typename V>
  void addValue(vector_size_t /*rowIndex*/, V value) {
    reader->addValue(value);
  }

  TReader* reader;

  dwio::common::NoHook& hook() {
    return noHook();
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

  template <typename T>
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

  template <typename T>
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

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class DictionaryColumnVisitor;

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
          VELOX_CHECK_LE(rowIndex_, rowOfNullWord + nextNonNull);
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
      if (velox::common::applyFilter(filter_, value)) {
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
    if (velox::common::applyFilter(filter_, value)) {
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

  int32_t numRows() const {
    return reader_->numRows();
  }

  SelectiveColumnReader& reader() const {
    return *reader_;
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
    if (!std::is_same<TFilter, velox::common::AlwaysTrue>::value) {
      addOutputRow(currentRow());
    }
  }

  inline void filterPassedForNull() {
    addNull();
    if (!std::is_same<TFilter, velox::common::AlwaysTrue>::value) {
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

  void setNumValuesBias(int32_t bias) {
    numValuesBias_ = bias;
  }

  void setNumValues(int32_t size) {
    reader_->setNumValues(numValuesBias_ + size);
    if (!std::is_same<TFilter, velox::common::AlwaysTrue>::value) {
      reader_->setNumRows(numValuesBias_ + size);
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
    setNumValues(numValues);
    reader_->setAllNull();
  }

  auto& innerNonNullRows() {
    return reader_->innerNonNullRows();
  }

  auto& outerNonNullRows() {
    return reader_->outerNonNullRows();
  }

  raw_vector<vector_size_t>& rowsCopy() const {
    return reader_->scanState().rowsCopy;
  }

  DictionaryColumnVisitor<T, TFilter, ExtractValues, isDense>
  toDictionaryColumnVisitor();

  // Use for replacing *coall rows with non-null rows for fast path with
  // processRun and processRle.
  void setRows(folly::Range<const int32_t*> newRows) {
    rows_ = newRows.data();
    numRows_ = newRows.size();
  }

 protected:
  TFilter& filter_;
  SelectiveColumnReader* reader_;
  const bool allowNulls_;
  const vector_size_t* rows_;
  vector_size_t numRows_;
  vector_size_t rowIndex_;
  int32_t numValuesBias_{0};
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
  values_.template addNull<T>(rowIndex_);
}

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
inline void ColumnVisitor<T, TFilter, ExtractValues, isDense>::addOutputRow(
    vector_size_t row) {
  reader_->addOutputRow(row);
}

template <typename TReader>
template <typename T>
void ExtractToReader<TReader>::addNull(vector_size_t /*rowIndex*/) {
  reader->template addNull<T>();
}

enum FilterResult { kUnknown = 0x40, kSuccess = 0x80, kFailure = 0 };

namespace detail {

template <typename T, typename A>
struct LoadIndices;

template <typename A>
struct LoadIndices<int32_t, A> {
  static xsimd::batch<int32_t, A> apply(const int32_t* values, const A&) {
    return xsimd::load_unaligned<A>(values);
  }
};

template <typename A>
struct LoadIndices<int16_t, A> {
  static xsimd::batch<int32_t, A> apply(
      const int16_t* values,
      const xsimd::generic&) {
    constexpr int N = xsimd::batch<int32_t, A>::size;
    alignas(A::alignment()) int32_t tmp[N];
    for (int i = 0; i < N; ++i) {
      tmp[i] = values[i];
    }
    return xsimd::load_aligned(tmp);
  }

#if XSIMD_WITH_AVX2
  static xsimd::batch<int32_t, A> apply(
      const int16_t* values,
      const xsimd::avx2&) {
    return _mm256_cvtepi16_epi32(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(values)));
  }
#endif
};

template <typename A>
struct LoadIndices<int64_t, A> {
  static xsimd::batch<int32_t, A> apply(const int64_t* values, const A& arch) {
    return simd::gather<int32_t, int32_t, 8>(
        reinterpret_cast<const int32_t*>(values),
        simd::iota<int32_t>(arch),
        arch);
  }
};

} // namespace detail

template <typename T, typename A = xsimd::default_arch>
inline xsimd::batch<int32_t> loadIndices(const T* values, const A& arch = {}) {
  return detail::LoadIndices<T, A>::apply(values, arch);
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
    xsimd::batch<int32_t> /*indices*/,
    int selected,
    xsimd::batch_bool<int32_t> dictMask,
    int8_t numBits,
    const T* dict,
    T* values) {
  auto selectedIndices = simd::byteSetBits(selected);
  auto inDict = simd::toBitMask(dictMask);
  for (auto i = 0; i < numBits; ++i) {
    auto value = input[inputIndex + selectedIndices[i]];
    if (inDict & (1 << selectedIndices[i])) {
      value = dict[value];
    }
    values[i] = value;
  }
}

template <>
inline void storeTranslatePermute(
    const int32_t* /*input*/,
    int32_t /*inputIndex*/,
    xsimd::batch<int32_t> indices,
    int selected,
    xsimd::batch_bool<int32_t> dictMask,
    int8_t /*numBits*/,
    const int32_t* dict,
    int32_t* values) {
  auto translated = simd::maskGather(indices, dictMask, dict, indices);
  simd::filter(translated, selected).store_unaligned(values);
}

// Stores 8 elements starting at 'input' + 'inputIndex' into
// 'values'. The values are translated via 'dict' for the positions
// that are true in 'dictMask'.
template <typename T>
inline void storeTranslate(
    const T* input,
    int32_t inputIndex,
    xsimd::batch<int32_t> /*indices*/,
    xsimd::batch_bool<int32_t> dictMask,
    const T* dict,
    T* values) {
  auto inDict = simd::toBitMask(dictMask);
  for (auto i = 0; i < dictMask.size; ++i) {
    auto value = input[inputIndex + i];
    if (inDict & (1 << i)) {
      value = dict[value];
    }
    values[i] = value;
  }
}

template <>
inline void storeTranslate(
    const int32_t* /*input*/,
    int32_t /*inputIndex*/,
    xsimd::batch<int32_t> indices,
    xsimd::batch_bool<int32_t> dictMask,
    const int32_t* dict,
    int32_t* values) {
  simd::maskGather(indices, dictMask, dict, indices).store_unaligned(values);
}

namespace detail {

#if XSIMD_WITH_AVX2
inline xsimd::batch<int64_t> cvtU32toI64(
    xsimd::batch<int32_t, xsimd::sse2> values) {
  return _mm256_cvtepu32_epi64(values);
}
#elif XSIMD_WITH_SSE2 || XSIMD_WITH_NEON
inline xsimd::batch<int64_t> cvtU32toI64(simd::Batch64<int32_t> values) {
  int64_t lo = static_cast<uint32_t>(values.data[0]);
  int64_t hi = static_cast<uint32_t>(values.data[1]);
  return xsimd::batch<int64_t>({lo, hi});
}
#endif

} // namespace detail

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class DictionaryColumnVisitor
    : public ColumnVisitor<T, TFilter, ExtractValues, isDense> {
  using super = ColumnVisitor<T, TFilter, ExtractValues, isDense>;

 public:
  DictionaryColumnVisitor(
      TFilter& filter,
      SelectiveColumnReader* reader,
      RowSet rows,
      ExtractValues values)
      : ColumnVisitor<T, TFilter, ExtractValues, isDense>(
            filter,
            reader,
            rows,
            values),
        state_(reader->scanState().rawState),
        width_(
            reader->type()->kind() == TypeKind::BIGINT        ? 8
                : reader->type()->kind() == TypeKind::INTEGER ? 4
                                                              : 2) {}

  FOLLY_ALWAYS_INLINE bool isInDict() {
    if (inDict()) {
      return bits::isBitSet(inDict(), super::currentRow());
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
    std::make_unsigned_t<T> index = value;
    T valueInDictionary = dict()[index];
    if (std::is_same<TFilter, velox::common::AlwaysTrue>::value) {
      super::filterPassed(valueInDictionary);
    } else {
      // check the dictionary cache
      if (TFilter::deterministic &&
          filterCache()[index] == FilterResult::kSuccess) {
        super::filterPassed(valueInDictionary);
      } else if (
          TFilter::deterministic &&
          filterCache()[index] == FilterResult::kFailure) {
        super::filterFailed();
      } else {
        if (super::filter_.testInt64(valueInDictionary)) {
          super::filterPassed(valueInDictionary);
          if (TFilter::deterministic) {
            filterCache()[index] = FilterResult::kSuccess;
          }
        } else {
          super::filterFailed();
          if (TFilter::deterministic) {
            filterCache()[index] = FilterResult::kFailure;
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
      if (inDict()) {
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
    constexpr int32_t kWidth = xsimd::batch<int32_t>::size;
    int32_t last = numInput & ~(kWidth - 1);
    for (auto i = 0; i < numInput; i += kWidth) {
      int8_t width = UNLIKELY(i == last) ? numInput - last : kWidth;
      auto indices = loadIndices(input + i);
      xsimd::batch_bool<int32_t> dictMask;
      if (inDict()) {
        if (simd::isDense(super::rows_ + super::rowIndex_ + i, width)) {
          dictMask = load8MaskDense(
              inDict(), super::rows_[super::rowIndex_ + i], width);
        } else {
          dictMask = load8MaskSparse(
              inDict(), super::rows_ + super::rowIndex_ + i, width);
        }
      } else {
        dictMask = simd::leadingMask<int32_t>(width);
      }

      // Load 8 filter cache values. Defaults the extra to values to 0 if
      // loading less than 8.
      auto cache = simd::maskGather<int32_t, int32_t, 1>(
          xsimd::broadcast<int32_t>(0),
          dictMask,
          reinterpret_cast<const int32_t*>(filterCache() - 3),
          indices);
      auto unknowns = simd::toBitMask(
          xsimd::batch_bool<int32_t>((cache & (kUnknown << 24)) << 1));
      auto passed = simd::toBitMask(xsimd::batch_bool<int32_t>(cache));
      if (UNLIKELY(unknowns)) {
        uint16_t bits = unknowns;
        // Ranges only over inputs that are in dictionary, the not in dictionary
        // were masked off in 'dictMask'.
        while (bits) {
          int index = bits::getAndClearLastSetBit(bits);
          auto value = input[i + index];
          if (applyFilter(super::filter_, dict()[value])) {
            filterCache()[value] = FilterResult::kSuccess;
            passed |= 1 << index;
          } else {
            filterCache()[value] = FilterResult::kFailure;
          }
        }
      }
      // Were there values not in dictionary?
      if (inDict()) {
        auto mask = simd::toBitMask(dictMask);
        const auto allTrue = simd::allSetBitMask<int32_t>();
        if (mask != allTrue) {
          uint16_t bits = (allTrue ^ mask) & bits::lowMask(kWidth);
          while (bits) {
            auto index = bits::getAndClearLastSetBit(bits);
            if (i + index >= numInput) {
              break;
            }
            if (velox::common::applyFilter(super::filter_, input[i + index])) {
              passed |= 1 << index;
            }
          }
        }
      }
      // We know 8 compare results. If all false, process next batch.
      if (!passed) {
        continue;
      } else if (passed == (1 << xsimd::batch<int32_t>::size) - 1) {
        // All passed, no need to shuffle the indices or values, write then to
        // 'values' and 'filterHits'.
        xsimd::load_unaligned(
            (scatter ? scatterRows : super::rows_) + super::rowIndex_ + i)
            .store_unaligned(filterHits + numValues);
        if (!kFilterOnly) {
          storeTranslate(
              input, i, indices, dictMask, dict(), values + numValues);
        }
        numValues += kWidth;
      } else {
        // Some passed. Permute  the passing row numbers and values to the left
        // of the SIMD vector and store.
        int8_t numBits = __builtin_popcount(passed);
        simd::filter(
            xsimd::load_unaligned(
                (scatter ? scatterRows : super::rows_) + super::rowIndex_ + i),
            passed)
            .store_unaligned(filterHits + numValues);
        if (!kFilterOnly) {
          storeTranslatePermute(
              input,
              i,
              indices,
              passed,
              dictMask,
              numBits,
              dict(),
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
      constexpr int32_t kWidth = xsimd::batch<int64_t>::size;
      for (auto i = 0; i < numRows; i += kWidth) {
        auto numbers = detail::cvtU32toI64(
                           simd::loadGatherIndices<int64_t>(
                               super::rows_ + super::rowIndex_ + i) -
                           currentRow) *
                delta +
            value;
        numbers.store_unaligned(values + numValues + i);
      }
    } else if (sizeof(T) == 4) {
      constexpr int32_t kWidth = xsimd::batch<int32_t>::size;
      for (auto i = 0; i < numRows; i += kWidth) {
        auto numbers =
            (xsimd::load_unaligned(super::rows_ + super::rowIndex_ + i) -
             currentRow) *
                static_cast<int32_t>(delta) +
            static_cast<int32_t>(value);
        numbers.store_unaligned(values + numValues + i);
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
        if (bits::isBitSet(inDict(), super::rows_[super::rowIndex_ + i])) {
          value = dict()[static_cast<U>(value)];
        } else if (!scatter) {
          continue;
        }
      } else {
        value = dict()[static_cast<U>(value)];
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
  xsimd::batch_bool<int32_t>
  load8MaskDense(const uint64_t* bits, int32_t index, int8_t numBits) {
    uint8_t shift = index & 7;
    uint32_t byte = index >> 3;
    auto asBytes = reinterpret_cast<const uint8_t*>(bits);
    auto mask = (*reinterpret_cast<const int16_t*>(asBytes + byte) >> shift) &
        bits::lowMask(numBits);
    return simd::fromBitMask<int32_t>(mask);
  }

  // Returns 'numBits' bits at bit offsets in 'rows' from 'bits' as a
  // 8x32 mask for use in maskGather.
  xsimd::batch_bool<int32_t>
  load8MaskSparse(const uint64_t* bits, const int32_t* rows, int8_t numRows) {
    return simd::fromBitMask<int32_t>(simd::gather8Bits(bits, rows, numRows));
  }

  void translateByDict(const T* values, int numValues, T* out) {
    if (!inDict()) {
      for (auto i = 0; i < numValues; ++i) {
        out[i] = dict()[values[i]];
      }
    } else if (super::dense) {
      bits::forEachSetBit(
          inDict(),
          super::rowIndex_,
          super::rowIndex_ + numValues,
          [&](int row) {
            auto valueIndex = row - super::rowIndex_;
            out[valueIndex] = dict()[values[valueIndex]];
            return true;
          });
    } else {
      for (auto i = 0; i < numValues; ++i) {
        if (bits::isBitSet(inDict(), super::rows_[super::rowIndex_ + i])) {
          out[i] = dict()[values[i]];
        }
      }
    }
  }

 protected:
  const uint64_t* inDict() const {
    return state_.inDictionary;
  }

  const T* dict() const {
    return reinterpret_cast<const T*>(state_.dictionary.values);
  }

  int32_t dictionarySize() const {
    return state_.dictionary.numValues;
  }

  uint8_t* filterCache() const {
    return state_.filterCache;
  }

  RawScanState state_;
  const uint8_t width_;
};

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
DictionaryColumnVisitor<T, TFilter, ExtractValues, isDense>
ColumnVisitor<T, TFilter, ExtractValues, isDense>::toDictionaryColumnVisitor() {
  auto result = DictionaryColumnVisitor<T, TFilter, ExtractValues, isDense>(
      filter_, reader_, RowSet(rows_ + rowIndex_, numRows_), values_);
  result.numValuesBias_ = numValuesBias_;
  return result;
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
      SelectiveColumnReader* reader,
      RowSet rows,
      ExtractValues values)
      : DictionaryColumnVisitor<int32_t, TFilter, ExtractValues, isDense>(
            filter,
            reader,
            rows,
            values) {}

  FOLLY_ALWAYS_INLINE vector_size_t process(int32_t value, bool& atEnd) {
    bool inStrideDict = !DictSuper::isInDict();
    auto index = value;
    if (inStrideDict) {
      index += DictSuper::dictionarySize();
    }
    vector_size_t previous =
        isDense && TFilter::deterministic ? 0 : super::currentRow();
    if (std::is_same<TFilter, velox::common::AlwaysTrue>::value) {
      super::filterPassed(index);
    } else {
      // check the dictionary cache
      if (TFilter::deterministic &&
          DictSuper::filterCache()[index] == FilterResult::kSuccess) {
        super::filterPassed(index);
      } else if (
          TFilter::deterministic &&
          DictSuper::filterCache()[index] == FilterResult::kFailure) {
        super::filterFailed();
      } else {
        if (velox::common::applyFilter(
                super::filter_, valueInDictionary(value, inStrideDict))) {
          super::filterPassed(index);
          if (TFilter::deterministic) {
            DictSuper::filterCache()[index] = FilterResult::kSuccess;
          }
        } else {
          super::filterFailed();
          if (TFilter::deterministic) {
            DictSuper::filterCache()[index] = FilterResult::kFailure;
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
    DCHECK(input == values + numValues);
    setByInDict(values + numValues, numInput);
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
        dwio::common::scatterDense(
            input, scatterRows + super::rowIndex_, numInput, values);
      }
      numValues = scatter ? scatterRows[super::rowIndex_ + numInput - 1] + 1
                          : numValues + numInput;
      super::rowIndex_ += numInput;
      return;
    }
    constexpr bool filterOnly =
        std::is_same<typename super::Extract, DropValues>::value;
    constexpr int32_t kWidth = xsimd::batch<int32_t>::size;
    for (auto i = 0; i < numInput; i += kWidth) {
      auto indices = xsimd::load_unaligned(input + i);
      xsimd::batch<int32_t> cache;
      auto base =
          reinterpret_cast<const int32_t*>(DictSuper::filterCache() - 3);
      if (i + kWidth > numInput) {
        cache = simd::maskGather<int32_t, int32_t, 1>(
            xsimd::broadcast<int32_t>(0),
            simd::leadingMask<int32_t>(numInput - i),
            base,
            indices);
      } else {
        cache = simd::gather<int32_t, int32_t, 1>(base, indices);
      }
      auto unknowns = simd::toBitMask(
          xsimd::batch_bool<int32_t>((cache & (kUnknown << 24)) << 1));
      auto passed = simd::toBitMask(xsimd::batch_bool<int32_t>(cache));
      if (UNLIKELY(unknowns)) {
        uint16_t bits = unknowns;
        while (bits) {
          int index = bits::getAndClearLastSetBit(bits);
          int32_t value = input[i + index];
          bool result;
          if (value >= DictSuper::dictionarySize()) {
            result = applyFilter(
                super::filter_,
                valueInDictionary(value - DictSuper::dictionarySize(), true));
          } else {
            result =
                applyFilter(super::filter_, valueInDictionary(value, false));
          }
          if (result) {
            DictSuper::filterCache()[value] = FilterResult::kSuccess;
            passed |= 1 << index;
          } else {
            DictSuper::filterCache()[value] = FilterResult::kFailure;
          }
        }
      }
      if (!passed) {
        continue;
      } else if (passed == (1 << kWidth) - 1) {
        xsimd::load_unaligned(
            (scatter ? scatterRows : super::rows_) + super::rowIndex_ + i)
            .store_unaligned(filterHits + numValues);
        if (!filterOnly) {
          indices.store_unaligned(values + numValues);
        }
        numValues += kWidth;
      } else {
        int8_t numBits = __builtin_popcount(passed);
        simd::filter(
            xsimd::load_unaligned(
                (scatter ? scatterRows : super::rows_) + super::rowIndex_ + i),
            passed)
            .store_unaligned(filterHits + numValues);
        if (!filterOnly) {
          simd::filter(indices, passed).store_unaligned(values + numValues);
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
    constexpr int32_t kWidth = xsimd::batch<int32_t>::size;
    for (auto i = 0; i < numRows; i += kWidth) {
      ((xsimd::load_unaligned(super::rows_ + super::rowIndex_ + i) -
        currentRow) *
           delta +
       value)
          .store_unaligned(values + numValues + i);
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
    if (DictSuper::inDict()) {
      auto current = super::rowIndex_;
      int32_t i = 0;
      for (; i < numValues; ++i) {
        if (!bits::isBitSet(DictSuper::inDict(), super::rows_[i + current])) {
          values[i] += DictSuper::dictionarySize();
        }
      }
    }
  }

  folly::StringPiece valueInDictionary(int64_t index, bool inStrideDict) {
    if (inStrideDict) {
      return folly::StringPiece(reinterpret_cast<const StringView*>(
          DictSuper::state_.dictionary2.values)[index]);
    }
    return folly::StringPiece(reinterpret_cast<const StringView*>(
        DictSuper::state_.dictionary.values)[index]);
  }
};

class ExtractStringDictionaryToGenericHook {
 public:
  static constexpr bool kSkipNulls = true;
  using HookType = ValueHook;

  ExtractStringDictionaryToGenericHook(
      ValueHook* hook,
      RowSet rows,
      RawScanState state)

      : hook_(hook), rows_(rows), state_(state) {}

  bool acceptsNulls() {
    return hook_->acceptsNulls();
  }

  template <typename T>
  void addNull(vector_size_t rowIndex) {
    hook_->addNull(rowIndex);
  }

  void addValue(vector_size_t rowIndex, int32_t value) {
    // We take the string from the stripe or stride dictionary
    // according to the index. Stride dictionary indices are offset up
    // by the stripe dict size.
    if (value < dictionarySize()) {
      auto view = folly::StringPiece(
          reinterpret_cast<const StringView*>(state_.dictionary.values)[value]);
      hook_->addValue(rowIndex, &view);
    } else {
      VELOX_DCHECK(state_.inDictionary);
      auto view = folly::StringPiece(
          reinterpret_cast<const StringView*>(state_.dictionary.values)[value]);
      hook_->addValue(rowIndex, &view);
    }
  }

  ValueHook& hook() {
    return *hook_;
  }

 private:
  int32_t dictionarySize() const {
    return state_.dictionary.numValues;
  }

  ValueHook* const hook_;
  RowSet const rows_;
  RawScanState state_;
};

template <typename T, typename TFilter, typename ExtractValues, bool isDense>
class DirectRleColumnVisitor
    : public ColumnVisitor<T, TFilter, ExtractValues, isDense> {
  using super = ColumnVisitor<T, TFilter, ExtractValues, isDense>;

 public:
  DirectRleColumnVisitor(
      TFilter& filter,
      SelectiveColumnReader* reader,
      RowSet rows,
      ExtractValues values)
      : ColumnVisitor<T, TFilter, ExtractValues, isDense>(
            filter,
            reader,
            rows,
            values) {}

  // Use for replacing all rows with non-null rows for fast path with
  // processRun and processRle.
  void setRows(folly::Range<const int32_t*> newRows) {
    super::rows_ = newRows.data();
    super::numRows_ = newRows.size();
  }

  // Processes 'numInput' T's in 'input'. Sets 'values' and
  // 'numValues'' to the resulting values. 'scatterRows' may be
  // non-null if there is no filter and the decoded values should be
  // scattered into values with gaps in between so as to leave gaps
  // for nulls. If scatterRows is given, the ith value goes to
  // values[scatterRows[i]], else it goes to 'values[i]'. If
  // 'hasFilter' is true, the passing values are written to
  // consecutive places in 'values'.
  template <bool hasFilter, bool hasHook, bool scatter>
  void processRun(
      const T* input,
      int32_t numInput,
      const int32_t* scatterRows,
      int32_t* filterHits,
      T* values,
      int32_t& numValues) {
    DCHECK_EQ(input, values + numValues);
    constexpr bool filterOnly =
        std::is_same<typename super::Extract, DropValues>::value;

    dwio::common::processFixedWidthRun<T, filterOnly, scatter, isDense>(
        folly::Range<const vector_size_t*>(super::rows_, super::numRows_),
        super::rowIndex_,
        numInput,
        scatterRows,
        values,
        filterHits,
        numValues,
        super::filter_,
        super::values_.hook());

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
      constexpr int32_t kWidth = xsimd::batch<int64_t>::size;
      for (auto i = 0; i < numRows; i += kWidth) {
        auto numbers = detail::cvtU32toI64(
                           simd::loadGatherIndices<int64_t>(
                               super::rows_ + super::rowIndex_ + i) -
                           currentRow) *
                delta +
            value;
        numbers.store_unaligned(values + numValues + i);
      }
    } else if (sizeof(T) == 4) {
      constexpr int32_t kWidth = xsimd::batch<int32_t>::size;
      for (auto i = 0; i < numRows; i += kWidth) {
        auto numbers =
            (xsimd::load_unaligned(super::rows_ + super::rowIndex_ + i) -
             currentRow) *
                static_cast<int32_t>(delta) +
            static_cast<int32_t>(value);
        numbers.store_unaligned(values + numValues + i);
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
};

} // namespace facebook::velox::dwio::common
