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

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>

#include "velox/common/base/Exceptions.h"
#include "velox/type/Filter.h"

namespace facebook::velox::common {

std::string Filter::toString() const {
  const char* strKind = "<unknown>";
  switch (kind_) {
    case FilterKind::kAlwaysFalse:
      strKind = "AlwaysFalse";
      break;
    case FilterKind::kAlwaysTrue:
      strKind = "AlwaysTrue";
      break;
    case FilterKind::kIsNull:
      strKind = "IsNull";
      break;
    case FilterKind::kIsNotNull:
      strKind = "IsNotNull";
      break;
    case FilterKind::kBoolValue:
      strKind = "BoolValue";
      break;
    case FilterKind::kBigintRange:
      strKind = "BigintRange";
      break;
    case FilterKind::kNegatedBigintRange:
      strKind = "NegatedBigintRange";
      break;
    case FilterKind::kBigintValuesUsingHashTable:
      strKind = "BigintValuesUsingHashTable";
      break;
    case FilterKind::kBigintValuesUsingBitmask:
      strKind = "BigintValuesUsingBitmask";
      break;
    case FilterKind::kNegatedBigintValuesUsingHashTable:
      strKind = "NegatedBigintValuesUsingHashTable";
      break;
    case FilterKind::kNegatedBigintValuesUsingBitmask:
      strKind = "NegatedBigintValuesUsingBitmask";
      break;
    case FilterKind::kDoubleRange:
      strKind = "DoubleRange";
      break;
    case FilterKind::kFloatRange:
      strKind = "FloatRange";
      break;
    case FilterKind::kBytesRange:
      strKind = "BytesRange";
      break;
    case FilterKind::kNegatedBytesRange:
      strKind = "NegatedBytesRange";
      break;
    case FilterKind::kBytesValues:
      strKind = "BytesValues";
      break;
    case FilterKind::kNegatedBytesValues:
      strKind = "NegatedBytesValues";
      break;
    case FilterKind::kBigintMultiRange:
      strKind = "BigintMultiRange";
      break;
    case FilterKind::kMultiRange:
      strKind = "MultiRange";
      break;
  };

  return fmt::format(
      "Filter({}, {}, {})",
      strKind,
      deterministic_ ? "deterministic" : "nondeterministic",
      nullAllowed_ ? "null allowed" : "null not allowed");
}

BigintValuesUsingBitmask::BigintValuesUsingBitmask(
    int64_t min,
    int64_t max,
    const std::vector<int64_t>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBigintValuesUsingBitmask),
      min_(min),
      max_(max) {
  VELOX_CHECK(min < max, "min must be less than max");
  VELOX_CHECK(values.size() > 1, "values must contain at least 2 entries");

  bitmask_.resize(max - min + 1);

  for (int64_t value : values) {
    bitmask_[value - min] = true;
  }
}

bool BigintValuesUsingBitmask::testInt64(int64_t value) const {
  if (value < min_ || value > max_) {
    return false;
  }
  return bitmask_[value - min_];
}

std::vector<int64_t> BigintValuesUsingBitmask::values() const {
  std::vector<int64_t> values;
  for (int i = 0; i < bitmask_.size(); i++) {
    if (bitmask_[i]) {
      values.push_back(min_ + i);
    }
  }
  return values;
}

bool BigintValuesUsingBitmask::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min == max) {
    return testInt64(min);
  }

  return !(min > max_ || max < min_);
}

BigintValuesUsingHashTable::BigintValuesUsingHashTable(
    int64_t min,
    int64_t max,
    const std::vector<int64_t>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBigintValuesUsingHashTable),
      min_(min),
      max_(max),
      values_(values) {
  constexpr int32_t kPaddingElements = 4;
  VELOX_CHECK(min < max, "min must be less than max");
  VELOX_CHECK(values.size() > 1, "values must contain at least 2 entries");

  // Size the hash table to be 2+x the entry count, e.g. 10 entries
  // gets 1 << log2 of 50 == 32. The filter is expected to fail often so we
  // wish to increase the chance of hitting empty on first probe.
  auto size = 1u << (uint32_t)std::log2(values.size() * 5);
  hashTable_.resize(size + kPaddingElements);
  sizeMask_ = size - 1;
  std::fill(hashTable_.begin(), hashTable_.end(), kEmptyMarker);
  for (auto value : values) {
    if (value == kEmptyMarker) {
      containsEmptyMarker_ = true;
    } else {
      auto position = ((value * M) & (size - 1));
      for (auto i = position; i < position + size; i++) {
        uint32_t index = i & sizeMask_;
        if (hashTable_[index] == kEmptyMarker) {
          hashTable_[index] = value;
          break;
        }
      }
    }
  }
  // Replicate the last element of hashTable kPaddingEntries times at 'size_' so
  // that one can load a full vector of elements past the last used index.
  for (auto i = 0; i < kPaddingElements; ++i) {
    hashTable_[sizeMask_ + 1 + i] = hashTable_[sizeMask_];
  }
  std::sort(values_.begin(), values_.end());
}

bool BigintValuesUsingHashTable::testInt64(int64_t value) const {
  if (containsEmptyMarker_ && value == kEmptyMarker) {
    return true;
  }
  if (value < min_ || value > max_) {
    return false;
  }
  uint32_t pos = (value * M) & sizeMask_;
  for (auto i = pos; i <= pos + sizeMask_; i++) {
    int32_t idx = i & sizeMask_;
    int64_t l = hashTable_[idx];
    if (l == kEmptyMarker) {
      return false;
    }
    if (l == value) {
      return true;
    }
  }
  return false;
}

xsimd::batch_bool<int64_t> BigintValuesUsingHashTable::testValues(
    xsimd::batch<int64_t> x) const {
  auto outOfRange = (x < xsimd::broadcast<int64_t>(min_)) |
      (x > xsimd::broadcast<int64_t>(max_));
  if (simd::toBitMask(outOfRange) == simd::allSetBitMask<int64_t>()) {
    return xsimd::batch_bool<int64_t>(false);
  }
  if (containsEmptyMarker_) {
    return Filter::testValues(x);
  }
  auto allEmpty = xsimd::broadcast<int64_t>(kEmptyMarker);
  xsimd::batch<int64_t> indices(xsimd::batch<uint64_t>(x) * M & sizeMask_);
  auto data =
      simd::maskGather(allEmpty, ~outOfRange, hashTable_.data(), indices);
  // The lanes with kEmptyMarker missed, the lanes matching x hit and the other
  // lanes must check next positions.

  auto result = x == data;
  auto resultBits = simd::toBitMask(result);
  auto missed = simd::toBitMask(data == allEmpty);
  static_assert(decltype(result)::size <= 16);
  uint16_t unresolved = simd::allSetBitMask<int64_t>() ^ (resultBits | missed);
  if (!unresolved) {
    return result;
  }
  constexpr int kAlign = xsimd::default_arch::alignment();
  constexpr int kArraySize = xsimd::batch<int64_t>::size;
  alignas(kAlign) int64_t indicesArray[kArraySize];
  alignas(kAlign) int64_t valuesArray[kArraySize];
  (indices + 1).store_aligned(indicesArray);
  x.store_aligned(valuesArray);
  while (unresolved) {
    auto lane = bits::getAndClearLastSetBit(unresolved);
    // Loop for each unresolved (not hit and
    // not empty) until finding hit or empty.
    int64_t index = indicesArray[lane];
    int64_t value = valuesArray[lane];
    auto allValue = xsimd::broadcast<int64_t>(value);
    for (;;) {
      auto line = xsimd::load_unaligned(hashTable_.data() + index);

      if (simd::toBitMask(line == allValue)) {
        resultBits |= 1 << lane;
        break;
      }
      if (simd::toBitMask(line == allEmpty)) {
        resultBits &= ~(1 << lane);
        break;
      }
      index += line.size;
      if (index > sizeMask_) {
        index = 0;
      }
    }
  }
  return simd::fromBitMask<int64_t>(resultBits);
}

xsimd::batch_bool<int32_t> BigintValuesUsingHashTable::testValues(
    xsimd::batch<int32_t> x) const {
  // Calls 4x64 twice since the hash table is 64 bits wide in any
  // case. A 32-bit hash table would be possible but all the use
  // cases seen are in the 64 bit range.
  auto first = simd::toBitMask(testValues(simd::getHalf<int64_t, 0>(x)));
  auto second = simd::toBitMask(testValues(simd::getHalf<int64_t, 1>(x)));
  return simd::fromBitMask<int32_t>(
      first | (second << xsimd::batch<int64_t>::size));
}

bool BigintValuesUsingHashTable::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min == max) {
    return testInt64(min);
  }

  if (min > max_ || max < min_) {
    return false;
  }
  auto it = std::lower_bound(values_.begin(), values_.end(), min);
  assert(it != values_.end()); // min is already tested to be <= max_.
  if (min == *it) {
    return true;
  }
  return max >= *it;
}

NegatedBigintValuesUsingBitmask::NegatedBigintValuesUsingBitmask(
    int64_t min,
    int64_t max,
    const std::vector<int64_t>& values,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kNegatedBigintValuesUsingBitmask),
      min_(min),
      max_(max) {
  VELOX_CHECK(min <= max, "min must be no greater than max");

  nonNegated_ = std::make_unique<BigintValuesUsingBitmask>(
      min, max, values, !nullAllowed);
}

bool NegatedBigintValuesUsingBitmask::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min == max) {
    return testInt64(min);
  }

  return true;
}

NegatedBigintValuesUsingHashTable::NegatedBigintValuesUsingHashTable(
    int64_t min,
    int64_t max,
    const std::vector<int64_t>& values,
    bool nullAllowed)
    : Filter(
          true,
          nullAllowed,
          FilterKind::kNegatedBigintValuesUsingHashTable) {
  nonNegated_ = std::make_unique<BigintValuesUsingHashTable>(
      min, max, values, !nullAllowed);
}

bool NegatedBigintValuesUsingHashTable::testInt64Range(
    int64_t min,
    int64_t max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min == max) {
    return testInt64(min);
  }

  if (max > nonNegated_->max() || min < nonNegated_->min()) {
    return true;
  }

  auto lo = std::lower_bound(
      nonNegated_->values().begin(), nonNegated_->values().end(), min);
  auto hi = std::lower_bound(
      nonNegated_->values().begin(), nonNegated_->values().end(), max);
  assert(
      lo !=
      nonNegated_->values().end()); // min is already tested to be <= max_.
  if (min != *lo || max != *hi) {
    // at least one of the endpoints of the range succeeds
    return true;
  }
  // Check if all values in this range are in values_ by counting the number
  // of things between min and max
  // if distance is any less, then we are missing an element => something
  // in the range is accepted
  return (std::distance(lo, hi) != max - min);
}

namespace {
std::unique_ptr<Filter> nullOrFalse(bool nullAllowed) {
  if (nullAllowed) {
    return std::make_unique<IsNull>();
  }
  return std::make_unique<AlwaysFalse>();
}

std::unique_ptr<Filter> notNullOrTrue(bool nullAllowed) {
  if (nullAllowed) {
    return std::make_unique<AlwaysTrue>();
  }
  return std::make_unique<IsNotNull>();
}

std::unique_ptr<Filter> createBigintValuesFilter(
    const std::vector<int64_t>& values,
    bool nullAllowed,
    bool negated) {
  if (values.empty()) {
    if (!negated) {
      return nullOrFalse(nullAllowed);
    }
    return notNullOrTrue(nullAllowed);
  }
  if (values.size() == 1) {
    if (negated) {
      return std::make_unique<NegatedBigintRange>(
          values.front(), values.front(), nullAllowed);
    }
    return std::make_unique<BigintRange>(
        values.front(), values.front(), nullAllowed);
  }
  int64_t min = values[0];
  int64_t max = values[0];
  for (int i = 1; i < values.size(); ++i) {
    if (values[i] > max) {
      max = values[i];
    } else if (values[i] < min) {
      min = values[i];
    }
  }
  // If bitmap would have more than 4 words per set bit, we prefer a
  // hash table. If bitmap fits in under 32 words, we use bitmap anyhow.
  int64_t range;
  bool overflow = __builtin_sub_overflow(max, min, &range);
  if (LIKELY(!overflow)) {
    // all accepted/rejected values form one contiguous block
    if ((uint64_t)range + 1 == values.size()) {
      if (negated) {
        return std::make_unique<NegatedBigintRange>(min, max, nullAllowed);
      }
      return std::make_unique<BigintRange>(min, max, nullAllowed);
    }

    if (range < 32 * 64 || range < values.size() * 4 * 64) {
      if (negated) {
        return std::make_unique<NegatedBigintValuesUsingBitmask>(
            min, max, values, nullAllowed);
      }
      return std::make_unique<BigintValuesUsingBitmask>(
          min, max, values, nullAllowed);
    }
  }
  if (negated) {
    return std::make_unique<NegatedBigintValuesUsingHashTable>(
        min, max, values, nullAllowed);
  }
  return std::make_unique<BigintValuesUsingHashTable>(
      min, max, values, nullAllowed);
}
} // namespace

std::unique_ptr<Filter> createBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed) {
  return createBigintValuesFilter(values, nullAllowed, false);
}

std::unique_ptr<Filter> createNegatedBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed) {
  return createBigintValuesFilter(values, nullAllowed, true);
}

BigintMultiRange::BigintMultiRange(
    std::vector<std::unique_ptr<BigintRange>> ranges,
    bool nullAllowed)
    : Filter(true, nullAllowed, FilterKind::kBigintMultiRange),
      ranges_(std::move(ranges)) {
  VELOX_CHECK(!ranges_.empty(), "ranges is empty");
  VELOX_CHECK(ranges_.size() > 1, "should contain at least 2 ranges");
  for (const auto& range : ranges_) {
    lowerBounds_.push_back(range->lower());
  }
  for (int i = 1; i < lowerBounds_.size(); i++) {
    VELOX_CHECK(
        lowerBounds_[i] >= ranges_[i - 1]->upper(),
        "bigint ranges must not overlap");
  }
}

namespace {
int compareRanges(const char* lhs, size_t length, const std::string& rhs) {
  int size = std::min(length, rhs.length());
  int compare = memcmp(lhs, rhs.data(), size);
  if (compare) {
    return compare;
  }
  return length - rhs.size();
}
} // namespace

bool BytesRange::testBytes(const char* value, int32_t length) const {
  if (length == 0) {
    // Empty string. value is null. This is the smallest possible string.
    // It passes the following filters: < non-empty, <= empty | non-empty, >=
    // empty.
    if (lowerUnbounded_) {
      return !upper_.empty() || !upperExclusive_;
    }

    return lower_.empty() && !lowerExclusive_;
  }

  if (singleValue_) {
    if (length != lower_.size()) {
      return false;
    }
    return memcmp(value, lower_.data(), length) == 0;
  }
  if (!lowerUnbounded_) {
    int compare = compareRanges(value, length, lower_);
    if (compare < 0 || (lowerExclusive_ && compare == 0)) {
      return false;
    }
  }
  if (!upperUnbounded_) {
    int compare = compareRanges(value, length, upper_);
    return compare < 0 || (!upperExclusive_ && compare == 0);
  }
  return true;
}

bool BytesRange::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min.has_value() && max.has_value() && min.value() == max.value()) {
    return testBytes(min->data(), min->length());
  }

  if (lowerUnbounded_) {
    // min > upper_
    return min.has_value() &&
        compareRanges(min->data(), min->length(), upper_) < 0;
  }

  if (upperUnbounded_) {
    // max < lower_
    return max.has_value() &&
        compareRanges(max->data(), max->length(), lower_) > 0;
  }

  // min > upper_
  if (min.has_value() &&
      compareRanges(min->data(), min->length(), upper_) > 0) {
    return false;
  }

  // max < lower_
  if (max.has_value() &&
      compareRanges(max->data(), max->length(), lower_) < 0) {
    return false;
  }
  return true;
}

bool BytesValues::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (min.has_value() && max.has_value() && min.value() == max.value()) {
    return testBytes(min->data(), min->length());
  }

  // min > upper_
  if (min.has_value() &&
      compareRanges(min->data(), min->length(), upper_) > 0) {
    return false;
  }

  // max < lower_
  if (max.has_value() &&
      compareRanges(max->data(), max->length(), lower_) < 0) {
    return false;
  }

  return true;
}

bool NegatedBytesRange::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if ((!min.has_value() && !isLowerUnbounded()) ||
      (!max.has_value() && !isUpperUnbounded())) {
    return true;
  }

  if (min.has_value() && max.has_value() && min.value() == max.value()) {
    return testBytes(min->data(), min->length());
  }

  // if both min and max are within the negated range then reject
  if (!testBytes(min->data(), min->length()) &&
      !testBytes(max->data(), max->length())) {
    return false;
  }

  return true;
}

bool NegatedBytesValues::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }
  if (min.has_value() && max.has_value() && min.value() == max.value()) {
    return testBytes(min->data(), min->length());
  }
  // a range of strings will always contain at least one string not in a set
  return true;
}

namespace {
int32_t binarySearch(const std::vector<int64_t>& values, int64_t value) {
  auto it = std::lower_bound(values.begin(), values.end(), value);
  if (it == values.end() || *it != value) {
    return -std::distance(values.begin(), it) - 1;
  } else {
    return std::distance(values.begin(), it);
  }
}
} // namespace

std::unique_ptr<Filter> BigintMultiRange::clone(
    std::optional<bool> nullAllowed) const {
  std::vector<std::unique_ptr<BigintRange>> ranges;
  ranges.reserve(ranges_.size());
  for (auto& range : ranges_) {
    ranges.emplace_back(std::make_unique<BigintRange>(*range));
  }
  if (nullAllowed) {
    return std::make_unique<BigintMultiRange>(
        std::move(ranges), nullAllowed.value());
  } else {
    return std::make_unique<BigintMultiRange>(std::move(ranges), nullAllowed_);
  }
}

bool BigintMultiRange::testInt64(int64_t value) const {
  int32_t i = binarySearch(lowerBounds_, value);
  if (i >= 0) {
    return true;
  }
  int place = (-i) - 1;
  if (place == 0) {
    // Below first
    return false;
  }
  // When value did not hit a lower bound of a filter, test with the filter
  // before the place where value would be inserted.
  return ranges_[place - 1]->testInt64(value);
}

bool BigintMultiRange::testInt64Range(int64_t min, int64_t max, bool hasNull)
    const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  for (const auto& range : ranges_) {
    if (range->testInt64Range(min, max, hasNull)) {
      return true;
    }
  }

  return false;
}

std::unique_ptr<Filter> MultiRange::clone(
    std::optional<bool> nullAllowed) const {
  std::vector<std::unique_ptr<Filter>> filters;
  for (auto& filter : filters_) {
    filters.push_back(filter->clone());
  }

  if (nullAllowed) {
    return std::make_unique<MultiRange>(
        std::move(filters), nullAllowed.value(), nanAllowed_);
  } else {
    return std::make_unique<MultiRange>(
        std::move(filters), nullAllowed_, nanAllowed_);
  }
}

bool MultiRange::testDouble(double value) const {
  if (std::isnan(value)) {
    return nanAllowed_;
  }
  for (const auto& filter : filters_) {
    if (filter->testDouble(value)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testFloat(float value) const {
  if (std::isnan(value)) {
    return nanAllowed_;
  }
  for (const auto& filter : filters_) {
    if (filter->testFloat(value)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testBytes(const char* value, int32_t length) const {
  for (const auto& filter : filters_) {
    if (filter->testBytes(value, length)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testLength(int32_t length) const {
  for (const auto& filter : filters_) {
    if (filter->testLength(length)) {
      return true;
    }
  }
  return false;
}

bool MultiRange::testBytesRange(
    std::optional<std::string_view> min,
    std::optional<std::string_view> max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  for (const auto& filter : filters_) {
    if (filter->testBytesRange(min, max, hasNull)) {
      return true;
    }
  }

  return false;
}

bool MultiRange::testDoubleRange(double min, double max, bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  for (const auto& filter : filters_) {
    if (filter->testDoubleRange(min, max, hasNull)) {
      return true;
    }
  }

  return false;
}

std::unique_ptr<Filter> MultiRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    // Rules of MultiRange with IsNull/IsNotNull
    // 1. MultiRange(nullAllowed=true) AND IS NULL => IS NULL
    // 2. MultiRange(nullAllowed=true) AND IS NOT NULL =>
    // MultiRange(nullAllowed=false)
    // 3. MultiRange(nullAllowed=false) AND IS NULL
    // => ALWAYS FALSE
    // 4. MultiRange(nullAllowed=false) AND IS NOT NULL
    // =>MultiRange(nullAllowed=false)
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(/*nullAllowed=*/false);
    case FilterKind::kDoubleRange:
    case FilterKind::kFloatRange:
      // TODO: Implement
      VELOX_UNREACHABLE();
    case FilterKind::kBytesValues:
    case FilterKind::kNegatedBytesValues:
    case FilterKind::kBytesRange:
    case FilterKind::kMultiRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      bool bothNanAllowed = nanAllowed_;
      std::vector<const Filter*> otherFilters;

      if (other->kind() == FilterKind::kMultiRange) {
        auto multiRangeOther = static_cast<const MultiRange*>(other);
        for (auto const& filterOther : multiRangeOther->filters()) {
          otherFilters.emplace_back(filterOther.get());
        }
        bothNanAllowed = bothNanAllowed && multiRangeOther->nanAllowed();
      } else {
        otherFilters.emplace_back(other);
      }

      std::vector<std::string> byteValues;
      std::vector<std::unique_ptr<Filter>> merged;
      merged.reserve(this->filters().size() + otherFilters.size());

      for (auto const& filter : this->filters()) {
        for (auto const& filterOther : otherFilters) {
          auto innerMerged = filter->mergeWith(filterOther);
          switch (innerMerged->kind()) {
            case FilterKind::kAlwaysFalse:
            case FilterKind::kIsNull:
              continue;
            case FilterKind::kBytesValues: {
              auto mergedBytesValues =
                  static_cast<const BytesValues*>(innerMerged.get());
              byteValues.reserve(
                  byteValues.size() + mergedBytesValues->values().size());
              for (const auto& value : mergedBytesValues->values()) {
                byteValues.emplace_back(value);
              }
              break;
            }
            case FilterKind::kMultiRange: {
              auto innerMergedMulti =
                  static_cast<const MultiRange*>(innerMerged.get());
              merged.reserve(
                  merged.size() + innerMergedMulti->filters().size());
              for (int i = 0; i < innerMergedMulti->filters().size(); ++i) {
                merged.emplace_back(innerMergedMulti->filters()[i]->clone());
              }
              break;
            }
            default:
              merged.emplace_back(innerMerged.release());
          }
        }
      }

      if (!byteValues.empty()) {
        merged.emplace_back(std::make_unique<BytesValues>(
            std::move(byteValues), bothNullAllowed));
      }

      if (merged.empty()) {
        return nullOrFalse(bothNullAllowed);
      } else if (merged.size() == 1) {
        return merged.front()->clone(bothNullAllowed);
      } else {
        return std::make_unique<MultiRange>(
            std::move(merged), bothNullAllowed, bothNanAllowed);
      }
    }
    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> IsNull::mergeWith(const Filter* other) const {
  VELOX_CHECK(other->isDeterministic());

  if (other->testNull()) {
    return this->clone();
  }

  return std::make_unique<AlwaysFalse>();
}

std::unique_ptr<Filter> IsNotNull::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kIsNotNull:
      return this->clone();
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return std::make_unique<AlwaysFalse>();
    default:
      return other->mergeWith(this);
  }
}

std::unique_ptr<Filter> BoolValue::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<BoolValue>(value_, false);
    case FilterKind::kBoolValue: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      if (other->testBool(value_)) {
        return std::make_unique<BoolValue>(value_, bothNullAllowed);
      }

      return nullOrFalse(bothNullAllowed);
    }
    default:
      VELOX_UNREACHABLE();
  }
}

namespace {
std::unique_ptr<Filter> combineBigintRanges(
    std::vector<std::unique_ptr<BigintRange>> ranges,
    bool nullAllowed) {
  if (ranges.empty()) {
    return nullOrFalse(nullAllowed);
  }

  if (ranges.size() == 1) {
    return std::make_unique<BigintRange>(
        ranges.front()->lower(), ranges.front()->upper(), nullAllowed);
  }

  return std::make_unique<BigintMultiRange>(std::move(ranges), nullAllowed);
}

std::unique_ptr<BigintRange> toBigintRange(std::unique_ptr<Filter> filter) {
  return std::unique_ptr<BigintRange>(
      dynamic_cast<BigintRange*>(filter.release()));
}

// takes a sorted vector of ranges and a sorted vector of rejected values, and
// returns a range filter of values accepted by both filters
std::unique_ptr<Filter> combineRangesAndNegatedValues(
    const std::vector<std::unique_ptr<BigintRange>>& ranges,
    std::vector<int64_t>& rejects,
    bool nullAllowed) {
  std::vector<std::unique_ptr<BigintRange>> outRanges;

  for (int i = 0; i < ranges.size(); ++i) {
    auto it =
        std::lower_bound(rejects.begin(), rejects.end(), ranges[i]->lower());
    int64_t start = ranges[i]->lower();
    int64_t end;

    while (it != rejects.end()) {
      end = *it - 1;
      if (start >= ranges[i]->lower() && end < ranges[i]->upper()) {
        if (start <= end) {
          outRanges.emplace_back(
              std::make_unique<common::BigintRange>(start, end, false));
        }
        start = *it + 1;
        ++it;
      } else {
        break;
      }
    }
    end = ranges[i]->upper();
    if (start <= end && start >= ranges[i]->lower() &&
        end <= ranges[i]->upper()) {
      outRanges.emplace_back(
          std::make_unique<common::BigintRange>(start, end, false));
    }
  }

  return combineBigintRanges(std::move(outRanges), nullAllowed);
}

std::unique_ptr<Filter> combineNegatedBigintLists(
    const std::vector<int64_t>& first,
    const std::vector<int64_t>& second,
    bool nullAllowed) {
  std::vector<int64_t> allRejected;
  allRejected.reserve(first.size() + second.size());

  auto it1 = first.begin();
  auto it2 = second.begin();

  // merge first and second lists
  while (it1 != first.end() && it2 != second.end()) {
    int64_t lo = std::min(*it1, *it2);
    allRejected.emplace_back(lo);
    // remove duplicates
    if (lo == *it1) {
      ++it1;
    }
    if (lo == *it2) {
      ++it2;
    }
  }
  // fill in remaining values from each list
  while (it1 != first.end()) {
    allRejected.emplace_back(*it1);
    ++it1;
  }
  while (it2 != second.end()) {
    allRejected.emplace_back(*it2);
    ++it2;
  }
  return createNegatedBigintValues(allRejected, nullAllowed);
}

std::unique_ptr<Filter> combineNegatedRangeOnIntRanges(
    int64_t negatedLower,
    int64_t negatedUpper,
    const std::vector<std::unique_ptr<BigintRange>>& ranges,
    bool nullAllowed) {
  std::vector<std::unique_ptr<BigintRange>> outRanges;
  // for a sensible set of ranges, at most one creates 2 output ranges
  outRanges.reserve(ranges.size() + 1);
  for (int i = 0; i < ranges.size(); ++i) {
    if (negatedUpper < ranges[i]->lower() ||
        ranges[i]->upper() < negatedLower) {
      outRanges.emplace_back(std::make_unique<BigintRange>(
          ranges[i]->lower(), ranges[i]->upper(), false));
    } else {
      if (ranges[i]->lower() < negatedLower) {
        outRanges.emplace_back(std::make_unique<BigintRange>(
            ranges[i]->lower(), negatedLower - 1, false));
      }
      if (negatedUpper < ranges[i]->upper()) {
        outRanges.emplace_back(std::make_unique<BigintRange>(
            negatedUpper + 1, ranges[i]->upper(), false));
      }
    }
  }

  return combineBigintRanges(std::move(outRanges), nullAllowed);
}

std::vector<std::unique_ptr<BigintRange>> negatedValuesToRanges(
    std::vector<int64_t>& values) {
  VELOX_DCHECK(std::is_sorted(values.begin(), values.end()));
  auto front = ++(values.begin());
  auto back = values.begin();
  std::vector<std::unique_ptr<BigintRange>> res;
  res.reserve(values.size() + 1);
  if (*back > std::numeric_limits<int64_t>::min()) {
    res.emplace_back(std::make_unique<BigintRange>(
        std::numeric_limits<int64_t>::min(), *back - 1, false));
  }
  while (front != values.end()) {
    if (*back + 1 <= *front - 1) {
      res.emplace_back(
          std::make_unique<BigintRange>(*back + 1, *front - 1, false));
    }
    ++front;
    ++back;
  }
  if (*back < std::numeric_limits<int64_t>::max()) {
    res.emplace_back(std::make_unique<BigintRange>(
        *back + 1, std::numeric_limits<int64_t>::max(), false));
  }
  return res;
}

} // namespace

std::unique_ptr<Filter> BigintRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<BigintRange>(lower_, upper_, false);
    case FilterKind::kBigintRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();

      auto otherRange = static_cast<const BigintRange*>(other);

      auto lower = std::max(lower_, otherRange->lower_);
      auto upper = std::min(upper_, otherRange->upper_);

      if (lower <= upper) {
        return std::make_unique<BigintRange>(lower, upper, bothNullAllowed);
      }

      return nullOrFalse(bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kBigintValuesUsingBitmask:
    case FilterKind::kBigintValuesUsingHashTable:
      return other->mergeWith(this);
    case FilterKind::kBigintMultiRange: {
      auto otherMultiRange = dynamic_cast<const BigintMultiRange*>(other);
      std::vector<std::unique_ptr<BigintRange>> newRanges;
      for (const auto& range : otherMultiRange->ranges()) {
        auto merged = this->mergeWith(range.get());
        if (merged->kind() == FilterKind::kBigintRange) {
          newRanges.push_back(toBigintRange(std::move(merged)));
        } else {
          VELOX_CHECK(merged->kind() == FilterKind::kAlwaysFalse);
        }
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return combineBigintRanges(std::move(newRanges), bothNullAllowed);
    }
    case FilterKind::kNegatedBigintValuesUsingBitmask:
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      if (!other->testInt64Range(lower_, upper_, false)) {
        return nullOrFalse(bothNullAllowed);
      }
      std::vector<int64_t> vals;
      if (other->kind() == FilterKind::kNegatedBigintValuesUsingBitmask) {
        auto otherNegated =
            dynamic_cast<const NegatedBigintValuesUsingBitmask*>(other);
        vals = otherNegated->values();
      } else {
        auto otherNegated =
            dynamic_cast<const NegatedBigintValuesUsingHashTable*>(other);
        vals = otherNegated->values();
      }
      std::vector<std::unique_ptr<common::BigintRange>> rangeList;
      rangeList.emplace_back(
          std::make_unique<common::BigintRange>(lower_, upper_, false));
      return combineRangesAndNegatedValues(rangeList, vals, bothNullAllowed);
    }
    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedBigintRange::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kBigintRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherRange = static_cast<const BigintRange*>(other);
      std::vector<std::unique_ptr<common::BigintRange>> rangeList;
      rangeList.emplace_back(std::make_unique<BigintRange>(
          otherRange->lower(), otherRange->upper(), false));
      return combineNegatedRangeOnIntRanges(
          this->lower(), this->upper(), rangeList, bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherNegatedRange = static_cast<const NegatedBigintRange*>(other);
      if (this->lower() > otherNegatedRange->lower()) {
        return other->mergeWith(this);
      }
      assert(this->lower() <= otherNegatedRange->lower());
      if (this->upper() + 1 < otherNegatedRange->lower()) {
        std::vector<std::unique_ptr<common::BigintRange>> outRanges;
        int64_t smallLower = this->lower();
        int64_t smallUpper = this->upper();
        int64_t bigLower = otherNegatedRange->lower();
        int64_t bigUpper = otherNegatedRange->upper();
        if (smallLower > std::numeric_limits<int64_t>::min()) {
          outRanges.emplace_back(std::make_unique<common::BigintRange>(
              std::numeric_limits<int64_t>::min(), smallLower - 1, false));
        }
        if (smallUpper < std::numeric_limits<int64_t>::max() &&
            bigLower > std::numeric_limits<int64_t>::min()) {
          outRanges.emplace_back(std::make_unique<common::BigintRange>(
              smallUpper + 1, bigLower - 1, false));
        }
        if (bigUpper < std::numeric_limits<int64_t>::max()) {
          outRanges.emplace_back(std::make_unique<common::BigintRange>(
              bigUpper + 1, std::numeric_limits<int64_t>::max(), false));
        }
        return combineBigintRanges(std::move(outRanges), bothNullAllowed);
      }
      return std::make_unique<common::NegatedBigintRange>(
          this->lower(),
          std::max<int64_t>(this->upper(), otherNegatedRange->upper()),
          bothNullAllowed);
    }
    case FilterKind::kBigintMultiRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherMultiRanges = static_cast<const BigintMultiRange*>(other);
      return combineNegatedRangeOnIntRanges(
          this->lower(),
          this->upper(),
          otherMultiRanges->ranges(),
          bothNullAllowed);
    }
    case FilterKind::kBigintValuesUsingHashTable:
    case FilterKind::kBigintValuesUsingBitmask:
      return other->mergeWith(this);
    case FilterKind::kNegatedBigintValuesUsingHashTable:
    case FilterKind::kNegatedBigintValuesUsingBitmask: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      std::vector<int64_t> rejectedValues;
      if (other->kind() == FilterKind::kNegatedBigintValuesUsingHashTable) {
        auto otherHashTable =
            static_cast<const NegatedBigintValuesUsingHashTable*>(other);
        rejectedValues = otherHashTable->values();
      } else {
        auto otherBitmask =
            static_cast<const NegatedBigintValuesUsingBitmask*>(other);
        rejectedValues = otherBitmask->values();
      }
      if (nonNegated_->isSingleValue()) {
        if (other->testInt64(this->lower())) {
          rejectedValues.push_back(this->lower());
        }
        return createNegatedBigintValues(rejectedValues, bothNullAllowed);
      }
      return combineNegatedRangeOnIntRanges(
          this->lower(),
          this->upper(),
          negatedValuesToRanges(rejectedValues),
          bothNullAllowed);
    }
    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BigintValuesUsingHashTable::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<BigintValuesUsingHashTable>(*this, false);
    case FilterKind::kBigintRange: {
      auto otherRange = dynamic_cast<const BigintRange*>(other);
      auto min = std::max(min_, otherRange->lower());
      auto max = std::min(max_, otherRange->upper());

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintValuesUsingHashTable: {
      auto otherValues = dynamic_cast<const BigintValuesUsingHashTable*>(other);
      auto min = std::max(min_, otherValues->min());
      auto max = std::min(max_, otherValues->max());

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintValuesUsingBitmask:
      return other->mergeWith(this);
    case FilterKind::kBigintMultiRange: {
      auto otherMultiRange = dynamic_cast<const BigintMultiRange*>(other);

      std::vector<int64_t> valuesToKeep;
      if (containsEmptyMarker_ && other->testInt64(kEmptyMarker)) {
        valuesToKeep.emplace_back(kEmptyMarker);
      }
      for (const auto& range : otherMultiRange->ranges()) {
        auto min = std::max(min_, range->lower());
        auto max = std::min(max_, range->upper());

        if (min <= max) {
          for (int64_t v : values_) {
            if (range->testInt64(v)) {
              valuesToKeep.emplace_back(v);
            }
          }
        }
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return createBigintValues(valuesToKeep, bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kNegatedBigintValuesUsingBitmask:
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      return mergeWith(min_, max_, other);
    }
    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BigintValuesUsingHashTable::mergeWith(
    int64_t min,
    int64_t max,
    const Filter* other) const {
  bool bothNullAllowed = nullAllowed_ && other->testNull();

  if (max < min) {
    return nullOrFalse(bothNullAllowed);
  }

  if (max == min) {
    if (testInt64(min) && other->testInt64(min)) {
      return std::make_unique<BigintRange>(min, min, bothNullAllowed);
    }

    return nullOrFalse(bothNullAllowed);
  }

  std::vector<int64_t> valuesToKeep;
  valuesToKeep.reserve(values_.size());
  if (containsEmptyMarker_ && other->testInt64(kEmptyMarker)) {
    valuesToKeep.emplace_back(kEmptyMarker);
  }

  for (int64_t v : hashTable_) {
    if (v != kEmptyMarker && other->testInt64(v)) {
      valuesToKeep.emplace_back(v);
    }
  }

  return createBigintValues(valuesToKeep, bothNullAllowed);
}

std::unique_ptr<Filter> BigintValuesUsingBitmask::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<BigintValuesUsingBitmask>(*this, false);
    case FilterKind::kBigintRange: {
      auto otherRange = dynamic_cast<const BigintRange*>(other);

      auto min = std::max(min_, otherRange->lower());
      auto max = std::min(max_, otherRange->upper());

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintValuesUsingHashTable: {
      auto otherValues = dynamic_cast<const BigintValuesUsingHashTable*>(other);

      auto min = std::max(min_, otherValues->min());
      auto max = std::min(max_, otherValues->max());

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintValuesUsingBitmask: {
      auto otherValues = dynamic_cast<const BigintValuesUsingBitmask*>(other);

      auto min = std::max(min_, otherValues->min_);
      auto max = std::min(max_, otherValues->max_);

      return mergeWith(min, max, other);
    }
    case FilterKind::kBigintMultiRange: {
      auto otherMultiRange = dynamic_cast<const BigintMultiRange*>(other);

      std::vector<int64_t> valuesToKeep;
      for (const auto& range : otherMultiRange->ranges()) {
        auto min = std::max(min_, range->lower());
        auto max = std::min(max_, range->upper());
        for (auto i = min; i <= max; ++i) {
          if (bitmask_[i - min_] && range->testInt64(i)) {
            valuesToKeep.push_back(i);
          }
        }
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return createBigintValues(valuesToKeep, bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kNegatedBigintValuesUsingBitmask:
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      return mergeWith(min_, max_, other);
    }
    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BigintValuesUsingBitmask::mergeWith(
    int64_t min,
    int64_t max,
    const Filter* other) const {
  bool bothNullAllowed = nullAllowed_ && other->testNull();

  std::vector<int64_t> valuesToKeep;
  for (auto i = min; i <= max; ++i) {
    if (bitmask_[i - min_] && other->testInt64(i)) {
      valuesToKeep.push_back(i);
    }
  }
  return createBigintValues(valuesToKeep, bothNullAllowed);
}

std::unique_ptr<Filter> NegatedBigintValuesUsingHashTable::mergeWith(
    const Filter* other) const {
  // Rules of NegatedBigintValuesUsingHashTable with IsNull/IsNotNull
  // 1. Negated...(nullAllowed=true) AND IS NULL => IS NULL
  // 2. Negated...(nullAllowed=true) AND IS NOT NULL =>
  // Negated...(nullAllowed=false)
  // 3. Negated...(nullAllowed=false) AND IS NULL
  // => ALWAYS FALSE
  // 4. Negated...(nullAllowed=false) AND IS NOT NULL
  // =>Negated...(nullAllowed=false)
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<NegatedBigintValuesUsingHashTable>(*this, false);
    case FilterKind::kBigintValuesUsingHashTable:
    case FilterKind::kBigintValuesUsingBitmask:
    case FilterKind::kBigintRange:
    case FilterKind::kBigintMultiRange: {
      return other->mergeWith(this);
    }
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      auto otherNegated =
          static_cast<const NegatedBigintValuesUsingHashTable*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return combineNegatedBigintLists(
          values(), otherNegated->values(), bothNullAllowed);
    }
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kNegatedBigintValuesUsingBitmask: {
      return other->mergeWith(this);
    }
    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedBigintValuesUsingBitmask::mergeWith(
    const Filter* other) const {
  // Rules of NegatedBigintValuesUsingBitmask with IsNull/IsNotNull
  // 1. Negated...(nullAllowed=true) AND IS NULL => IS NULL
  // 2. Negated...(nullAllowed=true) AND IS NOT NULL =>
  // Negated...(nullAllowed=false)
  // 3. Negated...(nullAllowed=false) AND IS NULL
  // => ALWAYS FALSE
  // 4. Negated...(nullAllowed=false) AND IS NOT NULL
  // =>Negated...(nullAllowed=false)
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<NegatedBigintValuesUsingBitmask>(*this, false);
    case FilterKind::kBigintValuesUsingHashTable:
    case FilterKind::kBigintValuesUsingBitmask:
    case FilterKind::kBigintRange:
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kBigintMultiRange: {
      return other->mergeWith(this);
    }
    case FilterKind::kNegatedBigintValuesUsingHashTable: {
      auto otherHashTable =
          dynamic_cast<const NegatedBigintValuesUsingHashTable*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      // kEmptyMarker is already in values for a bitmask
      return combineNegatedBigintLists(
          values(), otherHashTable->values(), bothNullAllowed);
    }
    case FilterKind::kNegatedBigintValuesUsingBitmask: {
      auto otherBitmask =
          dynamic_cast<const NegatedBigintValuesUsingBitmask*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return combineNegatedBigintLists(
          values(), otherBitmask->values(), bothNullAllowed);
    }
    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> BigintMultiRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull: {
      std::vector<std::unique_ptr<BigintRange>> ranges;
      ranges.reserve(ranges_.size());
      for (auto& range : ranges_) {
        ranges.push_back(std::make_unique<BigintRange>(*range));
      }
      return std::make_unique<BigintMultiRange>(std::move(ranges), false);
    }
    case FilterKind::kBigintRange:
    case FilterKind::kNegatedBigintRange:
    case FilterKind::kBigintValuesUsingBitmask:
    case FilterKind::kBigintValuesUsingHashTable: {
      return other->mergeWith(this);
    }
    case FilterKind::kBigintMultiRange: {
      std::vector<std::unique_ptr<BigintRange>> newRanges;
      for (const auto& range : ranges_) {
        auto merged = range->mergeWith(other);
        if (merged->kind() == FilterKind::kBigintRange) {
          newRanges.push_back(toBigintRange(std::move(merged)));
        } else if (merged->kind() == FilterKind::kBigintMultiRange) {
          auto mergedMultiRange = dynamic_cast<BigintMultiRange*>(merged.get());
          for (const auto& newRange : mergedMultiRange->ranges_) {
            newRanges.push_back(toBigintRange(newRange->clone()));
          }
        } else {
          VELOX_CHECK(merged->kind() == FilterKind::kAlwaysFalse);
        }
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      if (newRanges.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      if (newRanges.size() == 1) {
        return std::make_unique<BigintRange>(
            newRanges.front()->lower(),
            newRanges.front()->upper(),
            bothNullAllowed);
      }

      return std::make_unique<BigintMultiRange>(
          std::move(newRanges), bothNullAllowed);
    }
    case FilterKind::kNegatedBigintValuesUsingHashTable:
    case FilterKind::kNegatedBigintValuesUsingBitmask: {
      std::vector<std::unique_ptr<BigintRange>> newRanges;
      std::vector<int64_t> rejects;
      if (other->kind() == FilterKind::kNegatedBigintValuesUsingBitmask) {
        auto otherNegated =
            dynamic_cast<const NegatedBigintValuesUsingBitmask*>(other);
        rejects = otherNegated->values();
      } else {
        auto otherNegated =
            dynamic_cast<const NegatedBigintValuesUsingHashTable*>(other);
        rejects = otherNegated->values();
      }

      bool bothNullAllowed = nullAllowed_ && other->testNull();
      return combineRangesAndNegatedValues(ranges_, rejects, bothNullAllowed);
    }
    default:
      VELOX_UNREACHABLE();
  }
}

namespace {
// compareResult = left < right for upper, right < left for lower
bool mergeExclusive(int compareResult, bool left, bool right) {
  return compareResult == 0 ? (left || right)
                            : (compareResult < 0 ? left : right);
}
} // namespace

std::unique_ptr<Filter> BytesRange::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kBytesValues:
    case FilterKind::kNegatedBytesValues:
    case FilterKind::kMultiRange:
      return other->mergeWith(this);
    case FilterKind::kBytesRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();

      auto otherRange = static_cast<const BytesRange*>(other);

      bool upperUnbounded = false;
      bool lowerUnbounded = false;
      bool upperExclusive = false;
      bool lowerExclusive = false;
      std::string upper = "";
      std::string lower = "";

      if (lowerUnbounded_) {
        lowerUnbounded = otherRange->lowerUnbounded_;
        lowerExclusive = otherRange->lowerExclusive_;
        lower = otherRange->lower_;
      } else if (otherRange->lowerUnbounded_) {
        lowerUnbounded = lowerUnbounded_;
        lowerExclusive = lowerExclusive_;
        lower = lower_;
      } else {
        lowerUnbounded = false;
        auto compare = lower_.compare(otherRange->lower_);
        lower = compare < 0 ? otherRange->lower_ : lower_;
        lowerExclusive = mergeExclusive(
            -compare, lowerExclusive_, otherRange->lowerExclusive_);
      }

      if (upperUnbounded_) {
        upperUnbounded = otherRange->upperUnbounded_;
        upperExclusive = otherRange->upperExclusive_;
        upper = otherRange->upper_;
      } else if (otherRange->upperUnbounded_) {
        upperUnbounded = upperUnbounded_;
        upperExclusive = upperExclusive_;
        upper = upper_;
      } else {
        upperUnbounded = false;
        auto compare = upper_.compare(otherRange->upper_);
        upper = compare < 0 ? upper_ : otherRange->upper_;
        upperExclusive = mergeExclusive(
            compare, upperExclusive_, otherRange->upperExclusive_);
      }

      if (!lowerUnbounded && !upperUnbounded &&
          (lower > upper ||
           (lower == upper && (lowerExclusive || upperExclusive)))) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesRange>(
          lower,
          lowerUnbounded,
          lowerExclusive,
          upper,
          upperUnbounded,
          upperExclusive,
          bothNullAllowed);
    }

    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedBytesRange::mergeWith(
    const Filter* other) const {
  VELOX_NYI("MergeWith is not yet implemented for a NegatedBytesRange.");
}

std::unique_ptr<Filter> BytesValues::mergeWith(const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kMultiRange:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kBytesValues: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto otherBytesValues = static_cast<const BytesValues*>(other);

      if (this->upper_.compare(otherBytesValues->lower_) < 0 ||
          otherBytesValues->upper_.compare(this->lower_) < 0) {
        return nullOrFalse(bothNullAllowed);
      }
      const BytesValues* smallerFilter = this;
      const BytesValues* largerFilter = otherBytesValues;
      if (this->values().size() > otherBytesValues->values().size()) {
        smallerFilter = otherBytesValues;
        largerFilter = this;
      }

      std::vector<std::string> newValues;
      newValues.reserve(smallerFilter->values().size());

      for (const auto& value : smallerFilter->values()) {
        if (largerFilter->values_.contains(value)) {
          newValues.emplace_back(value);
        }
      }

      if (newValues.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesValues>(
          std::move(newValues), bothNullAllowed);
    }
    case FilterKind::kNegatedBytesValues: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      std::vector<std::string> newValues;
      newValues.reserve(values().size());
      for (const auto& value : values()) {
        if (other->testBytes(value.data(), value.length())) {
          newValues.emplace_back(value);
        }
      }

      if (newValues.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesValues>(
          std::move(newValues), bothNullAllowed);
    }
    case FilterKind::kBytesRange: {
      auto otherBytesRange = static_cast<const BytesRange*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();

      if (!testBytesRange(
              otherBytesRange->isLowerUnbounded()
                  ? std::nullopt
                  : std::optional(otherBytesRange->lower()),
              otherBytesRange->isUpperUnbounded()
                  ? std::nullopt
                  : std::optional(otherBytesRange->upper()),
              bothNullAllowed)) {
        return nullOrFalse(bothNullAllowed);
      }

      std::vector<std::string> newValues;
      newValues.reserve(this->values().size());
      for (const auto& value : this->values()) {
        if (otherBytesRange->testBytes(value.data(), value.length())) {
          newValues.emplace_back(value);
        }
      }

      if (newValues.empty()) {
        return nullOrFalse(bothNullAllowed);
      }

      return std::make_unique<BytesValues>(
          std::move(newValues), bothNullAllowed);
    }

    default:
      VELOX_UNREACHABLE();
  }
}

std::unique_ptr<Filter> NegatedBytesValues::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
    case FilterKind::kBytesValues:
    case FilterKind::kMultiRange:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return this->clone(false);
    case FilterKind::kNegatedBytesValues: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto negatedBytesOther = static_cast<const NegatedBytesValues*>(other);
      if (values().size() < negatedBytesOther->values().size()) {
        return other->mergeWith(this);
      }
      std::vector<std::string> rejectedValues;
      rejectedValues.reserve(
          values().size() + negatedBytesOther->values().size());
      rejectedValues.insert(
          rejectedValues.begin(), values().begin(), values().end());
      for (auto value : negatedBytesOther->values()) {
        // put in all values rejected by this filter that pass the other one
        if (testBytes(value.data(), value.length())) {
          rejectedValues.emplace_back(value);
        }
      }
      return std::make_unique<NegatedBytesValues>(
          std::move(rejectedValues), bothNullAllowed);
    }
    case FilterKind::kBytesRange: {
      auto bytesRangeOther = static_cast<const BytesRange*>(other);
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      // ordered set of values in the range that are rejected
      std::set<std::string> rejectedValues;
      for (const auto& value : values()) {
        if (other->testBytes(value.data(), value.length())) {
          rejectedValues.insert(value);
        }
      }
      // edge checks - if an inclusive endpoint is negated, just make exclusive
      // std::set contains is C++ 20, so we use count instead :(
      bool loExclusive = !bytesRangeOther->lowerUnbounded() &&
          (bytesRangeOther->lowerExclusive() ||
           rejectedValues.count(bytesRangeOther->lower()) > 0);
      if (!bytesRangeOther->lowerUnbounded()) {
        rejectedValues.erase(bytesRangeOther->lower());
      }
      bool hiExclusive = !bytesRangeOther->upperUnbounded() &&
          (bytesRangeOther->upperExclusive() ||
           rejectedValues.count(bytesRangeOther->upper()) > 0);
      if (!bytesRangeOther->upperUnbounded()) {
        rejectedValues.erase(bytesRangeOther->upper());
      }
      if (rejectedValues.empty()) {
        return std::make_unique<BytesRange>(
            bytesRangeOther->lower(),
            bytesRangeOther->lowerUnbounded(),
            loExclusive,
            bytesRangeOther->upper(),
            bytesRangeOther->upperUnbounded(),
            hiExclusive,
            bothNullAllowed);
      }

      // accumulate filters in a vector here
      std::vector<std::unique_ptr<Filter>> ranges;
      ranges.reserve(rejectedValues.size() + 1);
      auto back = rejectedValues.begin();
      auto front = ++(rejectedValues.begin());
      ranges.emplace_back(std::make_unique<BytesRange>(
          bytesRangeOther->lower(),
          bytesRangeOther->lowerUnbounded(),
          loExclusive,
          *back,
          false, // not unbounded
          true, // exclusive
          false));
      while (front != rejectedValues.end()) {
        ranges.emplace_back(std::make_unique<BytesRange>(
            *back, false, true, *front, false, true, false));
        ++front;
        ++back;
      }
      ranges.emplace_back(std::make_unique<BytesRange>(
          *back,
          false,
          true,
          bytesRangeOther->upper(),
          bytesRangeOther->upperUnbounded(),
          hiExclusive,
          false));
      return std::make_unique<MultiRange>(
          std::move(ranges), bothNullAllowed, false);
    }
    default:
      VELOX_UNREACHABLE();
  }
}
} // namespace facebook::velox::common
