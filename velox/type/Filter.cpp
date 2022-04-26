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
    case FilterKind::kBigintValuesUsingHashTable:
      strKind = "BigintValuesUsingHashTable";
      break;
    case FilterKind::kBigintValuesUsingBitmask:
      strKind = "BigintValuesUsingBitmask";
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
    case FilterKind::kBytesValues:
      strKind = "BytesValues";
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

__m256i BigintValuesUsingHashTable::test4x64(__m256i x) {
  using V64 = simd::Vectors<int64_t>;
  auto rangeMask = V64::compareGt(V64::setAll(min_), x) |
      V64::compareGt(x, V64::setAll(max_));
  if (V64::compareBitMask(rangeMask) == V64::kAllTrue) {
    return V64::setAll(0);
  }
  if (containsEmptyMarker_) {
    return Filter::test4x64(x);
  }
  rangeMask ^= -1;
  auto indices = x * M & sizeMask_;
  __m256i data = _mm256_mask_i64gather_epi64(
      V64::setAll(kEmptyMarker),
      reinterpret_cast<const long long int*>(hashTable_.data()),
      indices,
      rangeMask,
      8);
  // The lanes with kEmptyMarker missed, the lanes matching x hit and the other
  // lanes must check next positions.

  auto result = V64::compareEq(x, data);
  auto missed = V64::compareEq(data, V64::setAll(kEmptyMarker));
  uint16_t unresolved = V64::kAllTrue ^ V64::compareBitMask(result | missed);
  if (!unresolved) {
    return result;
  }
  int64_t indicesArray[4];
  int64_t valuesArray[4];
  int64_t resultArray[4];
  *reinterpret_cast<V64::TV*>(indicesArray) = indices + 1;
  *reinterpret_cast<V64::TV*>(valuesArray) = x;
  *reinterpret_cast<V64::TV*>(resultArray) = result;
  auto allEmpty = V64::setAll(kEmptyMarker);
  while (unresolved) {
    auto lane = bits::getAndClearLastSetBit(unresolved);
    // Loop for each unresolved (not hit and
    // not empty) until finding hit or empty.
    int64_t index = indicesArray[lane];
    int64_t value = valuesArray[lane];
    auto allValue = V64::setAll(value);
    for (;;) {
      auto line = V64::load(hashTable_.data() + index);

      if (V64::compareBitMask(V64::compareEq(line, allValue))) {
        resultArray[lane] = -1;
        break;
      }
      if (V64::compareBitMask(V64::compareEq(line, allEmpty))) {
        resultArray[lane] = 0;
        break;
      }
      index += 4;
      if (index > sizeMask_) {
        index = 0;
      }
    }
  }
  return V64::load(&resultArray);
}

__m256si BigintValuesUsingHashTable::test8x32(__m256i x) {
  // Calls 4x64 twice since the hash table is 64 bits wide in any
  // case. A 32-bit hash table would be possible but all the use
  // cases seen are in the 64 bit range.
  using V32 = simd::Vectors<int32_t>;
  using V64 = simd::Vectors<int64_t>;
  auto x8x32 = reinterpret_cast<V32::TV>(x);
  auto first = V64::compareBitMask(test4x64(V32::as4x64<0>(x8x32)));
  auto second = V64::compareBitMask(test4x64(V32::as4x64<1>(x8x32)));
  return V32::mask(first | (second << 4));
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

namespace {
std::unique_ptr<Filter> nullOrFalse(bool nullAllowed) {
  if (nullAllowed) {
    return std::make_unique<IsNull>();
  }
  return std::make_unique<AlwaysFalse>();
}
} // namespace

std::unique_ptr<Filter> createBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed) {
  if (values.empty()) {
    return nullOrFalse(nullAllowed);
  }

  if (values.size() == 1) {
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
    if (range + 1 == values.size()) {
      return std::make_unique<BigintRange>(min, max, nullAllowed);
    }

    if (range < 32 * 64 || range < values.size() * 4 * 64) {
      return std::make_unique<BigintValuesUsingBitmask>(
          min, max, values, nullAllowed);
    }
  }
  return std::make_unique<BigintValuesUsingHashTable>(
      min, max, values, nullAllowed);
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

} // namespace facebook::velox::common
