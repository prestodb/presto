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
      max_(max) {
  VELOX_CHECK(min < max, "min must be less than max");
  VELOX_CHECK(values.size() > 1, "values must contain at least 2 entries");

  auto size = 1u << (uint32_t)std::log2(values.size() * 3);
  hashTable_.resize(size);
  for (auto i = 0; i < size; ++i) {
    hashTable_[i] = kEmptyMarker;
  }
  for (auto value : values) {
    if (value == kEmptyMarker) {
      containsEmptyMarker_ = true;
    } else {
      auto position = ((value * M) & (size - 1));
      for (auto i = position; i < position + size; i++) {
        uint32_t index = i & (size - 1);
        if (hashTable_[index] == kEmptyMarker) {
          hashTable_[index] = value;
          break;
        }
      }
    }
  }
}

bool BigintValuesUsingHashTable::testInt64(int64_t value) const {
  if (containsEmptyMarker_ && value == kEmptyMarker) {
    return true;
  }
  if (value < min_ || value > max_) {
    return false;
  }
  uint32_t size = hashTable_.size();
  uint32_t pos = (value * M) & (size - 1);
  for (auto i = pos; i < pos + size; i++) {
    int32_t idx = i & (size - 1);
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

  return !(min > max_ || max < min_);
}

std::unique_ptr<Filter> createBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed) {
  VELOX_CHECK(values.size() > 1);
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

std::unique_ptr<Filter> BigintMultiRange::clone() const {
  std::vector<std::unique_ptr<BigintRange>> ranges;
  for (auto& range : ranges_) {
    ranges.push_back(std::make_unique<BigintRange>(*range));
  }
  return std::make_unique<BigintMultiRange>(std::move(ranges), nullAllowed_);
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

std::unique_ptr<Filter> MultiRange::clone() const {
  std::vector<std::unique_ptr<Filter>> filters;
  for (auto& filter : filters_) {
    filters.push_back(filter->clone());
  }
  return std::make_unique<MultiRange>(
      std::move(filters), nullAllowed_, nanAllowed_);
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
} // namespace facebook::velox::common
