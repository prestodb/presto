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

#include <algorithm>
#include <cmath>
#include <sstream>
#include <string>
#include <vector>

#include <folly/Range.h>
#include <folly/container/F14Set.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/type/StringView.h"

namespace facebook::velox::common {

enum class FilterKind {
  kAlwaysFalse,
  kAlwaysTrue,
  kIsNull,
  kIsNotNull,
  kBoolValue,
  kBigintRange,
  kBigintValuesUsingHashTable,
  kBigintValuesUsingBitmask,
  kNegatedBigintValuesUsingHashTable,
  kNegatedBigintValuesUsingBitmask,
  kDoubleRange,
  kFloatRange,
  kBytesRange,
  kBytesValues,
  kNegatedBytesValues,
  kBigintMultiRange,
  kMultiRange,
};

/**
 * A simple filter (e.g. comparison with literal) that can be applied
 * efficiently while extracting values from an ORC stream.
 */
class Filter {
 protected:
  Filter(bool deterministic, bool nullAllowed, FilterKind kind)
      : nullAllowed_(nullAllowed), deterministic_(deterministic), kind_(kind) {}

 public:
  virtual ~Filter() = default;

  // Templates parametrized on filter need to know determinism at compile
  // time. If this is false, deterministic() will be consulted at
  // runtime.
  static constexpr bool deterministic = true;

  FilterKind kind() const {
    return kind_;
  }

  /// Return a copy of this filter. If nullAllowed is set, modified the
  /// nullAllowed flag in the copy to match.
  virtual std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const = 0;

  /**
   * A filter becomes non-deterministic when applies to nested column,
   * e.g. a[1] > 10 is non-deterministic because > 10 filter applies only to
   * some positions, e.g. first entry in a set of entries that correspond to a
   * single top-level position.
   */
  virtual bool isDeterministic() const {
    return deterministic_;
  }

  /**
   * When a filter applied to a nested column fails, the whole top-level
   * position should fail. To enable this functionality, the filter keeps track
   * of the boundaries of top-level positions and allows the caller to find out
   * where the current top-level position started and how far it continues.
   * @return number of positions from the start of the current top-level
   * position up to the current position (excluding current position)
   */
  virtual int getPrecedingPositionsToFail() const {
    return 0;
  }

  /**
   * @return number of positions remaining until the end of the current
   * top-level position
   */
  virtual int getSucceedingPositionsToFail() const {
    return 0;
  }

  virtual bool testNull() const {
    return nullAllowed_;
  }

  /**
   * Used to apply is [not] null filters to complex types, e.g.
   * a[1] is null AND a[3] is not null, where a is an array(array(T)).
   *
   * In these case, the exact values are not known, but it is known whether they
   * are null or not. Furthermore, for some positions only nulls are allowed
   * (a[1] is null), for others only non-nulls (a[3] is not null), and for the
   * rest both are allowed (a[2] and a[N], where N > 3).
   */
  virtual bool testNonNull() const {
    VELOX_UNSUPPORTED("{}: testNonNull() is not supported.", toString());
  }

  virtual bool testInt64(int64_t /* unused */) const {
    VELOX_UNSUPPORTED("{}: testInt64() is not supported.", toString());
  }

  virtual bool testDouble(double /* unused */) const {
    VELOX_UNSUPPORTED("{}: testDouble() is not supported.", toString());
  }

  virtual bool testFloat(float /* unused */) const {
    VELOX_UNSUPPORTED("{}: testFloat() is not supported.", toString());
  }

  virtual xsimd::batch_bool<int64_t> testValues(xsimd::batch<int64_t> x) const {
    return genericTestValues(x, [this](int64_t x) { return testInt64(x); });
  }

  virtual xsimd::batch_bool<int32_t> testValues(xsimd::batch<int32_t> x) const {
    return genericTestValues(x, [this](int32_t x) { return testInt64(x); });
  }

  virtual xsimd::batch_bool<int16_t> testValues(xsimd::batch<int16_t> x) const {
    return genericTestValues(x, [this](int16_t x) { return testInt64(x); });
  }

  virtual xsimd::batch_bool<double> testValues(xsimd::batch<double> x) const {
    return genericTestValues(x, [this](double x) { return testDouble(x); });
  }

  virtual xsimd::batch_bool<float> testValues(xsimd::batch<float> x) const {
    return genericTestValues(x, [this](float x) { return testFloat(x); });
  }

  virtual bool testBool(bool /* unused */) const {
    VELOX_UNSUPPORTED("{}: testBool() is not supported.", toString());
  }

  virtual bool testBytes(const char* /* unused */, int32_t /* unused */) const {
    VELOX_UNSUPPORTED("{}: testBytes() is not supported.", toString());
  }

  // Returns true if it is useful to call testLength before other
  // tests. This should be true for string IN and equals because it is
  // possible to fail these based on the length alone. This would
  // typically be false of string ranges because these cannot be
  // generally decided without looking at the string itself.
  virtual bool hasTestLength() const {
    return false;
  }

  /**
   * Filters like string equality and IN, as well as conditions on cardinality
   * of lists and maps can be at least partly decided by looking at lengths
   * alone. If this is false, then no further checks are needed. If true,
   * eventual filters on the data itself need to be evaluated.
   */
  virtual bool testLength(int32_t /* unused */) const {
    VELOX_UNSUPPORTED("{}: testLength() is not supported.", toString());
  }

  // Tests multiple lengths at a time.
  virtual xsimd::batch_bool<int32_t> testLengths(
      xsimd::batch<int32_t> lengths) const {
    return genericTestValues(
        lengths, [this](int32_t x) { return testLength(x); });
  }

  // Returns true if at least one value in the specified range can pass the
  // filter. The range is defined as all values between min and max inclusive
  // plus null if hasNull is true.
  virtual bool
  testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/) const {
    VELOX_UNSUPPORTED("{}: testInt64Range() is not supported.", toString());
  }

  // Returns true if at least one value in the specified range can pass the
  // filter. The range is defined as all values between min and max inclusive
  // plus null if hasNull is true.
  virtual bool testDoubleRange(double /*min*/, double /*max*/, bool /*hasNull*/)
      const {
    VELOX_UNSUPPORTED("{}: testDoubleRange() is not supported.", toString());
  }

  virtual bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool /*hasNull*/) const {
    VELOX_UNSUPPORTED("{}: testBytesRange() is not supported.", toString());
  }

  // Combines this filter with another filter using 'AND' logic.
  virtual std::unique_ptr<Filter> mergeWith(const Filter* /*other*/) const {
    VELOX_UNSUPPORTED("{}: mergeWith() is not supported.", toString());
  }

  virtual std::string toString() const;

 protected:
  const bool nullAllowed_;

 private:
  const bool deterministic_;
  const FilterKind kind_;

  template <typename T, typename F>
  xsimd::batch_bool<T> genericTestValues(xsimd::batch<T> batch, F&& testValue)
      const {
    constexpr int N = decltype(batch)::size;
    constexpr int kAlign = decltype(batch)::arch_type::alignment();
    alignas(kAlign) T data[N];
    alignas(kAlign) T res[N];
    batch.store_aligned(data);
    for (int i = 0; i < N; ++i) {
      res[i] = testValue(data[i]);
    }
    return xsimd::broadcast<T>(0) != xsimd::load_aligned(res);
  }
};

/// TODO Check if this filter is needed. This should not be passed down.
class AlwaysFalse final : public Filter {
 public:
  AlwaysFalse() : Filter(true, false, FilterKind::kAlwaysFalse) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<AlwaysFalse>();
  }

  bool testNonNull() const final {
    return false;
  }

  bool testInt64(int64_t /* unused */) const final {
    return false;
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/)
      const final {
    return false;
  }

  bool testDouble(double /* unused */) const final {
    return false;
  }

  bool testFloat(float /* unused */) const final {
    return false;
  }

  bool testBool(bool /* unused */) const final {
    return false;
  }

  bool testBytes(const char* /* unused */, int32_t /* unused */) const final {
    return false;
  }

  bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool /*hasNull*/) const final {
    return false;
  }

  bool testLength(int32_t /* unused */) const final {
    return false;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* /*other*/) const final {
    // false AND <any> is false.
    return this->clone();
  }
};

/// TODO Check if this filter is needed. This should not be passed down.
class AlwaysTrue final : public Filter {
 public:
  AlwaysTrue() : Filter(true, true, FilterKind::kAlwaysTrue) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<AlwaysTrue>();
  }

  bool testNull() const final {
    return true;
  }

  bool testNonNull() const final {
    return true;
  }

  bool testInt64(int64_t /* unused */) const final {
    return true;
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/)
      const final {
    return true;
  }

  bool testDoubleRange(double /*min*/, double /*max*/, bool /*hasNull*/)
      const final {
    return true;
  }

  bool testDouble(double /* unused */) const final {
    return true;
  }

  bool testFloat(float /* unused */) const final {
    return true;
  }

  bool testBool(bool /* unused */) const final {
    return true;
  }

  bool testBytes(const char* /* unused */, int32_t /* unused */) const final {
    return true;
  }

  bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool /*hasNull*/) const final {
    return true;
  }

  bool testLength(int32_t /* unused */) const final {
    return true;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final {
    // true AND <any> is <any>.
    return other->clone();
  }
};

/// Returns true if the value is null. Supports all data types.
class IsNull final : public Filter {
 public:
  IsNull() : Filter(true, true, FilterKind::kIsNull) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<IsNull>();
  }

  bool testNonNull() const final {
    return false;
  }

  bool testInt64(int64_t /* unused */) const final {
    return false;
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool hasNull)
      const final {
    return hasNull;
  }

  bool testDoubleRange(double /*min*/, double /*max*/, bool hasNull)
      const final {
    return hasNull;
  }

  bool testDouble(double /* unused */) const final {
    return false;
  }

  bool testFloat(float /* unused */) const final {
    return false;
  }

  bool testBool(bool /* unused */) const final {
    return false;
  }

  bool testBytes(const char* /* unused */, int32_t /* unused */) const final {
    return false;
  }

  bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool hasNull) const final {
    return hasNull;
  }

  bool testLength(int32_t /* unused */) const final {
    return false;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
};

/// Returns true if the value is not null. Supports all data types.
class IsNotNull final : public Filter {
 public:
  IsNotNull() : Filter(true, false, FilterKind::kIsNotNull) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<IsNotNull>();
  }

  bool testNonNull() const final {
    return true;
  }

  bool testInt64(int64_t /* unused */) const final {
    return true;
  }

  bool testInt64Range(int64_t /*min*/, int64_t /*max*/, bool /*hasNull*/)
      const final {
    return true;
  }

  bool testDoubleRange(double /*min*/, double /*max*/, bool /*hasNull*/)
      const final {
    return true;
  }

  bool testDouble(double /* unused */) const final {
    return true;
  }

  bool testFloat(float /* unused */) const final {
    return true;
  }

  bool testBool(bool /* unused */) const final {
    return true;
  }

  bool testBytes(const char* /* unused */, int32_t /* unused */) const final {
    return true;
  }

  bool testBytesRange(
      std::optional<std::string_view> /*min*/,
      std::optional<std::string_view> /*max*/,
      bool /*hasNull*/) const final {
    return true;
  }

  bool testLength(int /* unused */) const final {
    return true;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
};

/// Tests whether boolean value is true or false or integral value is zero or
/// not. Support boolean and integral data types.
class BoolValue final : public Filter {
 public:
  /// @param value The boolean value that passes the filter. If true, integral
  /// values that are not zero are passing as well.
  /// @param nullAllowed Null values are passing the filter if true.
  BoolValue(bool value, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBoolValue), value_(value) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BoolValue>(this->value_, nullAllowed.value());
    } else {
      return std::make_unique<BoolValue>(*this);
    }
  }

  bool testBool(bool value) const final {
    return value_ == value;
  }

  bool testInt64(int64_t value) const final {
    return value_ == (value != 0);
  }

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    if (value_) {
      return min != 0 || max != 0;
    } else {
      return min <= 0 && max >= 0;
    }
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

 private:
  const bool value_;
};

/// Range filter for integral data types. Supports open, closed and unbounded
/// ranges, e.g. c >= 10, c <= 34, c BETWEEN 10 and 34. Open ranges can be
/// implemented by using the value to the left or right of the end of the range,
/// e.g. a < 10 is equivalent to a <= 9.
class BigintRange final : public Filter {
 public:
  /// @param lower Lower end of the range, inclusive.
  /// @param upper Upper end of the range, inclusive.
  /// @param nullAllowed Null values are passing the filter if true.
  BigintRange(int64_t lower, int64_t upper, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBigintRange),
        lower_(lower),
        upper_(upper),
        lower32_(std::max<int64_t>(lower, std::numeric_limits<int32_t>::min())),
        upper32_(std::min<int64_t>(upper, std::numeric_limits<int32_t>::max())),
        lower16_(std::max<int64_t>(lower, std::numeric_limits<int16_t>::min())),
        upper16_(std::min<int64_t>(upper, std::numeric_limits<int16_t>::max())),
        isSingleValue_(upper_ == lower_) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BigintRange>(
          this->lower_, this->upper_, nullAllowed.value());
    } else {
      return std::make_unique<BigintRange>(*this);
    }
  }

  bool testInt64(int64_t value) const final {
    return value >= lower_ && value <= upper_;
  }

  xsimd::batch_bool<int64_t> testValues(
      xsimd::batch<int64_t> values) const final {
    if (isSingleValue_) {
      return values == xsimd::broadcast<int64_t>(lower_);
    }
    return (xsimd::broadcast<int64_t>(lower_) <= values) &
        (values <= xsimd::broadcast<int64_t>(upper_));
  }

  xsimd::batch_bool<int32_t> testValues(
      xsimd::batch<int32_t> values) const final {
    if (isSingleValue_) {
      if (UNLIKELY(lower32_ != lower_)) {
        return xsimd::batch_bool<int32_t>(false);
      }
      return values == xsimd::broadcast<int32_t>(lower_);
    }
    return (xsimd::broadcast<int32_t>(lower32_) <= values) &
        (values <= xsimd::broadcast<int32_t>(upper32_));
  }

  xsimd::batch_bool<int16_t> testValues(
      xsimd::batch<int16_t> values) const final {
    if (isSingleValue_) {
      if (UNLIKELY(lower16_ != lower_)) {
        return xsimd::batch_bool<int16_t>(false);
      }
      return values == xsimd::broadcast<int16_t>(lower_);
    }
    return (xsimd::broadcast<int16_t>(lower16_) <= values) &
        (values <= xsimd::broadcast<int16_t>(upper16_));
  }

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    return !(min > upper_ || max < lower_);
  }

  int64_t lower() const {
    return lower_;
  }

  int64_t upper() const {
    return upper_;
  }

  bool isSingleValue() const {
    return isSingleValue_;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  std::string toString() const final {
    return fmt::format(
        "BigintRange: [{}, {}] {}",
        lower_,
        upper_,
        nullAllowed_ ? "with nulls" : "no nulls");
  }

 private:
  const int64_t lower_;
  const int64_t upper_;
  const int32_t lower32_;
  const int32_t upper32_;
  const int16_t lower16_;
  const int16_t upper16_;
  const bool isSingleValue_;
};

/// IN-list filter for integral data types. Implemented as a hash table. Good
/// for large number of values that do not fit within a small range.
class BigintValuesUsingHashTable final : public Filter {
 public:
  /// @param min Minimum value.
  /// @param max Maximum value.
  /// @param values A list of unique values that pass the filter. Must contain
  /// at least two entries.
  /// @param nullAllowed Null values are passing the filter if true.
  BigintValuesUsingHashTable(
      int64_t min,
      int64_t max,
      const std::vector<int64_t>& values,
      bool nullAllowed);

  BigintValuesUsingHashTable(
      const BigintValuesUsingHashTable& other,
      bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        min_(other.min_),
        max_(other.max_),
        hashTable_(other.hashTable_),
        containsEmptyMarker_(other.containsEmptyMarker_),
        values_(other.values_),
        sizeMask_(other.sizeMask_) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BigintValuesUsingHashTable>(
          *this, nullAllowed.value());
    } else {
      return std::make_unique<BigintValuesUsingHashTable>(*this);
    }
  }

  bool testInt64(int64_t value) const final;
  xsimd::batch_bool<int64_t> testValues(xsimd::batch<int64_t>) const final;
  xsimd::batch_bool<int32_t> testValues(xsimd::batch<int32_t>) const final;
  xsimd::batch_bool<int16_t> testValues(xsimd::batch<int16_t> x) const final {
    return Filter::testValues(x);
  }
  bool testInt64Range(int64_t min, int64_t max, bool hashNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  int64_t min() const {
    return min_;
  }

  int64_t max() const {
    return max_;
  }

  const std::vector<int64_t>& values() const {
    return values_;
  }

  std::string toString() const final {
    return fmt::format(
        "BigintValuesUsingHashTable: [{}, {}] {}",
        min_,
        max_,
        nullAllowed_ ? "with nulls" : "no nulls");
  }

 private:
  std::unique_ptr<Filter>
  mergeWith(int64_t min, int64_t max, const Filter* other) const;

  static constexpr int64_t kEmptyMarker = 0xdeadbeefbadefeedL;
  // from Murmur hash
  static constexpr uint64_t M = 0xc6a4a7935bd1e995L;

  const int64_t min_;
  const int64_t max_;
  std::vector<int64_t> hashTable_;
  bool containsEmptyMarker_ = false;
  std::vector<int64_t> values_;
  int32_t sizeMask_;
};

/// IN-list filter for integral data types. Implemented as a bitmask. Offers
/// better performance than the hash table when the range of values is small.
class BigintValuesUsingBitmask final : public Filter {
 public:
  /// @param min Minimum value.
  /// @param max Maximum value.
  /// @param values A list of unique values that pass the filter. Must contain
  /// at least two entries.
  /// @param nullAllowed Null values are passing the filter if true.
  BigintValuesUsingBitmask(
      int64_t min,
      int64_t max,
      const std::vector<int64_t>& values,
      bool nullAllowed);

  BigintValuesUsingBitmask(
      const BigintValuesUsingBitmask& other,
      bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBigintValuesUsingBitmask),
        bitmask_(other.bitmask_),
        min_(other.min_),
        max_(other.max_) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BigintValuesUsingBitmask>(
          *this, nullAllowed.value());
    } else {
      return std::make_unique<BigintValuesUsingBitmask>(*this);
    }
  }

  std::vector<int64_t> values() const;

  bool testInt64(int64_t value) const final;

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

 private:
  std::unique_ptr<Filter>
  mergeWith(int64_t min, int64_t max, const Filter* other) const;

  std::vector<bool> bitmask_;
  const int64_t min_;
  const int64_t max_;
};

// NOT IN-list filter for integral data types. Implemented as a hash table. Good
// for large number of rejected values that do not fit within a small range.
class NegatedBigintValuesUsingHashTable final : public Filter {
 public:
  /// @param min Minimum rejected value.
  /// @param max Maximum rejected value.
  /// @param values A list of unique values that fail the filter. Must contain
  /// at least two entries.
  /// @param nullAllowed Null values are passing the filter if true.
  NegatedBigintValuesUsingHashTable(
      int64_t min,
      int64_t max,
      const std::vector<int64_t>& values,
      bool nullAllowed);

  NegatedBigintValuesUsingHashTable(
      const NegatedBigintValuesUsingHashTable& other,
      bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        nonNegated_(
            std::make_unique<BigintValuesUsingHashTable>(*other.nonNegated_)) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<NegatedBigintValuesUsingHashTable>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  bool testInt64(int64_t value) const final {
    return !nonNegated_->testInt64(value);
  }

  xsimd::batch_bool<int64_t> testValues(xsimd::batch<int64_t> x) const final {
    return ~nonNegated_->testValues(x);
  }

  xsimd::batch_bool<int32_t> testValues(xsimd::batch<int32_t> x) const final {
    return ~nonNegated_->testValues(x);
  }

  xsimd::batch_bool<int16_t> testValues(xsimd::batch<int16_t> x) const final {
    return ~nonNegated_->testValues(x);
  }

  bool testInt64Range(int64_t min, int64_t max, bool hashNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  int64_t min() const {
    return nonNegated_->min();
  }

  int64_t max() const {
    return nonNegated_->max();
  }

  const std::vector<int64_t>& values() const {
    return nonNegated_->values();
  }

  std::string toString() const final {
    return fmt::format(
        "NegatedBigintValuesUsingHashTable: [{}, {}] {}",
        nonNegated_->min(),
        nonNegated_->max(),
        nullAllowed_ ? "with nulls" : "no nulls");
  }

 private:
  std::unique_ptr<Filter>
  mergeWith(int64_t min, int64_t max, const Filter* other) const;

  std::unique_ptr<BigintValuesUsingHashTable> nonNegated_;
};

/// NOT IN-list filter for integral data types. Implemented as a bitmask. Offers
/// better performance than the hash table when the range of values is small.
class NegatedBigintValuesUsingBitmask final : public Filter {
 public:
  /// @param min Minimum REJECTED value.
  /// @param max Maximum REJECTED value.
  /// @param values A list of unique values that pass the filter. Must contain
  /// at least two entries.
  /// @param nullAllowed Null values are passing the filter if true.
  NegatedBigintValuesUsingBitmask(
      int64_t min,
      int64_t max,
      const std::vector<int64_t>& values,
      bool nullAllowed);

  NegatedBigintValuesUsingBitmask(
      const NegatedBigintValuesUsingBitmask& other,
      bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        min_(other.min_),
        max_(other.max_),
        nonNegated_(
            std::make_unique<BigintValuesUsingBitmask>(*other.nonNegated_)) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<NegatedBigintValuesUsingBitmask>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  std::vector<int64_t> values() const {
    return nonNegated_->values();
  }

  bool testInt64(int64_t value) const final {
    return !nonNegated_->testInt64(value);
  }

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

 private:
  std::unique_ptr<Filter>
  mergeWith(int64_t min, int64_t max, const Filter* other) const;

  int min_;
  int max_;
  std::unique_ptr<BigintValuesUsingBitmask> nonNegated_;
};

/// Base class for range filters on floating point and string data types.
class AbstractRange : public Filter {
 public:
  bool lowerUnbounded() const {
    return lowerUnbounded_;
  }

  bool lowerExclusive() const {
    return lowerExclusive_;
  }

  bool upperUnbounded() const {
    return upperUnbounded_;
  }

  bool upperExclusive() const {
    return upperExclusive_;
  }

 protected:
  AbstractRange(
      bool lowerUnbounded,
      bool lowerExclusive,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed,
      FilterKind kind)
      : Filter(true, nullAllowed, kind),
        lowerUnbounded_(lowerUnbounded),
        lowerExclusive_(lowerExclusive),
        upperUnbounded_(upperUnbounded),
        upperExclusive_(upperExclusive) {}

 protected:
  const bool lowerUnbounded_;
  const bool lowerExclusive_;
  const bool upperUnbounded_;
  const bool upperExclusive_;
};

/// Range filter for floating point data types. Supports open, closed and
/// unbounded ranges, e.g. c >= 10.3, c > 10.3, c <= 34.8, c < 34.8, c >= 10.3
/// AND c < 34.8, c BETWEEN 10.3 and 34.8.
/// @tparam T Floating point type: float or double.
template <typename T>
class FloatingPointRange final : public AbstractRange {
 public:
  /// @param lower Lower end of the range.
  /// @param lowerUnbounded True if lower end is negative infinity in which case
  /// the value of lower is ignored.
  /// @param lowerExclusive True if open range, e.g. lower value doesn't pass
  /// the filter.
  /// @param upper Upper end of the range.
  /// @param upperUnbounded True if upper end is positive infinity in which case
  /// the value of upper is ignored.
  /// @param upperExclusive True if open range, e.g. upper value doesn't pass
  /// the filter.
  /// @param nullAllowed Null values are passing the filter if true.
  FloatingPointRange(
      T lower,
      bool lowerUnbounded,
      bool lowerExclusive,
      T upper,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed)
      : AbstractRange(
            lowerUnbounded,
            lowerExclusive,
            upperUnbounded,
            upperExclusive,
            nullAllowed,
            (std::is_same<T, double>::value) ? FilterKind::kDoubleRange
                                             : FilterKind::kFloatRange),
        lower_(lower),
        upper_(upper) {
    VELOX_CHECK(lowerUnbounded || !std::isnan(lower_));
    VELOX_CHECK(upperUnbounded || !std::isnan(upper_));
  }

  FloatingPointRange(const FloatingPointRange& other, bool nullAllowed)
      : AbstractRange(
            other.lowerUnbounded_,
            other.lowerExclusive_,
            other.upperUnbounded_,
            other.upperExclusive_,
            nullAllowed,
            (std::is_same<T, double>::value) ? FilterKind::kDoubleRange
                                             : FilterKind::kFloatRange),
        lower_(other.lower_),
        upper_(other.upper_) {
    VELOX_CHECK(lowerUnbounded_ || !std::isnan(lower_));
    VELOX_CHECK(upperUnbounded_ || !std::isnan(upper_));
  }

  double lower() const {
    return lower_;
  }

  double upper() const {
    return upper_;
  }

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<FloatingPointRange<T>>(
          *this, nullAllowed.value());
    } else {
      return std::make_unique<FloatingPointRange<T>>(*this);
    }
  }

  bool testDouble(double value) const final {
    return testFloatingPoint(value);
  }

  bool testFloat(float value) const final {
    return testFloatingPoint(value);
  }

  xsimd::batch_bool<double> testValues(xsimd::batch<double>) const final;
  xsimd::batch_bool<float> testValues(xsimd::batch<float>) const final;

  bool testDoubleRange(double min, double max, bool hasNull) const final {
    if (hasNull && nullAllowed_) {
      return true;
    }

    return !(min > upper_ || max < lower_);
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final {
    switch (other->kind()) {
      case FilterKind::kAlwaysTrue:
      case FilterKind::kAlwaysFalse:
      case FilterKind::kIsNull:
        return other->mergeWith(this);
      case FilterKind::kIsNotNull:
        return std::make_unique<FloatingPointRange<T>>(
            lower_,
            lowerUnbounded_,
            lowerExclusive_,
            upper_,
            upperUnbounded_,
            upperExclusive_,
            false);
      case FilterKind::kDoubleRange:
      case FilterKind::kFloatRange: {
        bool bothNullAllowed = nullAllowed_ && other->testNull();

        auto otherRange = static_cast<const FloatingPointRange<T>*>(other);

        auto lower = std::max(lower_, otherRange->lower_);
        auto upper = std::min(upper_, otherRange->upper_);

        auto bothLowerUnbounded =
            lowerUnbounded_ && otherRange->lowerUnbounded_;
        auto bothUpperUnbounded =
            upperUnbounded_ && otherRange->upperUnbounded_;

        auto lowerExclusive = !bothLowerUnbounded &&
            (!testDouble(lower) || !other->testDouble(lower));
        auto upperExclusive = !bothUpperUnbounded &&
            (!testDouble(upper) || !other->testDouble(upper));

        if (lower > upper || (lower == upper && lowerExclusive_)) {
          if (bothNullAllowed) {
            return std::make_unique<IsNull>();
          }
          return std::make_unique<AlwaysFalse>();
        }

        return std::make_unique<FloatingPointRange<T>>(
            lower,
            bothLowerUnbounded,
            lowerExclusive,
            upper,
            bothUpperUnbounded,
            upperExclusive,
            bothNullAllowed);
      }
      default:
        VELOX_UNREACHABLE();
    }
  }

  std::string toString() const final;

 private:
  std::string toString(const std::string& name) const {
    return fmt::format(
        "{}: {}{}, {}{} {}",
        name,
        (lowerExclusive_ || lowerUnbounded_) ? "(" : "[",
        lowerUnbounded_ ? "-inf" : std::to_string(lower_),
        upperUnbounded_ ? "+inf" : std::to_string(upper_),
        (upperExclusive_ || upperUnbounded_) ? ")" : "]",
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool testFloatingPoint(T value) const {
    if (std::isnan(value)) {
      return false;
    }
    if (!lowerUnbounded_) {
      if (value < lower_) {
        return false;
      }
      if (lowerExclusive_ && lower_ == value) {
        return false;
      }
    }
    if (!upperUnbounded_) {
      if (value > upper_) {
        return false;
      }
      if (upperExclusive_ && value == upper_) {
        return false;
      }
    }
    return true;
  }

  xsimd::batch_bool<T> testFloatingPoints(xsimd::batch<T> values) const {
    xsimd::batch_bool<T> result;
    if (!lowerUnbounded_) {
      auto allLower = xsimd::broadcast<T>(lower_);
      if (lowerExclusive_) {
        result = allLower < values;
      } else {
        result = allLower <= values;
      }
      if (!upperUnbounded_) {
        auto allUpper = xsimd::broadcast<T>(upper_);
        if (upperExclusive_) {
          result = result & (values < allUpper);
        } else {
          result = result & (values <= allUpper);
        }
      }
    } else {
      auto allUpper = xsimd::broadcast<T>(upper_);
      if (upperExclusive_) {
        result = values < allUpper;
      } else {
        result = values <= allUpper;
      }
    }
    return result;
  }

  const T lower_;
  const T upper_;
};

template <>
inline std::string FloatingPointRange<double>::toString() const {
  return toString("DoubleRange");
}

template <>
inline std::string FloatingPointRange<float>::toString() const {
  return toString("FloatRange");
}

template <>
inline xsimd::batch_bool<double> FloatingPointRange<double>::testValues(
    xsimd::batch<double> x) const {
  return testFloatingPoints(x);
}

template <>
inline xsimd::batch_bool<float> FloatingPointRange<double>::testValues(
    xsimd::batch<float>) const {
  VELOX_FAIL("Not defined for double filter");
}

template <>
inline xsimd::batch_bool<double> FloatingPointRange<float>::testValues(
    xsimd::batch<double>) const {
  VELOX_FAIL("Not defined for float filter");
}

template <>
inline xsimd::batch_bool<float> FloatingPointRange<float>::testValues(
    xsimd::batch<float> x) const {
  return testFloatingPoints(x);
}

using DoubleRange = FloatingPointRange<double>;
using FloatRange = FloatingPointRange<float>;

/// Range filter for string data type. Supports open, closed and
/// unbounded ranges.
class BytesRange final : public AbstractRange {
 public:
  /// @param lower Lower end of the range.
  /// @param lowerUnbounded True if lower end is "negative infinity" in which
  /// case the value of lower is ignored.
  /// @param lowerExclusive True if open range, e.g. lower value doesn't pass
  /// the filter.
  /// @param upper Upper end of the range.
  /// @param upperUnbounded True if upper end is "positive infinity" in which
  /// case the value of upper is ignored.
  /// @param upperExclusive True if open range, e.g. upper value doesn't pass
  /// the filter.
  /// @param nullAllowed Null values are passing the filter if true.
  BytesRange(
      const std::string& lower,
      bool lowerUnbounded,
      bool lowerExclusive,
      const std::string& upper,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed)
      : AbstractRange(
            lowerUnbounded,
            lowerExclusive,
            upperUnbounded,
            upperExclusive,
            nullAllowed,
            FilterKind::kBytesRange),
        lower_(lower),
        upper_(upper),
        singleValue_(
            !lowerExclusive_ && !upperExclusive_ && !lowerUnbounded_ &&
            !upperUnbounded_ && lower_ == upper_) {
    // Always-true filters should be specified using AlwaysTrue.
    VELOX_CHECK(!lowerUnbounded_ || !upperUnbounded_);
  }

  BytesRange(const BytesRange& other, bool nullAllowed)
      : AbstractRange(
            other.lowerUnbounded_,
            other.lowerExclusive_,
            other.upperUnbounded_,
            other.upperExclusive_,
            nullAllowed,
            FilterKind::kBytesRange),
        lower_(other.lower_),
        upper_(other.upper_),
        singleValue_(other.singleValue_) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BytesRange>(*this, nullAllowed.value());
    } else {
      return std::make_unique<BytesRange>(*this);
    }
  }

  std::string toString() const final {
    return fmt::format(
        "BytesRange: {}{}, {}{} {}",
        (lowerUnbounded_ || lowerExclusive_) ? "(" : "[",
        lowerUnbounded_ ? "..." : lower_,
        upperUnbounded_ ? "..." : upper_,
        (upperUnbounded_ || upperExclusive_) ? ")" : "]",
        nullAllowed_ ? "with nulls" : "no nulls");
  }

  bool testBytes(const char* value, int32_t length) const final;

  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  bool hasTestLength() const final {
    return singleValue_;
  }

  bool testLength(int length) const final {
    return !singleValue_ || lower_.size() == length;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  xsimd::batch_bool<int32_t> testLengths(
      xsimd::batch<int32_t> lengths) const final {
    VELOX_DCHECK(singleValue_);
    return lengths == xsimd::broadcast<int32_t>(lower_.size());
  }

  bool isSingleValue() const {
    return singleValue_;
  }

  bool isUpperUnbounded() const {
    return upperUnbounded_;
  }

  bool isLowerUnbounded() const {
    return lowerUnbounded_;
  }

  const std::string& lower() const {
    return lower_;
  }

  const std::string& upper() const {
    return upper_;
  }

 private:
  const std::string lower_;
  const std::string upper_;
  const bool singleValue_;
};

/// IN-list filter for string data type.
class BytesValues final : public Filter {
 public:
  /// @param values List of values that pass the filter. Must contain at least
  /// one entry.
  /// @param nullAllowed Null values are passing the filter if true.
  BytesValues(const std::vector<std::string>& values, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBytesValues) {
    VELOX_CHECK(!values.empty(), "values must not be empty");

    for (const auto& value : values) {
      lengths_.insert(value.size());
      values_.insert(value);
    }

    lower_ = *std::min_element(values_.begin(), values_.end());
    upper_ = *std::max_element(values_.begin(), values_.end());
  }

  BytesValues(const BytesValues& other, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kBytesValues),
        lower_(other.lower_),
        upper_(other.upper_),
        values_(other.values_),
        lengths_(other.lengths_) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    if (nullAllowed) {
      return std::make_unique<BytesValues>(*this, nullAllowed.value());
    } else {
      return std::make_unique<BytesValues>(*this);
    }
  }

  bool testLength(int32_t length) const final {
    return lengths_.contains(length);
  }

  bool testBytes(const char* value, int32_t length) const final {
    return lengths_.contains(length) &&
        values_.contains(std::string(value, length));
  }

  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  const folly::F14FastSet<std::string>& values() const {
    return values_;
  }

 private:
  std::string lower_;
  std::string upper_;
  folly::F14FastSet<std::string> values_;
  folly::F14FastSet<uint32_t> lengths_;
};

/// Represents a combination of two of more range filters on integral types with
/// OR semantics. The filter passes if at least one of the contained filters
/// passes.
class BigintMultiRange final : public Filter {
 public:
  /// @param ranges List of range filters. Must contain at least two entries.
  /// @param nullAllowed Null values are passing the filter if true. nullAllowed
  /// flags in the 'ranges' filters are ignored.
  BigintMultiRange(
      std::vector<std::unique_ptr<BigintRange>> ranges,
      bool nullAllowed);

  BigintMultiRange(const BigintMultiRange& other, bool nullAllowed);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool testInt64(int64_t value) const final;

  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  const std::vector<std::unique_ptr<BigintRange>>& ranges() const {
    return ranges_;
  }

  std::string toString() const override {
    std::ostringstream out;
    out << "BigintMultiRange: [";
    for (const auto& range : ranges_) {
      out << " " << range->toString();
    }
    out << " ]" << (nullAllowed_ ? "with nulls" : "no nulls");
    return out.str();
  }

 private:
  const std::vector<std::unique_ptr<BigintRange>> ranges_;
  std::vector<int64_t> lowerBounds_;
};

/// NOT IN-list filter for string data type.
class NegatedBytesValues final : public Filter {
 public:
  /// @param values List of values that fail the filter. Must contain at least
  /// one entry.
  /// @param nullAllowed Null values are passing the filter if true.
  NegatedBytesValues(const std::vector<std::string>& values, bool nullAllowed)
      : Filter(true, nullAllowed, FilterKind::kNegatedBytesValues) {
    VELOX_CHECK(!values.empty(), "values must not be empty");
    nonNegated_ = std::make_unique<BytesValues>(values, !nullAllowed);
  }

  NegatedBytesValues(const NegatedBytesValues& other, bool nullAllowed)
      : Filter(true, nullAllowed, other.kind()),
        nonNegated_(std::make_unique<BytesValues>(*other.nonNegated_)) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final {
    return std::make_unique<NegatedBytesValues>(
        *this, nullAllowed.value_or(nullAllowed_));
  }

  bool testLength(int32_t /* unused */) const final {
    // it is very rare that we will reject all strings of a given length
    // using a NegatedBytesValues filter.
    return true;
  }

  bool testBytes(const char* value, int32_t length) const final {
    return !nonNegated_->testBytes(value, length);
  }

  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  const folly::F14FastSet<std::string>& values() const {
    return nonNegated_->values();
  }

 private:
  std::unique_ptr<BytesValues> nonNegated_;
};

/// Represents a combination of two of more filters with
/// OR semantics. The filter passes if at least one of the contained filters
/// passes.
class MultiRange final : public Filter {
 public:
  /// @param ranges List of range filters. Must contain at least two entries.
  /// All entries must support the same data types.
  /// @param nullAllowed Null values are passing the filter if true. nullAllowed
  /// flags in the 'ranges' filters are ignored.
  /// @param nanAllowed Not-a-Number floating point values are passing the
  /// filter if true. Applies to floating point data types only. NaN values are
  /// not further tested using contained filters.
  MultiRange(
      std::vector<std::unique_ptr<Filter>> filters,
      bool nullAllowed,
      bool nanAllowed)
      : Filter(true, nullAllowed, FilterKind::kMultiRange),
        filters_(std::move(filters)),
        nanAllowed_(nanAllowed) {}

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool testDouble(double value) const final;

  bool testFloat(float value) const final;

  bool testBytes(const char* value, int32_t length) const final;

  bool testLength(int32_t length) const final;

  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const final;

  bool testDoubleRange(double min, double max, bool hasNull) const final;

  const std::vector<std::unique_ptr<Filter>>& filters() const {
    return filters_;
  }

  std::unique_ptr<Filter> mergeWith(const Filter* other) const override final;

  bool nanAllowed() const {
    return nanAllowed_;
  }

 private:
  const std::vector<std::unique_ptr<Filter>> filters_;
  const bool nanAllowed_;
};

// Helper for applying filters to different types
template <typename TFilter, typename T>
static inline bool applyFilter(TFilter& filter, T value) {
  if (std::is_same<T, int8_t>::value || std::is_same<T, int16_t>::value ||
      std::is_same<T, int32_t>::value || std::is_same<T, int64_t>::value) {
    return filter.testInt64(value);
  } else if (std::is_same<T, float>::value) {
    return filter.testFloat(value);
  } else if (std::is_same<T, double>::value) {
    return filter.testDouble(value);
  } else if (std::is_same<T, bool>::value) {
    return filter.testBool(value);
  } else {
    VELOX_CHECK(false, "Bad argument type to filter");
  }
}

template <typename TFilter>
static inline bool applyFilter(TFilter& filter, folly::StringPiece value) {
  return filter.testBytes(value.data(), value.size());
}

template <typename TFilter>
static inline bool applyFilter(TFilter& filter, StringView value) {
  return filter.testBytes(value.data(), value.size());
}

// Creates a hash or bitmap based IN filter depending on value distribution.
std::unique_ptr<Filter> createBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed);

// Creates a hash or bitmap based NOT IN filter depending on value distribution.
std::unique_ptr<Filter> createNegatedBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed);

} // namespace facebook::velox::common
