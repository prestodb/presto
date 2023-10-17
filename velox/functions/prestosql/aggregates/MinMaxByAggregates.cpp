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

#include "velox/functions/lib/aggregates/MinMaxByAggregatesBase.h"
#include "velox/functions/lib/aggregates/ValueSet.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/Compare.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {

/// Returns true if the value in 'index' row of 'newComparisons' is strictly
/// greater than or less than the value in the 'accumulator'.
template <bool greaterThan, typename T, typename TAccumulator>
struct Comparator {
  static bool compare(
      TAccumulator* accumulator,
      const DecodedVector& newComparisons,
      vector_size_t index,
      bool isFirstValue) {
    if constexpr (isNumeric<T>()) {
      if (isFirstValue) {
        return true;
      }
      if constexpr (greaterThan) {
        return newComparisons.valueAt<T>(index) > *accumulator;
      } else {
        return newComparisons.valueAt<T>(index) < *accumulator;
      }
    } else {
      // SingleValueAccumulator::compare has the semantics of accumulator value
      // is less than vector value.
      if constexpr (greaterThan) {
        return !accumulator->hasValue() ||
            prestosql::compare(accumulator, newComparisons, index) < 0;
      } else {
        return !accumulator->hasValue() ||
            prestosql::compare(accumulator, newComparisons, index) > 0;
      }
    }
  }
};

template <typename T>
struct RawValueExtractor {
  using TRawValue = std::conditional_t<std::is_same_v<T, bool>, uint64_t, T>;

  static TRawValue* mutableRawValues(VectorPtr& values) {
    if constexpr (std::is_same_v<T, bool>) {
      return values->as<FlatVector<T>>()->template mutableRawValues<uint64_t>();
    } else {
      return values->as<FlatVector<T>>()->mutableRawValues();
    }
  }

  static void extract(TRawValue* rawValues, vector_size_t offset, T value) {
    if constexpr (std::is_same_v<T, bool>) {
      bits::setBit(rawValues, offset, value);
    } else {
      rawValues[offset] = value;
    }
  }
};

/// @tparam V Type of value.
/// @tparam C Type of compare.
/// @tparam Compare Type of comparator of std::pair<C, std::optional<V>>.
template <typename V, typename C, typename Compare>
struct MinMaxByNAccumulator {
  using TRawValue = typename RawValueExtractor<V>::TRawValue;
  using TRawComparison = typename RawValueExtractor<C>::TRawValue;

  int64_t n{0};

  using Pair = std::pair<C, std::optional<V>>;
  using Queue =
      std::priority_queue<Pair, std::vector<Pair, StlAllocator<Pair>>, Compare>;
  Queue topPairs;

  explicit MinMaxByNAccumulator(HashStringAllocator* allocator)
      : topPairs{Compare{}, StlAllocator<Pair>(allocator)} {}

  int64_t getN() const {
    return n;
  }

  size_t size() const {
    return topPairs.size();
  }

  void checkAndSetN(DecodedVector& decodedN, vector_size_t row) {
    // Skip null N.
    if (decodedN.isNullAt(row)) {
      return;
    }

    const auto newN = decodedN.valueAt<int64_t>(row);
    VELOX_USER_CHECK_GT(
        newN, 0, "third argument of max_by/min_by must be a positive integer");

    if (n) {
      VELOX_USER_CHECK_EQ(
          newN,
          n,
          "third argument of max_by/min_by must be a constant for all rows in a group");
    } else {
      n = newN;
    }
  }

  void
  compareAndAdd(C comparison, std::optional<V> value, Compare& comparator) {
    if (topPairs.size() < n) {
      topPairs.push({comparison, value});
    } else {
      const auto& topPair = topPairs.top();
      if (comparator.compare(comparison, topPair)) {
        topPairs.pop();
        topPairs.push({comparison, value});
      }
    }
  }

  /// Moves all values from 'topPairs' into 'rawValues' and 'rawValueNulls'
  /// buffers. The queue of 'topPairs' will be empty after this call.
  void extractValues(
      TRawValue* rawValues,
      uint64_t* rawValueNulls,
      vector_size_t offset) {
    const vector_size_t size = topPairs.size();
    for (auto i = size - 1; i >= 0; --i) {
      const auto& topPair = topPairs.top();
      const auto index = offset + i;

      const bool valueIsNull = !topPair.second.has_value();
      bits::setNull(rawValueNulls, index, valueIsNull);
      if (!valueIsNull) {
        RawValueExtractor<V>::extract(rawValues, index, topPair.second.value());
      }

      topPairs.pop();
    }
  }

  /// Moves all pairs of (comparison, value) from 'topPairs' into
  /// 'rawComparisons', 'rawValues' and 'rawValueNulls' buffers. The queue of
  /// 'topPairs' will be empty after this call.
  void extractPairs(
      TRawComparison* rawComparisons,
      TRawValue* rawValues,
      uint64_t* rawValueNulls,
      vector_size_t offset) {
    const vector_size_t size = topPairs.size();
    for (auto i = size - 1; i >= 0; --i) {
      const auto& topPair = topPairs.top();
      const auto index = offset + i;

      RawValueExtractor<C>::extract(rawComparisons, index, topPair.first);

      const bool valueIsNull = !topPair.second.has_value();
      bits::setNull(rawValueNulls, index, valueIsNull);
      if (!valueIsNull) {
        RawValueExtractor<V>::extract(rawValues, index, topPair.second.value());
      }

      topPairs.pop();
    }
  }
};

template <typename V, typename C, typename Compare>
struct Extractor {
  using TRawValue = typename RawValueExtractor<V>::TRawValue;
  using TRawComparison = typename RawValueExtractor<C>::TRawValue;

  TRawValue* rawValues;
  uint64_t* rawValueNulls;

  explicit Extractor(VectorPtr& values) {
    rawValues = RawValueExtractor<V>::mutableRawValues(values);
    rawValueNulls = values->mutableRawNulls();
  }

  void extractValues(
      MinMaxByNAccumulator<V, C, Compare>* accumulator,
      vector_size_t offset) {
    accumulator->extractValues(rawValues, rawValueNulls, offset);
  }

  void extractPairs(
      MinMaxByNAccumulator<V, C, Compare>* accumulator,
      TRawComparison* rawComparisons,
      vector_size_t offset) {
    accumulator->extractPairs(rawComparisons, rawValues, rawValueNulls, offset);
  }
};

template <typename V, typename C, typename Compare>
struct MinMaxByNStringViewAccumulator {
  using BaseType = MinMaxByNAccumulator<V, C, Compare>;
  BaseType base;
  ValueSet valueSet;

  explicit MinMaxByNStringViewAccumulator(HashStringAllocator* allocator)
      : base{allocator}, valueSet{allocator} {}

  ~MinMaxByNStringViewAccumulator() {
    while (!base.topPairs.empty()) {
      auto& pair = base.topPairs.top();
      freePair(pair);
      base.topPairs.pop();
    }
  }

  int64_t getN() const {
    return base.n;
  }

  size_t size() const {
    return base.size();
  }

  void checkAndSetN(DecodedVector& decodedN, vector_size_t row) {
    return base.checkAndSetN(decodedN, row);
  }

  void
  compareAndAdd(C comparison, std::optional<V> value, Compare& comparator) {
    if (base.topPairs.size() < base.n) {
      addToAccumulator(comparison, value);
    } else {
      const auto& topPair = base.topPairs.top();
      if (comparator.compare(comparison, topPair)) {
        freePair(topPair);
        base.topPairs.pop();
        addToAccumulator(comparison, value);
      }
    }
  }

  /// Moves all values from 'topPairs' into 'values'
  /// buffers. The queue of 'topPairs' will be empty after this call.
  void extractValues(FlatVector<V>& values, vector_size_t offset) {
    const vector_size_t size = base.topPairs.size();
    for (auto i = size - 1; i >= 0; --i) {
      const auto& pair = base.topPairs.top();
      extractValue(pair, values, offset + i);
      freePair(pair);
      base.topPairs.pop();
    }
  }

  /// Moves all pairs of (comparison, value) from 'topPairs' into
  /// 'rawComparisons' buffer and 'values' vector. The queue of
  /// 'topPairs' will be empty after this call.
  void extractPairs(
      FlatVector<C>& compares,
      FlatVector<V>& values,
      vector_size_t offset) {
    const vector_size_t size = base.topPairs.size();
    for (auto i = size - 1; i >= 0; --i) {
      const auto& topPair = base.topPairs.top();
      const auto index = offset + i;

      extractCompare(topPair, compares, index);
      extractValue(topPair, values, index);
      freePair(topPair);
      base.topPairs.pop();
    }
  }

 private:
  using Pair = typename MinMaxByNAccumulator<V, C, Compare>::Pair;

  std::optional<StringView> writeString(
      const std::optional<StringView>& value) {
    if (!value.has_value()) {
      return std::nullopt;
    }
    return valueSet.write(*value);
  }

  void addToAccumulator(C comparison, std::optional<V> value) {
    if constexpr (
        std::is_same_v<V, StringView> && std::is_same_v<C, StringView>) {
      base.topPairs.push({valueSet.write(comparison), writeString(value)});
    } else if constexpr (std::is_same_v<V, StringView>) {
      base.topPairs.push({comparison, writeString(value)});
    } else {
      static_assert(
          std::is_same_v<C, StringView>,
          "At least one of V and C must be StringView.");
      base.topPairs.push({valueSet.write(comparison), value});
    }
  }

  void freePair(typename BaseType::Queue::const_reference topPair) {
    if constexpr (std::is_same_v<C, StringView>) {
      valueSet.free(topPair.first);
    }
    if constexpr (std::is_same_v<V, StringView>) {
      if (topPair.second.has_value()) {
        valueSet.free(*topPair.second);
      }
    }
  }

  void extractValue(
      const Pair& topPair,
      FlatVector<V>& values,
      vector_size_t index) {
    const bool valueIsNull = !topPair.second.has_value();
    values.setNull(index, valueIsNull);
    if (!valueIsNull) {
      values.set(index, topPair.second.value());
    }
  }

  void extractCompare(
      const Pair& topPair,
      FlatVector<C>& compares,
      vector_size_t index) {
    compares.setNull(index, false);
    compares.set(index, topPair.first);
  }
};

template <typename V, typename C, typename Compare>
struct StringViewExtractor {
  using TRawComparison = typename RawValueExtractor<C>::TRawValue;
  FlatVector<V>& values;
  FlatVector<C>* compares;

  StringViewExtractor(VectorPtr& _values, BaseVector* _compares)
      : values{*_values->asFlatVector<V>()},
        compares{_compares ? _compares->asFlatVector<C>() : nullptr} {}

  void extractValues(
      MinMaxByNStringViewAccumulator<V, C, Compare>* accumulator,
      vector_size_t offset) {
    accumulator->extractValues(values, offset);
  }

  void extractPairs(
      MinMaxByNStringViewAccumulator<V, C, Compare>* accumulator,
      vector_size_t offset) {
    VELOX_DCHECK_NOT_NULL(compares);
    accumulator->extractPairs(*compares, values, offset);
  }
};

/// @tparam C Type of compare.
/// @tparam Compare Type of comparator of
/// std::pair<C, std::optional<HashStringAllocator::Position>>.
template <typename C, typename Compare>
struct MinMaxByNComplexTypeAccumulator {
  using BaseType =
      MinMaxByNAccumulator<HashStringAllocator::Position, C, Compare>;
  BaseType base;
  ValueSet valueSet;

  explicit MinMaxByNComplexTypeAccumulator(HashStringAllocator* allocator)
      : base{allocator}, valueSet{allocator} {}

  ~MinMaxByNComplexTypeAccumulator() {
    while (!base.topPairs.empty()) {
      auto& pair = base.topPairs.top();
      freePair(pair);
      base.topPairs.pop();
    }
  }

  int64_t getN() const {
    return base.n;
  }

  size_t size() const {
    return base.size();
  }

  void checkAndSetN(DecodedVector& decodedN, vector_size_t row) {
    return base.checkAndSetN(decodedN, row);
  }

  void compareAndAdd(
      C comparison,
      DecodedVector& decoded,
      vector_size_t index,
      Compare& comparator) {
    if (base.topPairs.size() < base.n) {
      auto position = writeComplex(decoded, index);
      addToAccumulator(comparison, position);
    } else {
      const auto& topPair = base.topPairs.top();
      if (comparator.compare(comparison, topPair)) {
        freePair(topPair);
        base.topPairs.pop();

        auto position = writeComplex(decoded, index);
        addToAccumulator(comparison, position);
      }
    }
  }

  /// Moves all values from 'topPairs' into 'values' vector. The queue of
  /// 'topPairs' will be empty after this call.
  void extractValues(BaseVector& values, vector_size_t offset) {
    const vector_size_t size = base.topPairs.size();
    for (auto i = size - 1; i >= 0; --i) {
      const auto& pair = base.topPairs.top();
      extractValue(pair, values, offset + i);
      freePair(pair);
      base.topPairs.pop();
    }
  }

  /// Moves all pairs of (comparison, value) from 'topPairs' into
  /// 'rawComparisons' buffer and 'values' vector. The queue of
  /// 'topPairs' will be empty after this call.
  void extractPairs(
      FlatVector<C>& compares,
      BaseVector& values,
      vector_size_t offset) {
    const vector_size_t size = base.topPairs.size();
    for (auto i = size - 1; i >= 0; --i) {
      const auto& topPair = base.topPairs.top();
      const auto index = offset + i;

      extractCompare(topPair, compares, index);
      extractValue(topPair, values, index);
      freePair(topPair);
      base.topPairs.pop();
    }
  }

 private:
  using V = HashStringAllocator::Position;
  using Pair = typename MinMaxByNAccumulator<V, C, Compare>::Pair;

  void addToAccumulator(C comparison, const std::optional<V>& position) {
    if constexpr (std::is_same_v<C, StringView>) {
      base.topPairs.push({valueSet.write(comparison), position});
    } else {
      base.topPairs.push({comparison, position});
    }
  }

  std::optional<HashStringAllocator::Position> writeComplex(
      DecodedVector& decoded,
      vector_size_t index) {
    if (decoded.isNullAt(index)) {
      return std::nullopt;
    }
    HashStringAllocator::Position position;
    valueSet.write(*decoded.base(), decoded.index(index), position);
    return position;
  }

  void freePair(typename BaseType::Queue::const_reference topPair) {
    if constexpr (std::is_same_v<C, StringView>) {
      valueSet.free(topPair.first);
    }
    if (topPair.second.has_value()) {
      valueSet.free(topPair.second->header);
    }
  }

  void
  extractValue(const Pair& topPair, BaseVector& values, vector_size_t index) {
    const bool valueIsNull = !topPair.second.has_value();
    values.setNull(index, valueIsNull);
    if (!valueIsNull) {
      auto position = topPair.second.value();
      valueSet.read(&values, index, position.header);
    }
  }

  void extractCompare(
      const Pair& topPair,
      FlatVector<C>& compares,
      vector_size_t index) {
    compares.setNull(index, false);
    compares.set(index, topPair.first);
  }
}; // namespace

template <typename C, typename Compare>
struct ComplexTypeExtractor {
  BaseVector& values;
  FlatVector<C>* compares;

  explicit ComplexTypeExtractor(VectorPtr& _values, BaseVector* _compares)
      : values{*_values},
        compares{_compares ? _compares->asFlatVector<C>() : nullptr} {}

  void extractValues(
      MinMaxByNComplexTypeAccumulator<C, Compare>* accumulator,
      vector_size_t offset) {
    accumulator->extractValues(values, offset);
  }

  void extractPairs(
      MinMaxByNComplexTypeAccumulator<C, Compare>* accumulator,
      vector_size_t offset) {
    VELOX_DCHECK_NOT_NULL(compares);
    accumulator->extractPairs(*compares, values, offset);
  }
};

template <typename V, typename C>
struct Less {
  using Pair = std::pair<C, std::optional<V>>;
  bool operator()(const Pair& lhs, const Pair& rhs) {
    return lhs.first < rhs.first;
  }

  bool compare(C lhs, const Pair& rhs) {
    return lhs < rhs.first;
  }
};

template <typename V, typename C>
struct Greater {
  using Pair = std::pair<C, std::optional<V>>;
  bool operator()(const Pair& lhs, const Pair& rhs) {
    return lhs.first > rhs.first;
  }

  bool compare(C lhs, const Pair& rhs) {
    return lhs > rhs.first;
  }
};

template <typename V, typename C, typename Compare>
struct MinMaxByNAccumulatorTypeTraits {
  using AccumulatorType = MinMaxByNAccumulator<V, C, Compare>;
  using ExtractorType = Extractor<V, C, Compare>;
};

// Use MinMaxByNStringViewAccumulator and StringViewExtractor if at least one of
// V and C is StringView. (Unless V is ComplexType.)
template <typename C, typename Compare>
struct MinMaxByNAccumulatorTypeTraits<StringView, C, Compare> {
  using AccumulatorType =
      MinMaxByNStringViewAccumulator<StringView, C, Compare>;
  using ExtractorType = StringViewExtractor<StringView, C, Compare>;
};

template <typename Compare>
struct MinMaxByNAccumulatorTypeTraits<StringView, StringView, Compare> {
  using AccumulatorType =
      MinMaxByNStringViewAccumulator<StringView, StringView, Compare>;
  using ExtractorType = StringViewExtractor<StringView, StringView, Compare>;
};

template <typename V, typename Compare>
struct MinMaxByNAccumulatorTypeTraits<V, StringView, Compare> {
  using AccumulatorType =
      MinMaxByNStringViewAccumulator<V, StringView, Compare>;
  using ExtractorType = StringViewExtractor<V, StringView, Compare>;
};

// Use MinMaxByNComplexTypeAccumulator and ComplexTypeExtractor if V is
// ComplexType.
template <typename C, typename Compare>
struct MinMaxByNAccumulatorTypeTraits<ComplexType, C, Compare> {
  using AccumulatorType = MinMaxByNComplexTypeAccumulator<C, Compare>;
  using ExtractorType = ComplexTypeExtractor<C, Compare>;
};

template <typename Compare>
struct MinMaxByNAccumulatorTypeTraits<ComplexType, StringView, Compare> {
  using AccumulatorType = MinMaxByNComplexTypeAccumulator<StringView, Compare>;
  using ExtractorType = ComplexTypeExtractor<StringView, Compare>;
};

namespace {
std::pair<vector_size_t*, vector_size_t*> rawOffsetAndSizes(
    ArrayVector& arrayVector) {
  return {
      arrayVector.offsets()->asMutable<vector_size_t>(),
      arrayVector.sizes()->asMutable<vector_size_t>()};
}
} // namespace

template <typename V, typename C, typename Compare>
class MinMaxByNAggregate : public exec::Aggregate {
 public:
  explicit MinMaxByNAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType =
      typename MinMaxByNAccumulatorTypeTraits<V, C, Compare>::AccumulatorType;
  using ExtractorType =
      typename MinMaxByNAccumulatorTypeTraits<V, C, Compare>::ExtractorType;
  using TRawComparison = typename RawValueExtractor<C>::TRawValue;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (const vector_size_t i : indices) {
      auto group = groups[i];
      new (group + offset_) AccumulatorType(allocator_);
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto valuesArray = (*result)->as<ArrayVector>();
    valuesArray->resize(numGroups);

    const auto numValues =
        countValuesAndSetResultNulls(groups, numGroups, *result);

    auto values = valuesArray->elements();
    values->resize(numValues);

    std::optional<ExtractorType> extractor;
    if constexpr (
        std::is_same_v<V, StringView> || std::is_same_v<C, StringView> ||
        std::is_same_v<V, ComplexType>) {
      extractor.emplace(values, nullptr);
    } else {
      extractor.emplace(values);
    }

    auto [rawOffsets, rawSizes] = rawOffsetAndSizes(*valuesArray);

    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];

      if (!isNull(group)) {
        auto* accumulator = value(group);
        const vector_size_t size = accumulator->size();

        rawOffsets[i] = offset;
        rawSizes[i] = size;

        extractor->extractValues(accumulator, offset);

        offset += size;
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto rowVector = (*result)->as<RowVector>();
    auto nVector = rowVector->childAt(0);
    auto comparisonArray = rowVector->childAt(1)->as<ArrayVector>();
    auto valueArray = rowVector->childAt(2)->as<ArrayVector>();

    resizeRowVectorAndChildren(*rowVector, numGroups);

    auto* rawNs = nVector->as<FlatVector<int64_t>>()->mutableRawValues();

    const auto numValues =
        countValuesAndSetResultNulls(groups, numGroups, *result);

    auto values = valueArray->elements();
    auto comparisons = comparisonArray->elements();

    values->resize(numValues);
    comparisons->resize(numValues);

    std::optional<ExtractorType> extractor;
    if constexpr (
        std::is_same_v<V, StringView> || std::is_same_v<C, StringView> ||
        std::is_same_v<V, ComplexType>) {
      extractor.emplace(values, comparisons.get());
    } else {
      extractor.emplace(values);
    }

    TRawComparison* rawComparisons =
        RawValueExtractor<C>::mutableRawValues(comparisons);

    auto [rawValueOffsets, rawValueSizes] = rawOffsetAndSizes(*valueArray);
    auto [rawComparisonOffsets, rawComparisonSizes] =
        rawOffsetAndSizes(*comparisonArray);

    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];

      if (!isNull(group)) {
        auto* accumulator = value(group);
        const auto size = accumulator->size();

        rawNs[i] = accumulator->getN();

        rawValueOffsets[i] = offset;
        rawValueSizes[i] = size;

        rawComparisonOffsets[i] = offset;
        rawComparisonSizes[i] = size;

        if constexpr (
            std::is_same_v<V, StringView> || std::is_same_v<C, StringView> ||
            std::is_same_v<V, ComplexType>) {
          extractor->extractPairs(accumulator, offset);
        } else {
          extractor->extractPairs(accumulator, rawComparisons, offset);
        }

        offset += size;
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    decodedValue_.decode(*args[0], rows);
    decodedComparison_.decode(*args[1], rows);
    decodedN_.decode(*args[2], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedComparison_.isNullAt(i) || decodedN_.isNullAt(i)) {
        return;
      }

      auto* group = groups[i];

      auto* accumulator = value(group);
      accumulator->checkAndSetN(decodedN_, i);

      addRawInput(group, i);
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto results = decodeIntermediateResults(args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedIntermediates_.isNullAt(i)) {
        addIntermediateResults(groups[i], i, results);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    decodedValue_.decode(*args[0], rows);
    decodedComparison_.decode(*args[1], rows);

    auto* accumulator = value(group);
    validateN(args[2], rows, accumulator);

    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedComparison_.isNullAt(i) && !decodedN_.isNullAt(i)) {
        addRawInput(group, i);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto results = decodeIntermediateResults(args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (!decodedIntermediates_.isNullAt(i)) {
        addIntermediateResults(group, i, results);
      }
    });
  }

  void destroy(folly::Range<char**> groups) override {
    destroyAccumulators<AccumulatorType>(groups);
  }

 private:
  inline AccumulatorType* value(char* group) {
    return reinterpret_cast<AccumulatorType*>(group + Aggregate::offset_);
  }

  static std::optional<V> optionalValue(
      const DecodedVector& decoded,
      vector_size_t index) {
    std::optional<V> value;
    if (!decoded.isNullAt(index)) {
      value = decoded.valueAt<V>(index);
    }

    return value;
  }

  static std::optional<V> optionalValue(
      const FlatVector<V>& vector,
      vector_size_t index) {
    std::optional<V> value;
    if (!vector.isNullAt(index)) {
      value = vector.valueAt(index);
    }

    return value;
  }

  void addRawInput(char* group, vector_size_t index) {
    clearNull(group);

    auto* accumulator = value(group);

    const auto comparison = decodedComparison_.valueAt<C>(index);
    if constexpr (std::is_same_v<V, ComplexType>) {
      accumulator->compareAndAdd(comparison, decodedValue_, index, comparator_);
    } else {
      const auto value = optionalValue(decodedValue_, index);
      accumulator->compareAndAdd(comparison, value, comparator_);
    }
  }

  struct IntermediateResult {
    const ArrayVector* valueArray;
    // Used for complex types.
    DecodedVector values;
    // Used for primitive types.
    const FlatVector<V>* flatValues;
    const ArrayVector* comparisonArray;
    const FlatVector<C>* comparisons;
  };

  void addIntermediateResults(
      char* group,
      vector_size_t index,
      IntermediateResult& result) {
    clearNull(group);

    auto* accumulator = value(group);

    const auto decodedIndex = decodedIntermediates_.index(index);

    accumulator->checkAndSetN(decodedN_, decodedIndex);

    const auto* valueArray = result.valueArray;
    const auto* values = result.flatValues;
    const auto* comparisonArray = result.comparisonArray;
    const auto* comparisons = result.comparisons;

    const auto numValues = valueArray->sizeAt(decodedIndex);
    const auto valueOffset = valueArray->offsetAt(decodedIndex);
    const auto comparisonOffset = comparisonArray->offsetAt(decodedIndex);
    for (auto i = 0; i < numValues; ++i) {
      const auto comparison = comparisons->valueAt(comparisonOffset + i);
      if constexpr (std::is_same_v<V, ComplexType>) {
        accumulator->compareAndAdd(
            comparison, result.values, valueOffset + i, comparator_);
      } else {
        const auto value = optionalValue(*values, valueOffset + i);
        accumulator->compareAndAdd(comparison, value, comparator_);
      }
    }
  }

  IntermediateResult decodeIntermediateResults(
      const VectorPtr& arg,
      const SelectivityVector& rows) {
    decodedIntermediates_.decode(*arg, rows);

    auto baseRowVector =
        dynamic_cast<const RowVector*>(decodedIntermediates_.base());

    decodedN_.decode(*baseRowVector->childAt(0), rows);
    decodedComparison_.decode(*baseRowVector->childAt(1), rows);
    decodedValue_.decode(*baseRowVector->childAt(2), rows);

    IntermediateResult result{};
    result.valueArray = decodedValue_.base()->template as<ArrayVector>();
    result.comparisonArray =
        decodedComparison_.base()->template as<ArrayVector>();

    if constexpr (std::is_same_v<V, ComplexType>) {
      result.values.decode(*result.valueArray->elements());
    } else {
      result.flatValues =
          result.valueArray->elements()->template as<FlatVector<V>>();
    }
    result.comparisons =
        result.comparisonArray->elements()->template as<FlatVector<C>>();

    return result;
  }

  /// Return total number of values in all accumulators of specified 'groups'.
  /// Set null flags in 'result'.
  vector_size_t countValuesAndSetResultNulls(
      char** groups,
      int32_t numGroups,
      VectorPtr& result) {
    vector_size_t numValues = 0;

    uint64_t* rawNulls = getRawNulls(result.get());

    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];
      auto* accumulator = value(group);

      if (isNull(group)) {
        result->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        numValues += accumulator->size();
      }
    }

    return numValues;
  }

  void validateN(
      const VectorPtr& arg,
      const SelectivityVector& rows,
      AccumulatorType* accumulator) {
    decodedN_.decode(*arg, rows);
    if (decodedN_.isConstantMapping()) {
      accumulator->checkAndSetN(decodedN_, rows.begin());
    } else {
      rows.applyToSelected(
          [&](auto row) { accumulator->checkAndSetN(decodedN_, row); });
    }
  }

  Compare comparator_;
  DecodedVector decodedValue_;
  DecodedVector decodedComparison_;
  DecodedVector decodedN_;
  DecodedVector decodedIntermediates_;
};

template <typename V, typename C>
class MinByNAggregate : public MinMaxByNAggregate<V, C, Less<V, C>> {
 public:
  explicit MinByNAggregate(TypePtr resultType)
      : MinMaxByNAggregate<V, C, Less<V, C>>(resultType) {}
};

template <typename C>
class MinByNAggregate<ComplexType, C>
    : public MinMaxByNAggregate<
          ComplexType,
          C,
          Less<HashStringAllocator::Position, C>> {
 public:
  explicit MinByNAggregate(TypePtr resultType)
      : MinMaxByNAggregate<
            ComplexType,
            C,
            Less<HashStringAllocator::Position, C>>(resultType) {}
};

template <typename V, typename C>
class MaxByNAggregate : public MinMaxByNAggregate<V, C, Greater<V, C>> {
 public:
  explicit MaxByNAggregate(TypePtr resultType)
      : MinMaxByNAggregate<V, C, Greater<V, C>>(resultType) {}
};

template <typename C>
class MaxByNAggregate<ComplexType, C>
    : public MinMaxByNAggregate<
          ComplexType,
          C,
          Greater<HashStringAllocator::Position, C>> {
 public:
  explicit MaxByNAggregate(TypePtr resultType)
      : MinMaxByNAggregate<
            ComplexType,
            C,
            Greater<HashStringAllocator::Position, C>>(resultType) {}
};

template <template <typename U, typename V> class NAggregate, typename W>
std::unique_ptr<exec::Aggregate> createNArg(
    TypePtr resultType,
    TypePtr compareType,
    const std::string& errorMessage) {
  switch (compareType->kind()) {
    case TypeKind::BOOLEAN:
      return std::make_unique<NAggregate<W, bool>>(resultType);
    case TypeKind::TINYINT:
      return std::make_unique<NAggregate<W, int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<NAggregate<W, int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<NAggregate<W, int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<NAggregate<W, int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<NAggregate<W, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<NAggregate<W, double>>(resultType);
    case TypeKind::VARCHAR:
      return std::make_unique<NAggregate<W, StringView>>(resultType);
    case TypeKind::TIMESTAMP:
      return std::make_unique<NAggregate<W, Timestamp>>(resultType);
    default:
      VELOX_FAIL("{}", errorMessage);
      return nullptr;
  }
}

template <template <typename U, typename V> class NAggregate>
std::unique_ptr<exec::Aggregate> createNArg(
    TypePtr resultType,
    TypePtr valueType,
    TypePtr compareType,
    const std::string& errorMessage) {
  switch (valueType->kind()) {
    case TypeKind::BOOLEAN:
      return createNArg<NAggregate, bool>(
          resultType, compareType, errorMessage);
    case TypeKind::TINYINT:
      return createNArg<NAggregate, int8_t>(
          resultType, compareType, errorMessage);
    case TypeKind::SMALLINT:
      return createNArg<NAggregate, int16_t>(
          resultType, compareType, errorMessage);
    case TypeKind::INTEGER:
      return createNArg<NAggregate, int32_t>(
          resultType, compareType, errorMessage);
    case TypeKind::BIGINT:
      return createNArg<NAggregate, int64_t>(
          resultType, compareType, errorMessage);
    case TypeKind::REAL:
      return createNArg<NAggregate, float>(
          resultType, compareType, errorMessage);
    case TypeKind::DOUBLE:
      return createNArg<NAggregate, double>(
          resultType, compareType, errorMessage);
    case TypeKind::VARBINARY:
      [[fallthrough]];
    case TypeKind::VARCHAR:
      return createNArg<NAggregate, StringView>(
          resultType, compareType, errorMessage);
    case TypeKind::TIMESTAMP:
      return createNArg<NAggregate, Timestamp>(
          resultType, compareType, errorMessage);
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP:
      [[fallthrough]];
    case TypeKind::ROW:
      return createNArg<NAggregate, ComplexType>(
          resultType, compareType, errorMessage);
    default:
      VELOX_FAIL(errorMessage);
  }
}

std::string toString(const std::vector<TypePtr>& types) {
  std::ostringstream out;
  for (auto i = 0; i < types.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << types[i]->toString();
  }
  return out.str();
}

template <
    template <
        typename U,
        typename V,
        bool B1,
        template <bool B2, typename C1, typename C2>
        class C>
    class Aggregate,
    bool isMaxFunc,
    template <typename U, typename V>
    class NAggregate>
exec::AggregateRegistrationResult registerMinMaxBy(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  // V, C -> row(V, C) -> V.
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("V")
                           .typeVariable("C")
                           .returnType("V")
                           .intermediateType("row(V,C)")
                           .argumentType("V")
                           .argumentType("C")
                           .build());
  const std::vector<std::string> supportedCompareTypes = {
      "boolean",
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "real",
      "double",
      "varchar",
      "date",
      "timestamp"};

  // Add support for all value types to 3-arg version of the aggregate.
  for (const auto& compareType : supportedCompareTypes) {
    // V, C, bigint -> row(bigint, array(C), array(V)) -> array(V).
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .typeVariable("V")
                             .returnType("array(V)")
                             .intermediateType(fmt::format(
                                 "row(bigint,array({}),array(V))", compareType))
                             .argumentType("V")
                             .argumentType(compareType)
                             .argumentType("bigint")
                             .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        const auto isRawInput = exec::isRawInput(step);
        const std::string errorMessage = fmt::format(
            "Unknown input types for {} ({}) aggregation: {}",
            name,
            mapAggregationStepToName(step),
            toString(argTypes));

        const bool nAgg = (argTypes.size() == 3) ||
            (argTypes.size() == 1 && argTypes[0]->size() == 3);

        if (nAgg) {
          return createNArg<NAggregate>(
              resultType, argTypes[0], argTypes[1], errorMessage);
        } else {
          return create<Aggregate, Comparator, isMaxFunc>(
              resultType, argTypes[0], argTypes[1], errorMessage, true);
        }
      });
}

} // namespace

void registerMinMaxByAggregates(const std::string& prefix) {
  registerMinMaxBy<MinMaxByAggregateBase, true, MaxByNAggregate>(
      prefix + kMaxBy);
  registerMinMaxBy<MinMaxByAggregateBase, false, MinByNAggregate>(
      prefix + kMinBy);
}

} // namespace facebook::velox::aggregate::prestosql
