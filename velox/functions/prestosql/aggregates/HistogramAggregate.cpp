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
#include "folly/container/F14Map.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Strings.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T>
struct Accumulator {
  using ValueMap = folly::F14FastMap<
      T,
      int64_t,
      std::hash<T>,
      std::equal_to<T>,
      AlignedStlAllocator<std::pair<const T, int64_t>, 16>>;
  ValueMap values;

  explicit Accumulator(HashStringAllocator* allocator)
      : values{
            AlignedStlAllocator<std::pair<const T, int64_t>, 16>(allocator)} {}

  size_t size() const {
    return values.size();
  }

  void addValue(
      DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* /*allocator*/) {
    values[decoded.valueAt<T>(index)]++;
  }

  void addValueWithCount(
      T value,
      int64_t count,
      HashStringAllocator* /*allocator*/) {
    values[value] += count;
  }

  void extractValues(
      FlatVector<T>& keys,
      FlatVector<int64_t>& counts,
      vector_size_t offset) {
    auto index = offset;
    for (const auto& [value, count] : values) {
      keys.set(index, value);
      counts.set(index, count);

      ++index;
    }
  }
};

struct StringViewAccumulator {
  /// A map of unique StringViews pointing to storage managed by 'strings'.
  Accumulator<StringView> base;

  /// Stores unique non-null non-inline strings.
  Strings strings;

  explicit StringViewAccumulator(HashStringAllocator* allocator)
      : base{allocator} {}

  size_t size() const {
    return base.size();
  }

  void addValue(
      DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    auto value = decoded.valueAt<StringView>(index);
    base.values[store(value, allocator)]++;
  }

  void addValueWithCount(
      StringView value,
      int64_t count,
      HashStringAllocator* allocator) {
    base.values[store(value, allocator)] += count;
  }

  StringView store(StringView value, HashStringAllocator* allocator) {
    if (!value.isInline()) {
      auto it = base.values.find(value);
      if (it != base.values.end()) {
        value = it->first;
      } else {
        value = strings.append(value, *allocator);
      }
    }
    return value;
  }

  void extractValues(
      FlatVector<StringView>& keys,
      FlatVector<int64_t>& counts,
      vector_size_t offset) {
    base.extractValues(keys, counts, offset);
  }
};

template <typename T>
struct AccumulatorTypeTraits {
  using AccumulatorType = Accumulator<T>;
};

template <>
struct AccumulatorTypeTraits<StringView> {
  using AccumulatorType = StringViewAccumulator;
};

// Combines a partial aggregation represented by the key-value pair at row in
// mapKeys and mapValues into groupMap.
template <typename T, typename Accumulator>
FOLLY_ALWAYS_INLINE void addToFinalAggregation(
    const FlatVector<T>* mapKeys,
    const FlatVector<int64_t>* mapValues,
    vector_size_t index,
    const vector_size_t* rawSizes,
    const vector_size_t* rawOffsets,
    Accumulator* accumulator,
    HashStringAllocator* allocator) {
  auto size = rawSizes[index];
  auto offset = rawOffsets[index];
  for (int i = 0; i < size; ++i) {
    accumulator->addValueWithCount(
        mapKeys->valueAt(offset + i),
        mapValues->valueAt(offset + i),
        allocator);
  }
}

template <typename T>
class HistogramAggregate : public exec::Aggregate {
 public:
  explicit HistogramAggregate(TypePtr resultType)
      : Aggregate(std::move(resultType)) {}

  using AccumulatorType = typename AccumulatorTypeTraits<T>::AccumulatorType;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_) AccumulatorType{allocator_};
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto mapVector = (*result)->as<MapVector>();
    VELOX_CHECK(mapVector);
    mapVector->resize(numGroups);

    auto mapKeys = mapVector->mapKeys()->asUnchecked<FlatVector<T>>();
    auto mapValues = mapVector->mapValues()->asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(mapValues);

    auto numElements = countElements(groups, numGroups);
    mapKeys->resize(numElements);
    mapValues->resize(numElements);

    auto rawNulls = mapVector->mutableRawNulls();
    vector_size_t offset = 0;
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto* accumulator = value<AccumulatorType>(group);

      auto mapSize = accumulator->size();
      mapVector->setOffsetAndSize(i, offset, mapSize);
      if (mapSize == 0) {
        bits::setNull(rawNulls, i, true);
      } else {
        clearNull(rawNulls, i);
        accumulator->extractValues(*mapKeys, *mapValues, offset);
        offset += mapSize;
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedKeys_.decode(*args[0], rows);

    rows.applyToSelected([&](auto row) {
      if (decodedKeys_.isNullAt(row)) {
        // Nulls among the values being aggregated are ignored.
        return;
      }
      auto group = groups[row];
      auto* accumulator = value<AccumulatorType>(group);

      auto tracker = trackRowSize(group);
      accumulator->addValue(decodedKeys_, row, allocator_);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedKeys_.decode(*args[0], rows);
    auto* accumulator = value<AccumulatorType>(group);

    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](auto row) {
      // Nulls among the values being aggregated are ignored.
      if (!decodedKeys_.isNullAt(row)) {
        accumulator->addValue(decodedKeys_, row, allocator_);
      }
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);
    auto indices = decodedIntermediate_.indices();
    auto mapVector = decodedIntermediate_.base()->template as<MapVector>();

    auto mapKeys = mapVector->mapKeys()->template asUnchecked<FlatVector<T>>();
    auto mapValues =
        mapVector->mapValues()->template asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(mapValues);

    auto rawSizes = mapVector->rawSizes();
    auto rawOffsets = mapVector->rawOffsets();
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        auto group = groups[row];
        auto* accumulator = value<AccumulatorType>(group);

        auto tracker = trackRowSize(group);
        addToFinalAggregation<T, AccumulatorType>(
            mapKeys,
            mapValues,
            indices[row],
            rawSizes,
            rawOffsets,
            accumulator,
            allocator_);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedIntermediate_.decode(*args[0], rows);
    auto indices = decodedIntermediate_.indices();
    auto mapVector = decodedIntermediate_.base()->template as<MapVector>();

    auto mapKeys = mapVector->mapKeys()->template asUnchecked<FlatVector<T>>();
    auto mapValues =
        mapVector->mapValues()->template asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(mapValues);

    auto* accumulator = value<AccumulatorType>(group);

    auto tracker = trackRowSize(group);

    auto rawSizes = mapVector->rawSizes();
    auto rawOffsets = mapVector->rawOffsets();
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        addToFinalAggregation<T, AccumulatorType>(
            mapKeys,
            mapValues,
            indices[row],
            rawSizes,
            rawOffsets,
            accumulator,
            allocator_);
      }
    });
  }

  void destroy(folly::Range<char**> groups) override {
    destroyAccumulators<AccumulatorType>(groups);
  }

 private:
  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<AccumulatorType>(groups[i])->size();
    }
    return size;
  }

  DecodedVector decodedKeys_;
  DecodedVector decodedIntermediate_;
};

} // namespace

void registerHistogramAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto inputType :
       {"boolean",
        "tinyint",
        "smallint",
        "integer",
        "bigint",
        "real",
        "double",
        "timestamp",
        "date",
        "interval day to second",
        "varchar"}) {
    auto mapType = fmt::format("map({},bigint)", inputType);
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(mapType)
                             .intermediateType(mapType)
                             .argumentType(inputType)
                             .build());
  }

  auto name = prefix + kHistogram;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(),
            1,
            "{} ({}): unexpected number of arguments",
            name);

        auto inputType = argTypes[0];
        switch (exec::isRawInput(step) ? inputType->kind()
                                       : inputType->childAt(0)->kind()) {
          case TypeKind::BOOLEAN:
            return std::make_unique<HistogramAggregate<bool>>(resultType);
          case TypeKind::TINYINT:
            return std::make_unique<HistogramAggregate<int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<HistogramAggregate<int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<HistogramAggregate<int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<HistogramAggregate<int64_t>>(resultType);
          case TypeKind::REAL:
            return std::make_unique<HistogramAggregate<float>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<HistogramAggregate<double>>(resultType);
          case TypeKind::TIMESTAMP:
            return std::make_unique<HistogramAggregate<Timestamp>>(resultType);
          case TypeKind::VARCHAR:
            return std::make_unique<HistogramAggregate<StringView>>(resultType);
          default:
            VELOX_NYI(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
