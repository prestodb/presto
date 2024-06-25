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
#include "velox/exec/AddressableNonNullValueList.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Strings.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <
    typename T,
    typename Hash = std::hash<T>,
    typename EqualTo = std::equal_to<T>>
struct Accumulator {
  using ValueMap = folly::F14FastMap<
      T,
      int64_t,
      Hash,
      EqualTo,
      AlignedStlAllocator<std::pair<const T, int64_t>, 16>>;

  ValueMap values;

  Accumulator(const TypePtr& /*type*/, HashStringAllocator* allocator)
      : values{
            AlignedStlAllocator<std::pair<const T, int64_t>, 16>(allocator)} {}

  Accumulator(Hash hash, EqualTo equalTo, HashStringAllocator* allocator)
      : values{
            0,
            hash,
            equalTo,
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

  void free(HashStringAllocator& allocator) {
    using TValues = decltype(values);
    values.~TValues();
  }
};

struct StringViewAccumulator {
  /// A map of unique StringViews pointing to storage managed by 'strings'.
  Accumulator<StringView> base;

  /// Stores unique non-null non-inline strings.
  Strings strings;

  StringViewAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{type, allocator} {}

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

  void free(HashStringAllocator& allocator) {
    strings.free(allocator);
    using Base = decltype(base);
    base.~Base();
  }
};

struct ComplexTypeAccumulator {
  Accumulator<
      AddressableNonNullValueList::Entry,
      AddressableNonNullValueList::Hash,
      AddressableNonNullValueList::EqualTo>
      base;

  AddressableNonNullValueList serializedValues;

  ComplexTypeAccumulator(const TypePtr& type, HashStringAllocator* allocator)
      : base{
            AddressableNonNullValueList::Hash{},
            AddressableNonNullValueList::EqualTo{type},
            allocator} {}

  size_t size() const {
    return base.size();
  }

  void addValue(
      DecodedVector& decoded,
      vector_size_t index,
      HashStringAllocator* allocator) {
    addValueWithCount(*decoded.base(), decoded.index(index), 1, allocator);
  }

  void addValueWithCount(
      const BaseVector& keys,
      vector_size_t index,
      int64_t count,
      HashStringAllocator* allocator) {
    auto entry = serializedValues.append(keys, index, allocator);

    auto it = base.values.find(entry);
    if (it == base.values.end()) {
      // New entry.
      base.values[entry] += count;
    } else {
      // Existing entry.
      serializedValues.removeLast(entry);

      it->second += count;
    }
  }

  void extractValues(
      BaseVector& keys,
      FlatVector<int64_t>& counts,
      vector_size_t offset) {
    auto index = offset;
    for (const auto& [position, count] : base.values) {
      AddressableNonNullValueList::read(position, keys, index);
      counts.set(index, count);
      ++index;
    }
  }

  void free(HashStringAllocator& allocator) {
    serializedValues.free(allocator);
    using Base = decltype(base);
    base.~Base();
  }
};

template <typename T>
struct AccumulatorTypeTraits {
  using AccumulatorType = Accumulator<T>;
};

template <>
struct AccumulatorTypeTraits<float> {
  using AccumulatorType = Accumulator<
      float,
      util::floating_point::NaNAwareHash<float>,
      util::floating_point::NaNAwareEquals<float>>;
};

template <>
struct AccumulatorTypeTraits<double> {
  using AccumulatorType = Accumulator<
      double,
      util::floating_point::NaNAwareHash<double>,
      util::floating_point::NaNAwareEquals<double>>;
};

template <>
struct AccumulatorTypeTraits<StringView> {
  using AccumulatorType = StringViewAccumulator;
};

template <>
struct AccumulatorTypeTraits<ComplexType> {
  using AccumulatorType = ComplexTypeAccumulator;
};

// Combines a partial aggregation represented by the key-value pair at row in
// mapKeys and mapValues into groupMap.
template <typename T, typename Accumulator>
FOLLY_ALWAYS_INLINE void addToFinalAggregation(
    const BaseVector* mapKeys,
    const FlatVector<int64_t>* flatValues,
    vector_size_t index,
    const vector_size_t* rawSizes,
    const vector_size_t* rawOffsets,
    Accumulator* accumulator,
    HashStringAllocator* allocator) {
  auto size = rawSizes[index];
  auto offset = rawOffsets[index];

  if constexpr (std::is_same_v<T, ComplexType>) {
    for (int i = 0; i < size; ++i) {
      const auto count = flatValues->valueAt(offset + i);
      accumulator->addValueWithCount(*mapKeys, offset + i, count, allocator);
    }
  } else {
    auto* flatKeys = mapKeys->asFlatVector<T>();
    for (int i = 0; i < size; ++i) {
      accumulator->addValueWithCount(
          flatKeys->valueAt(offset + i),
          flatValues->valueAt(offset + i),
          allocator);
    }
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

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* mapVector = (*result)->as<MapVector>();
    VELOX_CHECK(mapVector);
    mapVector->resize(numGroups);

    auto& mapKeys = mapVector->mapKeys();
    auto* flatValues =
        mapVector->mapValues()->asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(flatValues);

    const auto numElements = countElements(groups, numGroups);
    mapKeys->resize(numElements);
    flatValues->resize(numElements);

    auto* rawNulls = mapVector->mutableRawNulls();
    vector_size_t offset = 0;
    if constexpr (std::is_same_v<T, ComplexType>) {
      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        auto* accumulator = value<AccumulatorType>(group);

        const auto mapSize = accumulator->size();
        mapVector->setOffsetAndSize(i, offset, mapSize);
        if (mapSize == 0) {
          bits::setNull(rawNulls, i, true);
        } else {
          clearNull(rawNulls, i);
          accumulator->extractValues(*mapKeys, *flatValues, offset);
          offset += mapSize;
        }
      }
    } else {
      auto* flatKeys = mapKeys->asFlatVector<T>();
      for (auto i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        auto* accumulator = value<AccumulatorType>(group);

        const auto mapSize = accumulator->size();
        mapVector->setOffsetAndSize(i, offset, mapSize);
        if (mapSize == 0) {
          bits::setNull(rawNulls, i, true);
        } else {
          clearNull(rawNulls, i);
          accumulator->extractValues(*flatKeys, *flatValues, offset);
          offset += mapSize;
        }
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
    auto* indices = decodedIntermediate_.indices();
    auto* mapVector = decodedIntermediate_.base()->template as<MapVector>();

    auto& mapKeys = mapVector->mapKeys();
    auto* flatValues =
        mapVector->mapValues()->template asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(flatValues);

    auto rawSizes = mapVector->rawSizes();
    auto rawOffsets = mapVector->rawOffsets();
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        auto group = groups[row];
        auto* accumulator = value<AccumulatorType>(group);

        auto tracker = trackRowSize(group);
        addToFinalAggregation<T, AccumulatorType>(
            mapKeys.get(),
            flatValues,
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
    auto* indices = decodedIntermediate_.indices();
    auto* mapVector = decodedIntermediate_.base()->template as<MapVector>();

    auto& mapKeys = mapVector->mapKeys();
    auto* flatValues =
        mapVector->mapValues()->template asUnchecked<FlatVector<int64_t>>();
    VELOX_CHECK_NOT_NULL(mapKeys);
    VELOX_CHECK_NOT_NULL(flatValues);

    auto* accumulator = value<AccumulatorType>(group);

    auto tracker = trackRowSize(group);

    auto* rawSizes = mapVector->rawSizes();
    auto* rawOffsets = mapVector->rawOffsets();
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        addToFinalAggregation<T, AccumulatorType>(
            mapKeys.get(),
            flatValues,
            indices[row],
            rawSizes,
            rawOffsets,
            accumulator,
            allocator_);
      }
    });
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    const auto& type = resultType()->childAt(0);
    for (auto index : indices) {
      new (groups[index] + offset_) AccumulatorType{type, allocator_};
    }
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto* group : groups) {
      if (isInitialized(group) && !isNull(group)) {
        value<AccumulatorType>(group)->free(*allocator_);
      }
    }
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
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("map(T,bigint)")
          .intermediateType("map(T,bigint)")
          .argumentType("T")
          .build(),
  };

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
          case TypeKind::VARBINARY:
            return std::make_unique<HistogramAggregate<StringView>>(resultType);
          case TypeKind::ARRAY:
          case TypeKind::MAP:
          case TypeKind::ROW:
            return std::make_unique<HistogramAggregate<ComplexType>>(
                resultType);
          case TypeKind::UNKNOWN:
            return std::make_unique<HistogramAggregate<int8_t>>(resultType);
          default:
            VELOX_NYI(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->toString());
        }
      },
      {false /*orderSensitive*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
