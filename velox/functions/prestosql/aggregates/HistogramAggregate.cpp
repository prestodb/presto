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
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T>
using ValueMap = folly::F14FastMap<
    T,
    int64_t,
    std::hash<T>,
    std::equal_to<T>,
    AlignedStlAllocator<std::pair<const T, int64_t>, 16>>;

// Combines a partial aggregation represented by the key-value pair at row in
// mapKeys and mapValues into groupMap.
template <typename T>
FOLLY_ALWAYS_INLINE void addToFinalAggregation(
    const FlatVector<T>* mapKeys,
    const FlatVector<int64_t>* mapValues,
    const vector_size_t* indices,
    const vector_size_t* rawSizes,
    const vector_size_t* rawOffsets,
    vector_size_t row,
    ValueMap<T>* groupMap) {
  auto size = rawSizes[indices[row]];
  auto offset = rawOffsets[indices[row]];
  for (int i = 0; i < size; ++i) {
    (*groupMap)[mapKeys->valueAt(offset + i)] += mapValues->valueAt(offset + i);
  }
}

template <typename T>
class HistogramAggregate : public exec::Aggregate {
 public:
  explicit HistogramAggregate(TypePtr resultType)
      : Aggregate(std::move(resultType)) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ValueMap<T>);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_) ValueMap<T>{
          AlignedStlAllocator<std::pair<const T, int64_t>, 16>(allocator_)};
    }
  }

  void finalize(char** /*groups*/, int32_t /*numGroups*/) override {}

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
    vector_size_t index = 0;
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto groupMap = value<ValueMap<T>>(group);

      auto mapSize = groupMap->size();
      if (mapSize == 0) {
        bits::setNull(rawNulls, i, true);
      } else {
        for (auto it = groupMap->begin(); it != groupMap->end(); ++it) {
          mapKeys->set(index, it->first);
          mapValues->set(index, it->second);

          ++index;
        }
      }
      mapVector->setOffsetAndSize(i, index - mapSize, mapSize);
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
      // Nulls among the values being aggregated are ignored.
      if (!decodedKeys_.isNullAt(row)) {
        auto group = groups[row];
        auto groupMap = value<ValueMap<T>>(group);

        (*groupMap)[decodedKeys_.valueAt<T>(row)]++;
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedKeys_.decode(*args[0], rows);
    auto groupMap = value<ValueMap<T>>(group);
    rows.applyToSelected([&](auto row) {
      // Nulls among the values being aggregated are ignored.
      if (!decodedKeys_.isNullAt(row)) {
        (*groupMap)[decodedKeys_.valueAt<T>(row)]++;
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
        auto groupMap = value<ValueMap<T>>(group);

        addToFinalAggregation<T>(
            mapKeys, mapValues, indices, rawSizes, rawOffsets, row, groupMap);
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

    auto groupMap = value<ValueMap<T>>(group);

    auto rawSizes = mapVector->rawSizes();
    auto rawOffsets = mapVector->rawOffsets();
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedIntermediate_.isNullAt(row)) {
        addToFinalAggregation<T>(
            mapKeys, mapValues, indices, rawSizes, rawOffsets, row, groupMap);
      }
    });
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      auto groupMap = value<ValueMap<T>>(group);
      std::destroy_at(groupMap);
    }
  }

 private:
  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<ValueMap<T>>(groups[i])->size();
    }
    return size;
  }

  DecodedVector decodedKeys_;
  DecodedVector decodedIntermediate_;
};

bool registerHistogramAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("map(T,bigint)")
          .intermediateType("map(T,bigint)")
          .argumentType("T")
          .build()};

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(),
            1,
            "{} ({}): unexpected number of arguments",
            name);

        auto inputType = argTypes[0];
        switch (exec::isRawInput(step) ? inputType->kind()
                                       : inputType->childAt(0)->kind()) {
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
          case TypeKind::DATE:
            return std::make_unique<HistogramAggregate<Date>>(resultType);
          default:
            VELOX_NYI(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      });
  return true;
}

} // namespace

void registerHistogramAggregate() {
  registerHistogramAggregate(kHistogram);
}

} // namespace facebook::velox::aggregate::prestosql
