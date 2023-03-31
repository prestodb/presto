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
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/CheckedArithmeticImpl.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename K, typename S>
class MapUnionSumAggregate : public exec::Aggregate {
 public:
  explicit MapUnionSumAggregate(TypePtr resultType)
      : Aggregate(std::move(resultType)) {}

  using Accumulator = folly::F14FastMap<
      K,
      S,
      std::hash<K>,
      std::equal_to<K>,
      AlignedStlAllocator<std::pair<const K, S>, 16>>;

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(Accumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto index : indices) {
      new (groups[index] + offset_) Accumulator{
          AlignedStlAllocator<std::pair<const K, S>, 16>(allocator_)};
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto mapVector = (*result)->as<MapVector>();
    VELOX_CHECK(mapVector);
    mapVector->resize(numGroups);

    auto mapKeys = mapVector->mapKeys()->as<FlatVector<K>>();
    auto mapValues = mapVector->mapValues()->as<FlatVector<S>>();

    auto numElements = countElements(groups, numGroups);
    mapKeys->resize(numElements);
    mapValues->resize(numElements);

    auto rawNulls = mapVector->mutableRawNulls();
    vector_size_t index = 0;
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        bits::setNull(rawNulls, i, true);
        mapVector->setOffsetAndSize(i, 0, 0);
      } else {
        clearNull(rawNulls, i);

        auto groupMap = value<Accumulator>(group);
        auto mapSize = groupMap->size();
        for (auto it = groupMap->begin(); it != groupMap->end(); ++it) {
          mapKeys->set(index, it->first);
          mapValues->set(index, it->second);

          ++index;
        }
        mapVector->setOffsetAndSize(i, index - mapSize, mapSize);
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
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->template as<MapVector>();
    auto mapKeys = mapVector->mapKeys()->template as<SimpleVector<K>>();
    auto mapValues = mapVector->mapValues()->template as<SimpleVector<S>>();

    rows.applyToSelected([&](auto row) {
      if (!decodedMaps_.isNullAt(row)) {
        clearNull(groups[row]);
        auto groupMap = value<Accumulator>(groups[row]);
        addMap(*groupMap, mapVector, mapKeys, mapValues, row);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->template as<MapVector>();
    auto mapKeys = mapVector->mapKeys()->template as<SimpleVector<K>>();
    auto mapValues = mapVector->mapValues()->template as<SimpleVector<S>>();

    auto groupMap = value<Accumulator>(group);

    rows.applyToSelected([&](auto row) {
      if (!decodedMaps_.isNullAt(row)) {
        clearNull(group);
        addMap(*groupMap, mapVector, mapKeys, mapValues, row);
      }
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addRawInput(groups, rows, args, false);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    addSingleGroupRawInput(group, rows, args, false);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      auto groupMap = value<Accumulator>(group);
      std::destroy_at(groupMap);
    }
  }

 private:
  void addMap(
      Accumulator& groupMap,
      const MapVector* mapVector,
      const SimpleVector<K>* mapKeys,
      const SimpleVector<S>* mapValues,
      vector_size_t row) const {
    auto decodedRow = decodedMaps_.index(row);
    auto offset = mapVector->offsetAt(decodedRow);
    auto size = mapVector->sizeAt(decodedRow);

    for (auto i = 0; i < size; ++i) {
      // Ignore null map keys.
      if (!mapKeys->isNullAt(offset + i)) {
        auto key = mapKeys->valueAt(offset + i);
        if (mapValues->isNullAt(offset + i)) {
          groupMap[key] += 0;
        } else {
          auto value = mapValues->valueAt(offset + i);

          if constexpr (std::is_same_v<S, double> || std::is_same_v<S, float>) {
            groupMap[key] += value;
          } else {
            groupMap[key] = functions::checkedPlus<S>(groupMap[key], value);
          }
        }
      }
    }
  }

  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<Accumulator>(groups[i])->size();
    }
    return size;
  }

  DecodedVector decodedMaps_;
};

template <typename K>
std::unique_ptr<exec::Aggregate> createMapUnionSumAggregate(
    TypeKind valueKind,
    const TypePtr& resultType) {
  switch (valueKind) {
    case TypeKind::TINYINT:
      return std::make_unique<MapUnionSumAggregate<K, int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<MapUnionSumAggregate<K, int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<MapUnionSumAggregate<K, int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<MapUnionSumAggregate<K, int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<MapUnionSumAggregate<K, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<MapUnionSumAggregate<K, double>>(resultType);
    default:
      VELOX_UNREACHABLE();
  }
}

bool registerMapUnionSum(const std::string& name) {
  // TODO Add support for keys of type VARCHAR.

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (auto keyType : {"tinyint", "smallint", "integer", "bigint"}) {
    for (auto valueType :
         {"tinyint", "smallint", "integer", "bigint", "double", "real"}) {
      auto mapType = fmt::format("map({},{})", keyType, valueType);
      signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                               .returnType(mapType)
                               .intermediateType(mapType)
                               .argumentType(mapType)
                               .build());
    }
  }

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1);
        VELOX_CHECK(argTypes[0]->isMap());
        auto& mapType = argTypes[0]->asMap();
        auto keyTypeKind = mapType.keyType()->kind();
        auto valueTypeKind = mapType.valueType()->kind();
        switch (keyTypeKind) {
          case TypeKind::TINYINT:
            return createMapUnionSumAggregate<int8_t>(
                valueTypeKind, resultType);
          case TypeKind::SMALLINT:
            return createMapUnionSumAggregate<int16_t>(
                valueTypeKind, resultType);
          case TypeKind::INTEGER:
            return createMapUnionSumAggregate<int32_t>(
                valueTypeKind, resultType);
          case TypeKind::BIGINT:
            return createMapUnionSumAggregate<int64_t>(
                valueTypeKind, resultType);
          default:
            VELOX_UNREACHABLE();
        }
      });
  return true;
}

} // namespace

void registerMapUnionSumAggregate(const std::string& prefix) {
  registerMapUnionSum(prefix + kMapUnionSum);
}

} // namespace facebook::velox::aggregate::prestosql
