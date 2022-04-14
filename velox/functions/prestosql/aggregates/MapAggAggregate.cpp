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
#include "velox/exec/ContainerRowSerde.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/ValueList.h"

namespace facebook::velox::aggregate {
namespace {

struct MapAccumulator {
  ValueList keys;
  ValueList values;
};

// See documentation at
// https://prestodb.io/docs/current/functions/aggregate.html
class MapAggAggregate : public exec::Aggregate {
 public:
  explicit MapAggAggregate(TypePtr resultType) : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(MapAccumulator);
  }

  bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto index : indices) {
      new (groups[index] + offset_) MapAccumulator();
    }
  }

  void finalize(char** groups, int32_t numGroups) override {
    for (auto i = 0; i < numGroups; i++) {
      value<MapAccumulator>(groups[i])->keys.finalize(allocator_);
      value<MapAccumulator>(groups[i])->values.finalize(allocator_);
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto mapVector = (*result)->as<MapVector>();
    VELOX_CHECK(mapVector);
    mapVector->resize(numGroups);

    auto mapKeys = mapVector->mapKeys();
    auto mapValues = mapVector->mapValues();

    auto numElements = countElements(groups, numGroups);
    mapKeys->resize(numElements);
    mapValues->resize(numElements);

    auto* rawNulls = getRawNulls(mapVector);
    vector_size_t offset = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      clearNull(rawNulls, i);

      auto accumulator = value<MapAccumulator>(group);
      auto mapSize = accumulator->keys.size();
      if (mapSize) {
        ValueListReader keysReader(accumulator->keys);
        ValueListReader valuesReader(accumulator->values);
        for (auto index = 0; index < mapSize; ++index) {
          keysReader.next(*mapKeys, offset + index);
          valuesReader.next(*mapValues, offset + index);
        }
        mapVector->setOffsetAndSize(i, offset, mapSize);
        offset += mapSize;
      } else {
        mapVector->setOffsetAndSize(i, offset, 0);
      }
    }

    // canonicalize requires a singly referenced MapVector. std::move
    // inside the cast does not clear *result, so we clear this
    // manually.
    auto mapVectorPtr = std::static_pointer_cast<MapVector>(std::move(*result));
    *result = nullptr;
    *result = removeDuplicates(mapVectorPtr);
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
    decodedValues_.decode(*args[1], rows);

    rows.applyToSelected([&](vector_size_t row) {
      // Skip null keys
      if (!decodedKeys_.isNullAt(row)) {
        auto group = groups[row];
        auto accumulator = value<MapAccumulator>(group);
        auto tracker = trackRowSize(group);
        accumulator->keys.appendValue(decodedKeys_, row, allocator_);
        accumulator->values.appendValue(decodedValues_, row, allocator_);
      }
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedIntermediate_.decode(*args[0], rows);

    auto mapVector = decodedIntermediate_.base()->as<MapVector>();
    auto& mapKeys = mapVector->mapKeys();
    auto& mapValues = mapVector->mapValues();
    rows.applyToSelected([&](vector_size_t row) {
      auto group = groups[row];
      auto accumulator = value<MapAccumulator>(group);

      auto decodedRow = decodedIntermediate_.index(row);
      auto offset = mapVector->offsetAt(decodedRow);
      auto size = mapVector->sizeAt(decodedRow);
      auto tracker = trackRowSize(group);
      accumulator->keys.appendRange(mapKeys, offset, size, allocator_);
      accumulator->values.appendRange(mapValues, offset, size, allocator_);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    auto accumulator = value<MapAccumulator>(group);
    auto& keys = accumulator->keys;
    auto& values = accumulator->values;

    decodedKeys_.decode(*args[0], rows);
    decodedValues_.decode(*args[1], rows);
    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t row) {
      // Skip null keys
      if (!decodedKeys_.isNullAt(row)) {
        keys.appendValue(decodedKeys_, row, allocator_);
        values.appendValue(decodedValues_, row, allocator_);
      }
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    auto accumulator = value<MapAccumulator>(group);
    auto& keys = accumulator->keys;
    auto& values = accumulator->values;

    VELOX_CHECK_EQ(args[0]->encoding(), VectorEncoding::Simple::MAP);
    auto mapVector = args[0]->as<MapVector>();
    auto& mapKeys = mapVector->mapKeys();
    auto& mapValues = mapVector->mapValues();
    auto tracker = trackRowSize(group);
    rows.applyToSelected([&](vector_size_t row) {
      auto offset = mapVector->offsetAt(row);
      auto size = mapVector->sizeAt(row);
      keys.appendRange(mapKeys, offset, size, allocator_);
      values.appendRange(mapValues, offset, size, allocator_);
    });
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      auto accumulator = value<MapAccumulator>(group);
      accumulator->keys.free(allocator_);
      accumulator->values.free(allocator_);
    }
  }

 private:
  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<MapAccumulator>(groups[i])->keys.size();
    }
    return size;
  }

  VectorPtr removeDuplicates(MapVectorPtr& mapVector) const {
    MapVector::canonicalize(mapVector);

    auto offsets = mapVector->rawOffsets();
    auto sizes = mapVector->rawSizes();
    auto mapKeys = mapVector->mapKeys();

    auto numRows = mapVector->size();
    auto numElements = mapKeys->size();

    BufferPtr newSizes;
    vector_size_t* rawNewSizes = nullptr;

    BufferPtr elementIndices;
    vector_size_t* rawElementIndices = nullptr;

    // Check for duplicate keys
    for (vector_size_t row = 0; row < numRows; row++) {
      auto offset = offsets[row];
      auto size = sizes[row];
      auto duplicateCnt = 0;
      for (vector_size_t i = 1; i < size; i++) {
        if (mapKeys->equalValueAt(mapKeys.get(), offset + i, offset + i - 1)) {
          // duplicate key
          duplicateCnt++;
          if (!rawNewSizes) {
            newSizes =
                allocateSizes(mapVector->mapKeys()->size(), mapVector->pool());
            rawNewSizes = newSizes->asMutable<vector_size_t>();

            elementIndices = allocateIndices(
                mapVector->mapKeys()->size(), mapVector->pool());
            rawElementIndices = elementIndices->asMutable<vector_size_t>();

            memcpy(rawNewSizes, sizes, row * sizeof(vector_size_t));
            std::iota(rawElementIndices, rawElementIndices + offset + i, 0);
          }
        } else if (rawNewSizes) {
          rawElementIndices[offset + i - duplicateCnt] = offset + i;
        }
      }
      if (rawNewSizes) {
        rawNewSizes[row] = size - duplicateCnt;
      }
    };

    if (rawNewSizes) {
      return std::make_shared<MapVector>(
          mapVector->pool(),
          mapVector->type(),
          mapVector->nulls(),
          mapVector->size(),
          mapVector->offsets(),
          newSizes,
          BaseVector::wrapInDictionary(
              BufferPtr(nullptr), elementIndices, numElements, mapKeys),
          BaseVector::wrapInDictionary(
              BufferPtr(nullptr),
              elementIndices,
              numElements,
              mapVector->mapValues()));
    } else {
      return mapVector;
    }
  }

  DecodedVector decodedKeys_;
  DecodedVector decodedValues_;
  DecodedVector decodedIntermediate_;
};

bool registerMapAggAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("K")
          .typeVariable("V")
          .returnType("map(K,V)")
          .intermediateType("map(K,V)")
          .argumentType("K")
          .argumentType("V")
          .build()};

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        auto rawInput = exec::isRawInput(step);
        VELOX_CHECK_EQ(
            argTypes.size(),
            rawInput ? 2 : 1,
            "{} ({}): unexpected number of arguments",
            name);
        return std::make_unique<MapAggAggregate>(resultType);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerMapAggAggregate(kMapAgg);
} // namespace
} // namespace facebook::velox::aggregate
