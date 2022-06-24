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

#include "velox/exec/ContainerRowSerde.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/ValueList.h"

namespace facebook::velox::aggregate {
struct MapAccumulator {
  ValueList keys;
  ValueList values;
};

class MapAggregateBase : public exec::Aggregate {
 public:
  explicit MapAggregateBase(TypePtr resultType) : Aggregate(resultType) {}

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
      override;

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addMapInputToAccumulator(groups, rows, args, false);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addSingleGroupMapInputToAccumulator(group, rows, args, false);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      auto accumulator = value<MapAccumulator>(group);
      accumulator->keys.free(allocator_);
      accumulator->values.free(allocator_);
    }
  }

 protected:
  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<MapAccumulator>(groups[i])->keys.size();
    }
    return size;
  }

  VectorPtr removeDuplicates(MapVectorPtr& mapVector) const;

  void addMapInputToAccumulator(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown);

  void addSingleGroupMapInputToAccumulator(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown);

  DecodedVector decodedKeys_;
  DecodedVector decodedValues_;
  DecodedVector decodedMaps_;
};
} // namespace facebook::velox::aggregate
