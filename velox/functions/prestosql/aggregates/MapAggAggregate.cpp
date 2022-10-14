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
#include "velox/functions/prestosql/aggregates/MapAggregateBase.h"

namespace facebook::velox::aggregate::prestosql {

namespace {
// See documentation at
// https://prestodb.io/docs/current/functions/aggregate.html
class MapAggAggregate : public aggregate::MapAggregateBase {
 public:
  explicit MapAggAggregate(TypePtr resultType) : MapAggregateBase(resultType) {}

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

} // namespace

void registerMapAggAggregate() {
  registerMapAggAggregate(kMapAgg);
}

} // namespace facebook::velox::aggregate::prestosql
