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
template <typename K>
class MapAggAggregate : public MapAggregateBase<K> {
 public:
  explicit MapAggAggregate(TypePtr resultType)
      : MapAggregateBase<K>(std::move(resultType)) {}

  using Base = MapAggregateBase<K>;

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    const auto& keys = args[0];
    const auto& values = args[1];

    const auto numRows = rows.size();

    // Convert input to a single-entry map. Convert entries with null keys to
    // null maps.

    // Set nulls for rows not present in 'rows'.
    auto* pool = Base::allocator_->pool();
    BufferPtr nulls = allocateNulls(numRows, pool);
    auto* rawNulls = nulls->asMutable<uint64_t>();
    memcpy(rawNulls, rows.asRange().bits(), bits::nbytes(numRows));

    // Set nulls for rows with null keys.
    if (keys->mayHaveNulls()) {
      DecodedVector decodedKeys(*keys, rows);
      if (decodedKeys.mayHaveNulls()) {
        rows.applyToSelected([&](auto row) {
          if (decodedKeys.isNullAt(row)) {
            bits::setNull(rawNulls, row);
          }
        });
      }
    }

    // Set offsets to 0, 1, 2, 3...
    BufferPtr offsets = allocateOffsets(numRows, pool);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    std::iota(rawOffsets, rawOffsets + numRows, 0);

    // Set sizes to 1.
    BufferPtr sizes = allocateSizes(numRows, pool);
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    std::fill(rawSizes, rawSizes + numRows, 1);

    result = std::make_shared<MapVector>(
        pool,
        MAP(keys->type(), values->type()),
        nulls,
        numRows,
        offsets,
        sizes,
        BaseVector::loadedVectorShared(keys),
        BaseVector::loadedVectorShared(values));
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    Base::decodedKeys_.decode(*args[0], rows);
    Base::decodedValues_.decode(*args[1], rows);

    rows.applyToSelected([&](vector_size_t row) {
      // Skip null keys
      if (!Base::decodedKeys_.isNullAt(row)) {
        auto group = groups[row];
        Base::clearNull(group);
        auto tracker = Base::trackRowSize(group);
        Base::accumulator(group)->insert(
            Base::decodedKeys_, Base::decodedValues_, row, *Base::allocator_);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    auto singleAccumulator = Base::accumulator(group);

    Base::decodedKeys_.decode(*args[0], rows);
    Base::decodedValues_.decode(*args[1], rows);
    auto tracker = Base::trackRowSize(group);
    rows.applyToSelected([&](vector_size_t row) {
      // Skip null keys
      if (!Base::decodedKeys_.isNullAt(row)) {
        Base::clearNull(group);
        singleAccumulator->insert(
            Base::decodedKeys_, Base::decodedValues_, row, *Base::allocator_);
      }
    });
  }
};

exec::AggregateRegistrationResult registerMapAgg(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .knownTypeVariable("K")
          .typeVariable("V")
          .returnType("map(K,V)")
          .intermediateType("map(K,V)")
          .argumentType("K")
          .argumentType("V")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        auto rawInput = exec::isRawInput(step);
        VELOX_CHECK_EQ(
            argTypes.size(),
            rawInput ? 2 : 1,
            "{} ({}): unexpected number of arguments",
            name);

        return createMapAggregate<MapAggAggregate>(resultType);
      });
}

} // namespace

void registerMapAggAggregate(const std::string& prefix) {
  registerMapAgg(prefix + kMapAgg);
}

} // namespace facebook::velox::aggregate::prestosql
