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
class MapUnionAggregate : public MapAggregateBase<K> {
 public:
  explicit MapUnionAggregate(TypePtr resultType)
      : MapAggregateBase<K>(resultType) {}

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    if (rows.isAllSelected()) {
      result = args[0];
    } else {
      auto* pool = MapAggregateBase<K>::allocator_->pool();
      const auto numRows = rows.size();

      // Set nulls for rows not present in 'rows'.
      BufferPtr nulls = allocateNulls(numRows, pool);
      memcpy(
          nulls->asMutable<uint64_t>(),
          rows.asRange().bits(),
          bits::nbytes(numRows));

      BufferPtr indices = allocateIndices(numRows, pool);
      auto* rawIndices = indices->asMutable<vector_size_t>();
      std::iota(rawIndices, rawIndices + numRows, 0);
      result =
          BaseVector::wrapInDictionary(nulls, indices, rows.size(), args[0]);
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MapAggregateBase<K>::addMapInputToAccumulator(groups, rows, args, false);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    MapAggregateBase<K>::addSingleGroupMapInputToAccumulator(
        group, rows, args, false);
  }
};

} // namespace

void registerMapUnionAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("K")
          .typeVariable("V")
          .returnType("map(K,V)")
          .intermediateType("map(K,V)")
          .argumentType("map(K,V)")
          .build()};

  auto name = prefix + kMapUnion;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(),
            1,
            "{} ({}): unexpected number of arguments",
            name);

        return createMapAggregate<MapUnionAggregate>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
