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

#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

using namespace facebook::velox::exec;

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T>
class BitwiseXorAggregate {
 public:
  using InputType = Row<T>;

  using IntermediateType = T;

  using OutputType = T;

  static bool toIntermediate(exec::out_type<T>& out, exec::arg_type<T> in) {
    out = in;
    return true;
  }

  struct AccumulatorType {
    T xor_{0};

    AccumulatorType() = delete;

    explicit AccumulatorType(HashStringAllocator* /*allocator*/) {}

    void addInput(HashStringAllocator* /*allocator*/, exec::arg_type<T> data) {
      xor_ ^= data;
    }

    void combine(HashStringAllocator* /*allocator*/, exec::arg_type<T> other) {
      xor_ ^= other;
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      out = xor_;
      return true;
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      out = xor_;
      return true;
    }
  };
};

} // namespace

void registerBitwiseXorAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool onlyPrestoSignatures,
    bool overwrite) {
  const std::string name = prefix + kBitwiseXor;

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  std::vector<std::string> typeList{"tinyint", "smallint", "integer", "bigint"};
  if (onlyPrestoSignatures) {
    typeList = {"bigint"};
  }
  for (const auto& inputType : typeList) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType(inputType)
                             .argumentType(inputType)
                             .build());
  }

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_USER_CHECK_EQ(argTypes.size(), 1, "{} takes one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<
                SimpleAggregateAdapter<BitwiseXorAggregate<int8_t>>>(
                resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<
                SimpleAggregateAdapter<BitwiseXorAggregate<int16_t>>>(
                resultType);
          case TypeKind::INTEGER:
            return std::make_unique<
                SimpleAggregateAdapter<BitwiseXorAggregate<int32_t>>>(
                resultType);
          case TypeKind::BIGINT:
            return std::make_unique<
                SimpleAggregateAdapter<BitwiseXorAggregate<int64_t>>>(
                resultType);
          default:
            VELOX_USER_FAIL(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->toString());
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
