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

#include <cmath>
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

using namespace facebook::velox::exec;

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename TInput, typename TResult>
class GeometricMeanAggregate {
 public:
  using InputType = Row<TInput>;

  using IntermediateType =
      Row</*logSum*/ double,
          /*count*/ int64_t>;

  using OutputType = TResult;

  static bool toIntermediate(
      exec::out_type<Row<double, int64_t>>& out,
      exec::arg_type<TInput> in) {
    out.copy_from(std::make_tuple(std::log(in), 1));
    return true;
  }

  struct AccumulatorType {
    // Sum of `log(value)`.
    double logSum_{0};
    // Count of values.
    int64_t count_{0};

    AccumulatorType() = delete;

    explicit AccumulatorType(HashStringAllocator* /*allocator*/) {}

    void addInput(
        HashStringAllocator* /*allocator*/,
        exec::arg_type<TInput> data) {
      logSum_ += std::log(data);
      count_ = checkedPlus<int64_t>(count_, 1);
    }

    void combine(
        HashStringAllocator* /*allocator*/,
        exec::arg_type<Row<double, int64_t>> other) {
      VELOX_CHECK(other.at<0>().has_value());
      VELOX_CHECK(other.at<1>().has_value());
      logSum_ += other.at<0>().value();
      count_ = checkedPlus<int64_t>(count_, other.at<1>().value());
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      out = std::exp(logSum_ / count_);
      return true;
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      out = std::make_tuple(logSum_, count_);
      return true;
    }
  };
};

} // namespace

void registerGeometricMeanAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  const std::string name = prefix + kGeometricMean;

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : {"bigint", "double"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(double,bigint)")
                             .argumentType(inputType)
                             .build());
  }

  // Register for real input type.
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("real")
                           .intermediateType("row(double,bigint)")
                           .argumentType("real")
                           .build());

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
          case TypeKind::BIGINT:
            return std::make_unique<SimpleAggregateAdapter<
                GeometricMeanAggregate<int64_t, double>>>(resultType);
          case TypeKind::DOUBLE:
            return std::make_unique<
                SimpleAggregateAdapter<GeometricMeanAggregate<double, double>>>(
                resultType);
          case TypeKind::REAL:
            return std::make_unique<
                SimpleAggregateAdapter<GeometricMeanAggregate<float, float>>>(
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
