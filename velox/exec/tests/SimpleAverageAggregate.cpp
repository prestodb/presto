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
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/VectorWriters.h"

using namespace facebook::velox::exec;

namespace facebook::velox::aggregate {

namespace {

// Implementation of the average aggregation function through the
// SimpleAggregateAdapter.
template <typename T>
class AverageAggregate {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<T>;

  // Type of intermediate result vector wrapped in Row.
  using IntermediateType =
      Row</*sum*/ double,
          /*count*/ int64_t>;

  // Type of output vector.
  using OutputType =
      std::conditional_t<std::is_same_v<T, float>, float, double>;

  static bool toIntermediate(
      exec::out_type<Row<double, int64_t>>& out,
      exec::arg_type<T> in) {
    out.copy_from(std::make_tuple(static_cast<double>(in), 1));
    return true;
  }

  struct AccumulatorType {
    double sum_;
    int64_t count_;

    AccumulatorType() = delete;

    // Constructor used in initializeNewGroups().
    explicit AccumulatorType(HashStringAllocator* /*allocator*/) {
      sum_ = 0;
      count_ = 0;
    }

    // addInput expects one parameter of exec::arg_type<T> for each child-type T
    // wrapped in InputType.
    void addInput(HashStringAllocator* /*allocator*/, exec::arg_type<T> data) {
      sum_ += data;
      count_ = checkedPlus<int64_t>(count_, 1);
    }

    // combine expects one parameter of exec::arg_type<IntermediateType>.
    void combine(
        HashStringAllocator* /*allocator*/,
        exec::arg_type<Row<double, int64_t>> other) {
      // Both field of an intermediate result should be non-null because
      // writeIntermediateResult() never make an intermediate result with a
      // single null.
      VELOX_CHECK(other.at<0>().has_value());
      VELOX_CHECK(other.at<1>().has_value());
      sum_ += other.at<0>().value();
      count_ = checkedPlus<int64_t>(count_, other.at<1>().value());
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      out = sum_ / count_;
      return true;
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      out = std::make_tuple(sum_, count_);
      return true;
    }
  };
};

} // namespace

exec::AggregateRegistrationResult registerSimpleAverageAggregate(
    const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : {"smallint", "integer", "bigint", "double"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(double,bigint)")
                             .argumentType(inputType)
                             .build());
  }

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("real")
                           .intermediateType("row(double,bigint)")
                           .argumentType("real")
                           .build());

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        auto inputType = argTypes[0];
        if (exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case TypeKind::SMALLINT:
              return std::make_unique<
                  SimpleAggregateAdapter<AverageAggregate<int16_t>>>(
                  resultType);
            case TypeKind::INTEGER:
              return std::make_unique<
                  SimpleAggregateAdapter<AverageAggregate<int32_t>>>(
                  resultType);
            case TypeKind::BIGINT:
              return std::make_unique<
                  SimpleAggregateAdapter<AverageAggregate<int64_t>>>(
                  resultType);
            case TypeKind::REAL:
              return std::make_unique<
                  SimpleAggregateAdapter<AverageAggregate<float>>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<
                  SimpleAggregateAdapter<AverageAggregate<double>>>(resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          switch (resultType->kind()) {
            case TypeKind::REAL:
              return std::make_unique<
                  SimpleAggregateAdapter<AverageAggregate<float>>>(resultType);
            case TypeKind::DOUBLE:
            case TypeKind::ROW:
              return std::make_unique<
                  SimpleAggregateAdapter<AverageAggregate<double>>>(resultType);
            default:
              VELOX_FAIL(
                  "Unsupported result type for final aggregation: {}",
                  resultType->kindName());
          }
        }
      },
      true /*registerCompanionFunctions*/,
      true /*overwrite*/);
}

} // namespace facebook::velox::aggregate
