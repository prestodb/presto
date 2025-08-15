/*
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

#include "DataSketches/theta_sketch.hpp"
#include "DataSketches/theta_union.hpp"
#include "velox/exec/Aggregate.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace datasketches;

namespace facebook::presto::functions::aggregate {

namespace {

static const char* const kThetaSketch = "sketch_theta";

template <typename T>
class ThetaSketchAggregate {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = Row<T>;

  // Type of intermediate result
  using IntermediateType = Varbinary;

  // Type of output vector.
  using OutputType = Varbinary;

  static constexpr bool default_null_behavior_ = false;

  static bool toIntermediate(
      exec::out_type<IntermediateType>& out,
      exec::optional_arg_type<T> in) {
    if (!in.has_value())
      return true;
    auto updateSketch = update_theta_sketch::builder().build();
    updateSketch.update(in.value());
    theta_union theta_union = theta_union::builder().build();
    theta_union.update(updateSketch);
    auto compactSketch = theta_union.get_result();
    out.resize(compactSketch.get_serialized_size_bytes());
    auto serializedBytes = compactSketch.serialize();
    std::memcpy(out.data(), serializedBytes.data(), out.size());
    return true;
  }

  struct AccumulatorType {
    theta_union thetaUnion = theta_union::builder().build();
    update_theta_sketch updateSketch = update_theta_sketch::builder().build();

    AccumulatorType() = delete;

    // Constructor used in initializeNewGroups().
    explicit AccumulatorType(
        HashStringAllocator* /*allocator*/,
        ThetaSketchAggregate* /*fn*/) {}

    // addInput expects one parameter of exec::arg_type<T> for each child-type T
    // wrapped in InputType.
    bool addInput(
        HashStringAllocator* /*allocator*/,
        exec::optional_arg_type<T> data) {
      if (!data.has_value())
        return true;
      updateSketch.update(data.value());
      return true;
    }

    // combine expects one parameter of exec::arg_type<IntermediateType>.
    bool combine(
        HashStringAllocator* /*allocator*/,
        exec::optional_arg_type<Varbinary> other) {
      if (!other.has_value())
        return true;
      thetaUnion.update(updateSketch);
      auto compactSketch =
          wrapped_compact_theta_sketch::wrap(other->data(), other->size());
      thetaUnion.update(compactSketch);
      updateSketch.reset();
      return true;
    }

    bool writeFinalResult(bool nonNullGroup, exec::out_type<Varbinary>& out) {
      thetaUnion.update(updateSketch);
      auto compactSketch = thetaUnion.get_result();
      out.resize(compactSketch.get_serialized_size_bytes());
      auto serializedBytes = compactSketch.serialize();
      std::memcpy(out.data(), serializedBytes.data(), out.size());
      updateSketch.reset();
      return true;
    }

    bool writeIntermediateResult(
        bool nonNullGroup,
        exec::out_type<Varbinary>& out) {
      thetaUnion.update(updateSketch);
      auto compactSketch = thetaUnion.get_result();
      out.resize(compactSketch.get_serialized_size_bytes());
      auto serializedBytes = compactSketch.serialize();
      std::memcpy(out.data(), serializedBytes.data(), out.size());
      updateSketch.reset();
      return true;
    }
  };
};

} // namespace

exec::AggregateRegistrationResult registerThetaSketchAggregate(
    const std::string& prefix,
    bool withCompanionFunctions = true,
    bool overwrite = false) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType :
       {"smallint", "integer", "bigint", "real", "double", "varchar"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("varbinary")
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .build());
  }

  auto name = prefix + kThetaSketch;

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
                  SimpleAggregateAdapter<ThetaSketchAggregate<int16_t>>>(
                  step, argTypes, resultType);
            case TypeKind::INTEGER:
              return std::make_unique<
                  SimpleAggregateAdapter<ThetaSketchAggregate<int32_t>>>(
                  step, argTypes, resultType);
            case TypeKind::BIGINT:
              return std::make_unique<
                  SimpleAggregateAdapter<ThetaSketchAggregate<int64_t>>>(
                  step, argTypes, resultType);
            case TypeKind::REAL:
              return std::make_unique<
                  SimpleAggregateAdapter<ThetaSketchAggregate<float>>>(
                  step, argTypes, resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<
                  SimpleAggregateAdapter<ThetaSketchAggregate<double>>>(
                  step, argTypes, resultType);
            case TypeKind::VARCHAR:
              return std::make_unique<
                  SimpleAggregateAdapter<ThetaSketchAggregate<std::string>>>(
                  step, argTypes, resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          return std::make_unique<
              SimpleAggregateAdapter<ThetaSketchAggregate<Varbinary>>>(
              step, argTypes, resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::presto::functions::aggregate
