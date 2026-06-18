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

#include "presto_cpp/main/functions/theta_sketch/ThetaSketchRegistration.h"

#include "DataSketches/theta_sketch.hpp"
#include "DataSketches/theta_union.hpp"

#include "velox/exec/Aggregate.h"
#include "velox/exec/SimpleAggregateAdapter.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/type/HugeInt.h"

namespace facebook::presto::functions::aggregate {

namespace {

const char* const kThetaSketch = "sketch_theta";

template <typename T>
class ThetaSketchAggregate {
 public:
  // Type(s) of input vector(s) wrapped in Row.
  using InputType = velox::Row<T>;

  // Type of intermediate result
  using IntermediateType = velox::Varbinary;

  // Type of output vector.
  using OutputType = velox::Varbinary;

  static constexpr bool default_null_behavior_ = false;

  static bool toIntermediate(
      velox::exec::out_type<IntermediateType>& out,
      velox::exec::optional_arg_type<T> in) {
    if (in.has_value()) {
      auto updateSketch = datasketches::update_theta_sketch::builder().build();
      if constexpr (std::is_same_v<T, velox::int128_t>) {
        updateSketch.update(std::to_string(in.value()));
      } else if constexpr (
          std::is_same_v<T, std::string> ||
          std::is_same_v<T, velox::Varbinary>) {
        const auto& strView = in.value();
        updateSketch.update(std::string(strView.data(), strView.size()));
      } else {
        updateSketch.update(in.value());
      }
      datasketches::theta_union thetaUnion =
          datasketches::theta_union::builder().build();
      thetaUnion.update(updateSketch);
      auto compactSketch = thetaUnion.get_result();
      out.resize(compactSketch.get_serialized_size_bytes());
      auto serializedBytes = compactSketch.serialize();
      std::memcpy(out.data(), serializedBytes.data(), out.size());
    }
    return true;
  }

  struct AccumulatorType {
    datasketches::theta_union thetaUnion =
        datasketches::theta_union::builder().build();
    datasketches::update_theta_sketch updateSketch =
        datasketches::update_theta_sketch::builder().build();

    AccumulatorType() = delete;

    // Constructor used in initializeNewGroups().
    explicit AccumulatorType(
        velox::HashStringAllocator* /*allocator*/,
        ThetaSketchAggregate* /*fn*/) {}

    void updateUnion() {
      thetaUnion.update(updateSketch);
      updateSketch.reset();
    }

    // addInput expects one parameter of exec::arg_type<T> for each child-type T
    // wrapped in InputType.
    bool addInput(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<T> data) {
      if (data.has_value()) {
        if constexpr (std::is_same_v<T, velox::int128_t>) {
          updateSketch.update(std::to_string(data.value()));
        } else if constexpr (
            std::is_same_v<T, std::string> ||
            std::is_same_v<T, velox::Varbinary>) {
          const auto& strView = data.value();
          updateSketch.update(std::string(strView.data(), strView.size()));
        } else {
          updateSketch.update(data.value());
        }
      }
      return true;
    }

    // combine expects one parameter of exec::arg_type<IntermediateType>.
    bool combine(
        velox::HashStringAllocator* /*allocator*/,
        velox::exec::optional_arg_type<velox::Varbinary> other) {
      if (other.has_value()) {
        updateUnion();
        auto compactSketch = datasketches::wrapped_compact_theta_sketch::wrap(
            other->data(), other->size());
        thetaUnion.update(compactSketch);
      }
      return true;
    }

    bool getResult(velox::exec::out_type<velox::Varbinary>& out) {
      updateUnion();
      auto compactSketch = thetaUnion.get_result();
      out.resize(compactSketch.get_serialized_size_bytes());
      auto serializedBytes = compactSketch.serialize();
      std::memcpy(out.data(), serializedBytes.data(), out.size());
      return true;
    }

    bool writeFinalResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      return getResult(out);
    }

    bool writeIntermediateResult(
        bool nonNullGroup,
        velox::exec::out_type<velox::Varbinary>& out) {
      return getResult(out);
    }
  };
};

} // namespace

velox::exec::AggregateRegistrationResult registerThetaSketchAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<velox::exec::AggregateFunctionSignature>>
      signatures;
  std::string returnType = "varbinary";
  std::string intermediateType = "varbinary";

  for (const auto& inputType :
       {"tinyint",
        "smallint",
        "integer",
        "bigint",
        "hugeint",
        "real",
        "double",
        "varchar",
        "date",
        "timestamp"}) {
    signatures.push_back(
        velox::exec::AggregateFunctionSignatureBuilder()
            .returnType(returnType)
            .intermediateType(intermediateType)
            .argumentType(inputType)
            .build());
  }
  signatures.push_back(
      velox::exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .returnType(returnType)
          .intermediateType(intermediateType)
          .argumentType("DECIMAL(a_precision, a_scale)")
          .build());

  auto name = prefix + kThetaSketch;

  return velox::exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          velox::core::AggregationNode::Step step,
          const std::vector<velox::TypePtr>& argTypes,
          const velox::TypePtr& resultType,
          const velox::core::QueryConfig& /*config*/)
          -> std::unique_ptr<velox::exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        auto inputType = argTypes[0];
        if (velox::exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case velox::TypeKind::TINYINT:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<int8_t>>>(step, argTypes, resultType);
            case velox::TypeKind::SMALLINT:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<int16_t>>>(step, argTypes, resultType);
            case velox::TypeKind::INTEGER:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<int32_t>>>(step, argTypes, resultType);
            case velox::TypeKind::BIGINT:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<int64_t>>>(step, argTypes, resultType);
            case velox::TypeKind::HUGEINT:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<velox::int128_t>>>(
                  step, argTypes, resultType);
            case velox::TypeKind::REAL:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<float>>>(step, argTypes, resultType);
            case velox::TypeKind::DOUBLE:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<double>>>(step, argTypes, resultType);
            case velox::TypeKind::VARCHAR:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<std::string>>>(
                  step, argTypes, resultType);
            case velox::TypeKind::TIMESTAMP:
              return std::make_unique<velox::exec::SimpleAggregateAdapter<
                  ThetaSketchAggregate<typename velox::TypeTraits<
                      velox::TypeKind::TIMESTAMP>::NativeType>>>(
                  step, argTypes, resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation {}",
                  name,
                  inputType->kindName());
          }
        } else {
          return std::make_unique<velox::exec::SimpleAggregateAdapter<
              ThetaSketchAggregate<velox::Varbinary>>>(
              step, argTypes, resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::presto::functions::aggregate
