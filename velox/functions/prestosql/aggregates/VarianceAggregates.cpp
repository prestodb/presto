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
#include "velox/functions/prestosql/aggregates/VarianceAggregates.h"
#include "velox/exec/Aggregate.h"
#include "velox/functions/lib/aggregates/VarianceAggregatesBase.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {

// 'Population standard deviation' result accessor for the Variance Accumulator.
struct StdDevPopResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() > 0;
  }

  static double result(const VarianceAccumulator& accData) {
    return std::sqrt(accData.m2() / accData.count());
  }
};

// 'Sample standard deviation' result accessor for the Variance Accumulator.
struct StdDevSampResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() >= 2;
  }

  static double result(const VarianceAccumulator& accData) {
    return std::sqrt(accData.m2() / (accData.count() - 1));
  }
};

// 'Population variance' result accessor for the Variance Accumulator.
struct VarPopResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() > 0;
  }

  static double result(const VarianceAccumulator& accData) {
    return accData.m2() / accData.count();
  }
};

// 'Sample variance' result accessor for the Variance Accumulator.
struct VarSampResultAccessor {
  static bool hasResult(const VarianceAccumulator& accData) {
    return accData.count() >= 2;
  }

  static double result(const VarianceAccumulator& accData) {
    return accData.m2() / (accData.count() - 1);
  }
};

// Implements 'Population Standard Deviation' aggregate.
// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class StdDevPopAggregate
    : public VarianceAggregate<T, StdDevPopResultAccessor> {
 public:
  explicit StdDevPopAggregate(TypePtr resultType)
      : VarianceAggregate<T, StdDevPopResultAccessor>(resultType) {}
};

// Implements 'Sample Standard Deviation' aggregate.
// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class StdDevSampAggregate
    : public VarianceAggregate<T, StdDevSampResultAccessor> {
 public:
  explicit StdDevSampAggregate(TypePtr resultType)
      : VarianceAggregate<T, StdDevSampResultAccessor>(resultType) {}
};

// Implements 'Population Variance' aggregate.
// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class VarPopAggregate : public VarianceAggregate<T, VarPopResultAccessor> {
 public:
  explicit VarPopAggregate(TypePtr resultType)
      : VarianceAggregate<T, VarPopResultAccessor>(resultType) {}
};

// Implements 'Sample Variance' aggregate.
// T is the input type for partial aggregation. Not used for final aggregation.
template <typename T>
class VarSampAggregate : public VarianceAggregate<T, VarSampResultAccessor> {
 public:
  explicit VarSampAggregate(TypePtr resultType)
      : VarianceAggregate<T, VarSampResultAccessor>(resultType) {}
};

template <template <typename TInput> class TClass>
exec::AggregateRegistrationResult registerVariance(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  std::vector<std::string> inputTypes = {
      "smallint", "integer", "bigint", "real", "double"};
  for (const auto& inputType : inputTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("row(bigint,double,double)")
                             .argumentType(inputType)
                             .build());
  }

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
              return std::make_unique<TClass<int16_t>>(resultType);
            case TypeKind::INTEGER:
              return std::make_unique<TClass<int32_t>>(resultType);
            case TypeKind::BIGINT:
              return std::make_unique<TClass<int64_t>>(resultType);
            case TypeKind::REAL:
              return std::make_unique<TClass<float>>(resultType);
            case TypeKind::DOUBLE:
              return std::make_unique<TClass<double>>(resultType);
            default:
              VELOX_FAIL(
                  "Unknown input type for {} aggregation: {}",
                  name,
                  inputType->toString());
          }
        } else {
          checkSumCountRowType(
              inputType,
              "Input type for final aggregation must be "
              "(count:bigint, mean:double, m2:double) struct");
          return std::make_unique<TClass<int64_t>>(resultType);
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerVarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerVariance<StdDevSampAggregate>(
      prefix + kStdDev, withCompanionFunctions, overwrite);
  registerVariance<StdDevPopAggregate>(
      prefix + kStdDevPop, withCompanionFunctions, overwrite);
  registerVariance<StdDevSampAggregate>(
      prefix + kStdDevSamp, withCompanionFunctions, overwrite);
  registerVariance<VarSampAggregate>(
      prefix + kVariance, withCompanionFunctions, overwrite);
  registerVariance<VarPopAggregate>(
      prefix + kVarPop, withCompanionFunctions, overwrite);
  registerVariance<VarSampAggregate>(
      prefix + kVarSamp, withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
