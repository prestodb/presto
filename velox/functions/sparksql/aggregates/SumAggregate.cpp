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
#include "velox/functions/sparksql/aggregates/SumAggregate.h"

#include "velox/functions/lib/aggregates/SumAggregateBase.h"
#include "velox/functions/sparksql/aggregates/DecimalSumAggregate.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::functions::aggregate::sparksql {

namespace {
template <typename TInput, typename TAccumulator, typename ResultType>
using SumAggregate = SumAggregateBase<TInput, TAccumulator, ResultType, true>;

TypePtr getDecimalSumType(const TypePtr& resultType) {
  if (resultType->isRow()) {
    // If the resultType is ROW, then the type if sum is the type of the first
    // child of the ROW.
    return resultType->childAt(0);
  }
  return resultType;
}

void checkAccumulatorRowType(const TypePtr& type) {
  VELOX_CHECK_EQ(type->kind(), TypeKind::ROW);
  VELOX_CHECK(
      type->childAt(0)->isShortDecimal() || type->childAt(0)->isLongDecimal());
  VELOX_CHECK_EQ(type->childAt(1)->kind(), TypeKind::BOOLEAN);
}

std::unique_ptr<exec::Aggregate> constructDecimalSumAgg(
    const TypePtr& inputType,
    const TypePtr& sumType,
    const TypePtr& resultType) {
  uint8_t precision = getDecimalPrecisionScale(*sumType).first;
  switch (precision) {
    // The sum precision is calculated from the input precision with the formula
    // min(p + 10, 38). Therefore, the sum precision must >= 11.
#define PRECISION_CASE(precision)                                           \
  case precision:                                                           \
    if (inputType->isShortDecimal() && sumType->isShortDecimal()) {         \
      return std::make_unique<exec::SimpleAggregateAdapter<                 \
          DecimalSumAggregate<int64_t, int64_t, precision>>>(resultType);   \
    } else if (inputType->isShortDecimal() && sumType->isLongDecimal()) {   \
      return std::make_unique<exec::SimpleAggregateAdapter<                 \
          DecimalSumAggregate<int64_t, int128_t, precision>>>(resultType);  \
    } else {                                                                \
      return std::make_unique<exec::SimpleAggregateAdapter<                 \
          DecimalSumAggregate<int128_t, int128_t, precision>>>(resultType); \
    }
    PRECISION_CASE(11)
    PRECISION_CASE(12)
    PRECISION_CASE(13)
    PRECISION_CASE(14)
    PRECISION_CASE(15)
    PRECISION_CASE(16)
    PRECISION_CASE(17)
    PRECISION_CASE(18)
    PRECISION_CASE(19)
    PRECISION_CASE(20)
    PRECISION_CASE(21)
    PRECISION_CASE(22)
    PRECISION_CASE(23)
    PRECISION_CASE(24)
    PRECISION_CASE(25)
    PRECISION_CASE(26)
    PRECISION_CASE(27)
    PRECISION_CASE(28)
    PRECISION_CASE(29)
    PRECISION_CASE(30)
    PRECISION_CASE(31)
    PRECISION_CASE(32)
    PRECISION_CASE(33)
    PRECISION_CASE(34)
    PRECISION_CASE(35)
    PRECISION_CASE(36)
    PRECISION_CASE(37)
    PRECISION_CASE(38)
#undef PRECISION_CASE
    default:
      VELOX_UNREACHABLE();
  }
}
} // namespace

exec::AggregateRegistrationResult registerSum(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("double")
          .argumentType("real")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("double")
          .argumentType("double")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .integerVariable("a_precision")
          .integerVariable("a_scale")
          .integerVariable("r_precision", "min(38, a_precision + 10)")
          .integerVariable("r_scale", "min(38, a_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .intermediateType("ROW(DECIMAL(r_precision, r_scale), boolean)")
          .returnType("DECIMAL(r_precision, r_scale)")
          .build(),
  };

  for (const auto& inputType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("bigint")
                             .intermediateType("bigint")
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
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<SumAggregate<int8_t, int64_t, int64_t>>(
                BIGINT());
          case TypeKind::SMALLINT:
            return std::make_unique<SumAggregate<int16_t, int64_t, int64_t>>(
                BIGINT());
          case TypeKind::INTEGER:
            return std::make_unique<SumAggregate<int32_t, int64_t, int64_t>>(
                BIGINT());
          case TypeKind::BIGINT: {
            if (inputType->isShortDecimal()) {
              return constructDecimalSumAgg(
                  inputType, getDecimalSumType(resultType), resultType);
            }
            return std::make_unique<SumAggregate<int64_t, int64_t, int64_t>>(
                BIGINT());
          }
          case TypeKind::HUGEINT: {
            VELOX_CHECK(inputType->isLongDecimal());
            // If inputType is long decimal,
            // its output type is always long decimal.
            return constructDecimalSumAgg(
                inputType, getDecimalSumType(resultType), resultType);
          }
          case TypeKind::REAL:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<SumAggregate<float, double, float>>(
                  resultType);
            }
            return std::make_unique<SumAggregate<float, double, double>>(
                DOUBLE());
          case TypeKind::DOUBLE:
            if (resultType->kind() == TypeKind::REAL) {
              return std::make_unique<SumAggregate<double, double, float>>(
                  resultType);
            }
            return std::make_unique<SumAggregate<double, double, double>>(
                DOUBLE());
          case TypeKind::ROW: {
            VELOX_DCHECK(!exec::isRawInput(step));
            checkAccumulatorRowType(inputType);
            // For the intermediate aggregation step, input intermediate sum
            // type is equal to final result sum type.
            return constructDecimalSumAgg(
                inputType->childAt(0), inputType->childAt(0), resultType);
          }
            [[fallthrough]];
          default:
            VELOX_UNREACHABLE(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::functions::aggregate::sparksql
