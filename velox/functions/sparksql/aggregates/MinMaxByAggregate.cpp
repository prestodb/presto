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

#include "velox/functions/lib/aggregates/MinMaxByAggregatesBase.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::functions::aggregate::sparksql {

namespace {

/// Returns compare result align with Spark's specific behavior,
/// which returns true if the value in 'index' row of 'newComparisons' is
/// greater than/equal or less than/equal the value in the 'accumulator'.
template <bool sparkGreaterThan, typename T, typename TAccumulator>
struct SparkComparator {
  static bool compare(
      TAccumulator* accumulator,
      const DecodedVector& newComparisons,
      vector_size_t index,
      bool isFirstValue) {
    if constexpr (isNumeric<T>()) {
      if (isFirstValue) {
        return true;
      }
      if constexpr (sparkGreaterThan) {
        return newComparisons.valueAt<T>(index) >= *accumulator;
      } else {
        return newComparisons.valueAt<T>(index) <= *accumulator;
      }
    } else {
      if constexpr (sparkGreaterThan) {
        return !accumulator->hasValue() ||
            compare(accumulator, newComparisons, index) <= 0;
      } else {
        return !accumulator->hasValue() ||
            compare(accumulator, newComparisons, index) >= 0;
      }
    }
  }

  FOLLY_ALWAYS_INLINE static int32_t compare(
      const SingleValueAccumulator* accumulator,
      const DecodedVector& decoded,
      vector_size_t index) {
    static const CompareFlags kCompareFlags{
        true, // nullsFirst
        true, // ascending
        false, // equalsOnly
        CompareFlags::NullHandlingMode::kNullAsValue};
    auto result = accumulator->compare(decoded, index, kCompareFlags);
    return result.value();
  }
};

std::string toString(const std::vector<TypePtr>& types) {
  std::ostringstream out;
  for (auto i = 0; i < types.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << types[i]->toString();
  }
  return out.str();
}

template <
    template <
        typename U,
        typename V,
        bool B1,
        template <bool B2, typename C1, typename C2>
        class C>
    class Aggregate,
    bool isMaxFunc>
exec::AggregateRegistrationResult registerMinMaxBy(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  // V, C -> row(V, C) -> V.
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("V")
                           .typeVariable("C")
                           .returnType("V")
                           .intermediateType("row(V,C)")
                           .argumentType("V")
                           .argumentType("C")
                           .build());
  const std::vector<std::string> supportedCompareTypes = {
      "boolean",
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "real",
      "double",
      "varchar",
      "date",
      "timestamp"};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        const auto isRawInput = exec::isRawInput(step);
        const std::string errorMessage = fmt::format(
            "Unknown input types for {} ({}) aggregation: {}",
            name,
            mapAggregationStepToName(step),
            toString(argTypes));

        if (isRawInput) {
          // Input is: V, C.
          return create<Aggregate, SparkComparator, isMaxFunc>(
              resultType, argTypes[0], argTypes[1], errorMessage);
        } else {
          // Input is: ROW(V, C).
          const auto& rowType = argTypes[0];
          const auto& valueType = rowType->childAt(0);
          const auto& compareType = rowType->childAt(1);
          return create<Aggregate, SparkComparator, isMaxFunc>(
              resultType, valueType, compareType, errorMessage);
        }
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace

void registerMinMaxByAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerMinMaxBy<MinMaxByAggregateBase, true>(
      prefix + "max_by", withCompanionFunctions, overwrite);
  registerMinMaxBy<MinMaxByAggregateBase, false>(
      prefix + "min_by", withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::functions::aggregate::sparksql
