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

#include "velox/functions/sparksql/aggregates/RegrReplacementAggregate.h"

#include "velox/exec/SimpleAggregateAdapter.h"

namespace facebook::velox::functions::aggregate::sparksql {
namespace {
class RegrReplacementAggregate {
 public:
  using InputType = Row<double>;
  using IntermediateType =
      Row</*n*/ double,
          /*avg*/ double,
          /*m2*/ double>;
  using OutputType = double;

  static bool toIntermediate(
      exec::out_type<Row<double, double, double>>& out,
      exec::arg_type<double> in) {
    out.copy_from(std::make_tuple(1.0, in, 0.0));
    return true;
  }

  struct AccumulatorType {
    double n{0.0};
    double avg{0.0};
    double m2{0.0};

    explicit AccumulatorType(HashStringAllocator* /*allocator*/) {}

    void addInput(
        HashStringAllocator* /*allocator*/,
        exec::arg_type<double> data) {
      n += 1.0;
      double delta = data - avg;
      double deltaN = delta / n;
      avg += deltaN;
      m2 += delta * (delta - deltaN);
    }

    void combine(
        HashStringAllocator* /*allocator*/,
        exec::arg_type<Row<double, double, double>> other) {
      VELOX_CHECK(other.at<0>().has_value());
      VELOX_CHECK(other.at<1>().has_value());
      VELOX_CHECK(other.at<2>().has_value());

      double otherN = other.at<0>().value();
      double otherAvg = other.at<1>().value();
      double otherM2 = other.at<2>().value();

      double originN = n;
      n += otherN;
      double delta = otherAvg - avg;
      double deltaN = n == 0.0 ? 0.0 : delta / n;
      avg += deltaN * otherN;
      m2 += otherM2 + delta * deltaN * originN * otherN;
    }

    bool writeIntermediateResult(exec::out_type<IntermediateType>& out) {
      out = std::make_tuple(n, avg, m2);
      return true;
    }

    bool writeFinalResult(exec::out_type<OutputType>& out) {
      if (n == 0.0) {
        return false;
      }
      out = m2;
      return true;
    }
  };
};

exec::AggregateRegistrationResult registerRegrReplacement(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("row(double, double, double)")
          .argumentType("double")
          .build()};
  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step /*step*/,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<
            exec::SimpleAggregateAdapter<RegrReplacementAggregate>>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace

void registerRegrReplacementAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerRegrReplacement(
      prefix + "regr_replacement", withCompanionFunctions, overwrite);
}
} // namespace facebook::velox::functions::aggregate::sparksql
