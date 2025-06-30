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

#include "velox/functions/prestosql/aggregates/NoisyCountIfGaussianAggregate.h"
#include <cmath>
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/noisy_aggregation/NoisyCountSumAvgAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/NoisyHelperFunctionFactory.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {

class NoisyCountIfGaussianAggregate : public exec::Aggregate {
 public:
  explicit NoisyCountIfGaussianAggregate(TypePtr resultType)
      : exec::Aggregate(std::move(resultType)) {}

  using AccumulatorType = functions::aggregate::NoisyCountSumAvgAccumulator;

  bool isFixedSize() const override {
    return true;
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    // ImmediateResults are serialized accumulators.
    // So we convert the accumulators (in group) to strings where each is
    // serialized version of the accumulator and store the strings in the result
    // vector
    std::function<bool(char*)> isNull = [this](char* group) {
      return this->isNull(group);
    };

    std::function<AccumulatorType(char*)> getAccumulator = [this](char* group) {
      return *this->value<AccumulatorType>(group);
    };

    NoisyHelperFunctionFactory::extractAccumulators(
        isNull, getAccumulator, groups, numGroups, result);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* vector = (*result)->as<FlatVector<int64_t>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    // Find noise scale. Because noise scale is the same for all groups, we can
    // find it once and reuse it for all groups
    std::function<bool(char*)> isNull = [this](char* group) {
      return this->isNull(group);
    };
    std::function<AccumulatorType(char*)> getAccumulator = [this](char* group) {
      return *this->value<AccumulatorType>(group);
    };

    auto [noiseScale, randomSeed] =
        NoisyHelperFunctionFactory::getFinalNoiseScaleAndRandomSeed(
            isNull, getAccumulator, groups, numGroups);

    if (noiseScale < 0) {
      // Because we checked noiseScale >= 0 whenever we update noise scale,
      // this would only happen if noiseScale was never updated,
      // meaning empty groups, meaning return NULL for all groups
      for (auto i = 0; i < numGroups; ++i) {
        vector->setNull(i, true);
      }
      return;
    }

    // If noise_scale is 0, gen will always return 0.
    NoisyHelperFunctionFactory::NoiseGenerator gen{noiseScale, randomSeed};

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
        continue;
      }
      auto* accumulator = value<AccumulatorType>(group);
      double rawNoise = gen.nextNoise();

      VELOX_USER_CHECK_GE(
          rawNoise,
          static_cast<double>(std::numeric_limits<int64_t>::min()),
          "Noise is too large. Please reduce noise scale.");
      VELOX_USER_CHECK_LE(
          rawNoise,
          static_cast<double>(std::numeric_limits<int64_t>::max()),
          "Noise is too large. Please reduce noise scale.");
      auto noise = static_cast<int64_t>(
          std::round(rawNoise)); // Need to round back to int64_t because
                                 // we want to return int64_t

      uint64_t trueCount = accumulator->getCount();
      // Check that true count won't overflow converting from unsigned int to
      // signed int. We need the conversion because noise is signed int.
      VELOX_CHECK_LT(
          trueCount,
          static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
      int64_t noisyCount = checkedPlus(static_cast<int64_t>(trueCount), noise);

      // Post-process the noisy count to make sure it is non-negative
      vector->set(i, std::max<int64_t>(noisyCount, 0L));
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    NoisyHelperFunctionFactory::decodeInputData(
        decodedValue_, decodedNoiseScale_, decodedRandomSeed_, rows, args);
    bool hasRandomSeed = NoisyHelperFunctionFactory::checkRandomSeed(args);

    // Process the args data and update the accumulator for each group.
    rows.applyToSelected([&](vector_size_t i) {
      auto accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
      NoisyHelperFunctionFactory::updateAccumulatorFromInput(
          decodedValue_,
          decodedNoiseScale_,
          decodedRandomSeed_,
          args,
          *accumulator,
          i,
          hasRandomSeed,
          true /*isCountIf*/);
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    // Add intermediate results (in serialized string) to accumulators
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      auto* accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
      NoisyHelperFunctionFactory::updateAccumulatorFromIntermediateResult(
          *accumulator, decoded, i);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    NoisyHelperFunctionFactory::decodeInputData(
        decodedValue_, decodedNoiseScale_, decodedRandomSeed_, rows, args);
    bool hasRandomSeed = NoisyHelperFunctionFactory::checkRandomSeed(args);
    auto accumulator = exec::Aggregate::value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      NoisyHelperFunctionFactory::updateAccumulatorFromInput(
          decodedValue_,
          decodedNoiseScale_,
          decodedRandomSeed_,
          args,
          *accumulator,
          i,
          hasRandomSeed,
          true /*isCountIf*/);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    // Add spilled immediate results to the accumulator for final aggregation
    DecodedVector decoded(*args[0], rows);

    auto* accumulator = value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      NoisyHelperFunctionFactory::updateAccumulatorFromIntermediateResult(
          *accumulator, decoded, i);
    });
  }

 protected:
  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    // Initialize the accumulator for each group
    for (auto i : indices) {
      *value<AccumulatorType>(groups[i]) = AccumulatorType();
    }
  }

 private:
  DecodedVector decodedValue_;
  DecodedVector decodedNoiseScale_;
  DecodedVector decodedRandomSeed_;
};

} // namespace

void registerNoisyCountIfGaussianAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("varbinary")
          .argumentType("boolean")
          .argumentType("double")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("varbinary")
          .argumentType("boolean")
          .argumentType("bigint") // support BIGINT noise scale
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("varbinary")
          .argumentType("boolean")
          .argumentType("double")
          .argumentType("bigint") // support random seed
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("bigint")
          .intermediateType("varbinary")
          .argumentType("boolean")
          .argumentType("bigint")
          .argumentType("bigint") // support random seed
          .build(),
  };

  auto name = prefix + kNoisyCountIfGaussian;
  exec::registerAggregateFunction(
      name,
      signatures,
      [name](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr& /*resultType*/,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(argTypes.size(), 3, "{} takes 2 or 3 arguments", name);
        VELOX_CHECK_GE(argTypes.size(), 2, "{} takes 2 or 3 arguments", name);

        if (exec::isPartialOutput(step)) {
          VELOX_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::BOOLEAN,
              "{} function only accepts boolean parameter",
              name);
          // While providing VARBINARY as the returnType for partial step is
          // redundant because it can be derived from rawInput types and step,
          // we still need to provide it for the framework for work because of
          // framework's legacy reasons
          return std::make_unique<NoisyCountIfGaussianAggregate>(VARBINARY());
        }

        return std::make_unique<NoisyCountIfGaussianAggregate>(BIGINT());
      },
      {false /*orderSensitive*/, false /*companionFunction*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
