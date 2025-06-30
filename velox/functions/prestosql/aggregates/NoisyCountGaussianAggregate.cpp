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

#include "velox/functions/prestosql/aggregates/NoisyCountGaussianAggregate.h"
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
class NoisyCountGaussianAggregate : public exec::Aggregate {
 public:
  explicit NoisyCountGaussianAggregate(TypePtr resultType)
      : exec::Aggregate(std::move(resultType)) {}

  using AccumulatorType = functions::aggregate::NoisyCountSumAvgAccumulator;

  int32_t accumulatorFixedWidthSize() const override {
    return static_cast<int32_t>(sizeof(AccumulatorType));
  }

  bool isFixedSize() const override {
    return true;
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
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
          hasRandomSeed);
    });
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    std::function<bool(char*)> isNull = [this](char* group) {
      return this->isNull(group);
    };

    std::function<AccumulatorType(char*)> getAccumulator = [this](char* group) {
      return *this->value<AccumulatorType>(group);
    };

    NoisyHelperFunctionFactory::extractAccumulators(
        isNull, getAccumulator, groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    auto decodedVector = DecodedVector(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      auto* accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
      NoisyHelperFunctionFactory::updateAccumulatorFromIntermediateResult(
          *accumulator, decodedVector, i);
    });
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* vector = (*result)->as<FlatVector<int64_t>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

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
      for (auto i = 0; i < numGroups; ++i) {
        vector->setNull(i, true);
      }
      return;
    }

    NoisyHelperFunctionFactory::NoiseGenerator gen{noiseScale, randomSeed};

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
        continue;
      }
      auto* accumulator = value<AccumulatorType>(group);
      // For groups that have allNull Values
      if (accumulator->getNoiseScale() < 0) {
        vector->setNull(i, true);
        continue;
      }

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

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    DecodedVector decodedVector(*args[0], rows);
    auto accumulator = exec::Aggregate::value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      NoisyHelperFunctionFactory::updateAccumulatorFromIntermediateResult(
          *accumulator, decodedVector, i);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
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
          hasRandomSeed);
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

void registerNoisyCountGaussianAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("varbinary")
          .argumentType("T")
          .argumentType("double") // support DOUBLE noise scale
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("varbinary")
          .argumentType("T")
          .argumentType("bigint") // support BIGINT noise scale
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("varbinary")
          .argumentType("T")
          .argumentType("double") // support DOUBLE noise scale
          .argumentType("bigint") // support BIGINT random seed
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .intermediateType("varbinary")
          .argumentType("T")
          .argumentType("bigint") // support BIGINT noise scale
          .argumentType("bigint") // support BIGINT random seed
          .build(),
  };

  auto name = prefix + kNoisyCountGaussian;

  exec::registerAggregateFunction(
      name,
      signatures,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          [[maybe_unused]] const TypePtr& resultType,
          [[maybe_unused]] const core::QueryConfig& config)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 3, "{} takes at most 3 arguments", name);
        VELOX_CHECK_GE(
            argTypes.size(), 2, "{} takes at least 2 arguments", name);

        if (exec::isPartialOutput(step)) {
          return std::make_unique<NoisyCountGaussianAggregate>(VARBINARY());
        }

        return std::make_unique<NoisyCountGaussianAggregate>(BIGINT());
      },
      {false /*orderSensitive*/, false /*companionFunction*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
