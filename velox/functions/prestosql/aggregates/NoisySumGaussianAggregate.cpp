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
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/noisy_aggregation/NoisyCountSumAvgAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/NoisyHelperFunctionFactory.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {
class NoisySumGaussianAggregate : public exec::Aggregate {
 public:
  explicit NoisySumGaussianAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType = functions::aggregate::NoisyCountSumAvgAccumulator;

  int32_t accumulatorFixedWidthSize() const override {
    return static_cast<int32_t>(sizeof(AccumulatorType));
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    NoisyHelperFunctionFactory::decodeInputData(
        decodedValue_,
        decodedNoiseScale_,
        decodedLowerBound_,
        decodedUpperBound_,
        decodedRandomSeed_,
        rows,
        args);
    bool hasRandomSeed = NoisyHelperFunctionFactory::checkRandomSeed(args);
    bool hasBounds = NoisyHelperFunctionFactory::checkBounds(args);

    rows.applyToSelected([&](vector_size_t i) {
      auto* accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
      NoisyHelperFunctionFactory::updateAccumulatorFromInput(
          decodedValue_,
          decodedNoiseScale_,
          decodedLowerBound_,
          decodedUpperBound_,
          decodedRandomSeed_,
          args,
          *accumulator,
          i,
          hasBounds,
          hasRandomSeed);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    NoisyHelperFunctionFactory::decodeInputData(
        decodedValue_,
        decodedNoiseScale_,
        decodedLowerBound_,
        decodedUpperBound_,
        decodedRandomSeed_,
        rows,
        args);
    bool hasRandomSeed = NoisyHelperFunctionFactory::checkRandomSeed(args);
    bool hasBounds = NoisyHelperFunctionFactory::checkBounds(args);
    auto* accumulator = exec::Aggregate::value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      NoisyHelperFunctionFactory::updateAccumulatorFromInput(
          decodedValue_,
          decodedNoiseScale_,
          decodedLowerBound_,
          decodedUpperBound_,
          decodedRandomSeed_,
          args,
          *accumulator,
          i,
          hasBounds,
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

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* vector = (*result)->as<FlatVector<double>>();
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
      auto accumulator = value<AccumulatorType>(group);
      if (isNull(group)) {
        vector->setNull(i, true);
        continue;
      }
      // For groups that have allNull Values
      if (accumulator->getNoiseScale() < 0) {
        vector->setNull(i, true);
        continue;
      }
      double noise = gen.nextNoise();

      // Check the sign of noisy sum is consistent with the bounds.
      double nosiySum = accumulator->getSum() + noise;
      auto finalResult = NoisyHelperFunctionFactory::postProcessNoisyValue(
          nosiySum, *accumulator);
      vector->set(i, finalResult);
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      auto* accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
      NoisyHelperFunctionFactory::updateAccumulatorFromIntermediateResult(
          *accumulator, decoded, i);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    DecodedVector decoded(*args[0], rows);

    auto* accumulator = exec::Aggregate::value<AccumulatorType>(group);
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
  DecodedVector decodedLowerBound_;
  DecodedVector decodedUpperBound_;
  DecodedVector decodedRandomSeed_;
};
} // namespace

void registerNoisySumGaussianAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  // Helper function to create a signature builder with return and
  // intermediate types
  auto createBuilder = []() {
    return exec::AggregateFunctionSignatureBuilder()
        .returnType("double") // noisy_sum_guassian always returns double
        .intermediateType("varbinary");
  };

  // List of possible argument types.
  const std::vector<std::string> simpleDataTypes = {
      "tinyint", "smallint", "integer", "bigint", "real", "double"};
  const std::vector<std::string> noiseScaleTypes = {"double", "bigint"};
  const std::string randomSeedType = "bigint";
  const std::vector<std::string> boundTypes = {"double", "bigint"};

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  // Generate signatures for all type combinations.
  for (const auto& noiseScaleType : noiseScaleTypes) {
    // Handle simple types.
    for (const auto& dataType : simpleDataTypes) {
      // Signature 1: (col, noise_scale)
      signatures.push_back(createBuilder()
                               .argumentType(dataType)
                               .argumentType(noiseScaleType)
                               .build());
      // Signature 2: (col, noise_scale, random_seed)
      signatures.push_back(createBuilder()
                               .argumentType(dataType)
                               .argumentType(noiseScaleType)
                               .argumentType(randomSeedType)
                               .build());

      for (const auto& lowerBoundType : boundTypes) {
        for (const auto& upperBoundType : boundTypes) {
          // Signature 3: (col, noise_scale, lower_bound, upper_bound)
          signatures.push_back(createBuilder()
                                   .argumentType(dataType)
                                   .argumentType(noiseScaleType)
                                   .argumentType(lowerBoundType)
                                   .argumentType(upperBoundType)
                                   .build());
          // Signature 4: (col, noise_scale, lower_bound, upper_bound,
          // random_seed)
          signatures.push_back(createBuilder()
                                   .argumentType(dataType)
                                   .argumentType(noiseScaleType)
                                   .argumentType(lowerBoundType)
                                   .argumentType(upperBoundType)
                                   .argumentType(randomSeedType)
                                   .build());
        }
      }
    }
    // Handle decimal types separately.
    // Signature 1: (col, noise_scale)
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("double")
                             .intermediateType("varbinary")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType(noiseScaleType)
                             .build());
    // Signature 2: (col, noise_scale, random_seed)
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("double")
                             .intermediateType("varbinary")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType(noiseScaleType)
                             .argumentType(randomSeedType)
                             .build());

    for (const auto& lowerBoundType : boundTypes) {
      for (const auto& upperBoundType : boundTypes) {
        // Signature 3: (col, noise_scale, lower_bound, upper_bound)
        signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                                 .integerVariable("a_precision")
                                 .integerVariable("a_scale")
                                 .returnType("double")
                                 .intermediateType("varbinary")
                                 .argumentType("DECIMAL(a_precision, a_scale)")
                                 .argumentType(noiseScaleType)
                                 .argumentType(lowerBoundType)
                                 .argumentType(upperBoundType)
                                 .build());
        // Signature 4: (col, noise_scale, lower_bound, upper_bound,
        // random_seed)
        signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                                 .integerVariable("a_precision")
                                 .integerVariable("a_scale")
                                 .returnType("double")
                                 .intermediateType("varbinary")
                                 .argumentType("DECIMAL(a_precision, a_scale)")
                                 .argumentType(noiseScaleType)
                                 .argumentType(lowerBoundType)
                                 .argumentType(upperBoundType)
                                 .argumentType(randomSeedType)
                                 .build());
      }
    }
  }

  auto name = prefix + kNoisySumGaussian;
  exec::registerAggregateFunction(
      name,
      signatures,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          [[maybe_unused]] const TypePtr& resultType,
          [[maybe_unused]] const core::QueryConfig&)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_GE(
            argTypes.size(), 2, "{} takes at least 2 arguments", name);
        VELOX_CHECK_LE(
            argTypes.size(), 5, "{} takes at most 5 arguments", name);

        if (exec::isPartialOutput(step)) {
          return std::make_unique<NoisySumGaussianAggregate>(VARBINARY());
        }
        return std::make_unique<NoisySumGaussianAggregate>(DOUBLE());
      },
      {false /*orderSensitive*/, false /*companionFunction*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
