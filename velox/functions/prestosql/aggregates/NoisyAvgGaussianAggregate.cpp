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

#include "velox/functions/prestosql/aggregates/NoisyAvgGaussianAggregate.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/noisy_aggregation/NoisyCountSumAvgAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/NoisyHelperFunctionFactory.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {
class NoisyAvgGaussianAggregate : public exec::Aggregate {
 public:
  explicit NoisyAvgGaussianAggregate(TypePtr resultType)
      : exec::Aggregate(std::move(resultType)) {}

  using AccumulatorType = functions::aggregate::NoisyCountSumAvgAccumulator;

  bool isFixedSize() const override {
    return true;
  }

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
    bool hasBounds = NoisyHelperFunctionFactory::checkBounds(args);
    bool hasRandomSeed = NoisyHelperFunctionFactory::checkRandomSeed(args);

    // Process the args data and update the accumulator for each group.
    rows.applyToSelected([&](vector_size_t i) {
      auto accumulator = value<AccumulatorType>(groups[i]);
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
    bool hasBounds = NoisyHelperFunctionFactory::checkBounds(args);
    bool hasRandomSeed = NoisyHelperFunctionFactory::checkRandomSeed(args);

    auto accumulator = exec::Aggregate::value<AccumulatorType>(group);

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

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    DecodedVector decodedVector(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      auto* accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
      NoisyHelperFunctionFactory::updateAccumulatorFromIntermediateResult(
          *accumulator, decodedVector, i);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    DecodedVector decodedVector(*args[0], rows);
    auto* accumulator = exec::Aggregate::value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      NoisyHelperFunctionFactory::updateAccumulatorFromIntermediateResult(
          *accumulator, decodedVector, i);
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

      uint64_t trueCount = accumulator->getCount();
      double trueSum = accumulator->getSum();
      VELOX_CHECK_LE(trueCount, std::numeric_limits<double>::max());
      double trueAvg = trueSum / static_cast<double>(trueCount);
      double noise = gen.nextNoise();

      double noisyAvg = trueAvg + noise;
      double finalResult = NoisyHelperFunctionFactory::postProcessNoisyValue(
          noisyAvg, *accumulator);
      vector->set(i, finalResult);
    }
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

void registerNoisyAvgGaussianAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  // Helper function to create a signature builder with return and
  // intermediate types
  auto createBuilder = []() {
    return exec::AggregateFunctionSignatureBuilder()
        .returnType("double")
        .intermediateType("varbinary");
  };

  // List of possible argument types.
  const std::vector<std::string> simpleDataTypes = {
      "tinyint", "smallint", "integer", "bigint", "real", "double"};
  const std::vector<std::string> noiseScaleTypes = {"double", "bigint"};
  const std::vector<std::string> boundTypes = {"double", "bigint"};
  const std::string randomSeedType = "bigint";

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

  auto name = prefix + kNoisyAvgGaussian;
  exec::registerAggregateFunction(
      name,
      signatures,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& /*resultType*/,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_GE(
            argTypes.size(), 2, "{} takes at least 2 arguments", name);

        if (exec::isPartialOutput(step)) {
          return std::make_unique<NoisyAvgGaussianAggregate>(VARBINARY());
        }
        return std::make_unique<NoisyAvgGaussianAggregate>(DOUBLE());
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
