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

#pragma once

// #include "velox/exec/Aggregate.h"
#include "velox/functions/lib/aggregates/noisy_aggregation/NoisyCountSumAvgAccumulator.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::aggregate::prestosql {

class NoisyHelperFunctionFactory {
  using AccumulatorType = functions::aggregate::NoisyCountSumAvgAccumulator;

 public:
  class NoiseGenerator {
   public:
    NoiseGenerator(double noiseScale, std::optional<int64_t> randomSeed)
        : noiseScale_(noiseScale), randomSeed_(randomSeed) {
      if (randomSeed_.has_value()) {
        rng_.seed(randomSeed_.value());
      } else {
        rng_.seed(folly::Random::secureRand64());
      }
      // Only create normal distribution if noiseScale > 0
      // When noiseScale is 0, we want no noise (deterministic behavior)
      if (noiseScale_ > 0) {
        dist_ = std::normal_distribution<double>(0, noiseScale_);
      }
    }

    double nextNoise() {
      // If noise scale is 0, return 0 (no noise)
      if (noiseScale_ <= 0) {
        return 0.0;
      }
      return dist_(rng_);
    }

   private:
    double noiseScale_;
    std::optional<int64_t> randomSeed_;
    folly::Random::DefaultGenerator rng_;
    std::normal_distribution<double> dist_;
  };

  static void decodeInputData(
      DecodedVector& decodedValue_,
      DecodedVector& decodedNoiseScale_,
      DecodedVector& decodedLowerBound_,
      DecodedVector& decodedUpperBound_,
      DecodedVector& decodedRandomSeed_,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args);

  // Overloaded version for functions that don't use bounds (noisy_count and
  // noisy_count_if)
  static void decodeInputData(
      DecodedVector& decodedValue_,
      DecodedVector& decodedNoiseScale_,
      DecodedVector& decodedRandomSeed_,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args);

  static void updateAccumulatorFromInput(
      DecodedVector& decodedValue_,
      DecodedVector& decodedNoiseScale_,
      DecodedVector& decodedLowerBound_,
      DecodedVector& decodedUpperBound_,
      DecodedVector& decodedRandomSeed_,
      const std::vector<VectorPtr>& args,
      AccumulatorType& accumulator,
      vector_size_t i,
      bool hasBounds,
      bool hasRandomSeed);

  // Overloaded version for functions that don't use bounds (noisy_count and
  // noisy_count_if)
  static void updateAccumulatorFromInput(
      DecodedVector& decodedValue_,
      DecodedVector& decodedNoiseScale_,
      DecodedVector& decodedRandomSeed_,
      const std::vector<VectorPtr>& args,
      AccumulatorType& accumulator,
      vector_size_t i,
      bool hasRandomSeed,
      bool isCountIf = false);

  static void updateAccumulatorFromIntermediateResult(
      AccumulatorType& accumulator,
      DecodedVector& decodedVector,
      vector_size_t i);

  static double postProcessNoisyValue(
      double noisyValue,
      const AccumulatorType& accumulator);

  static void extractAccumulators(
      const std::function<bool(char*)>& isNull,
      const std::function<AccumulatorType(char*)>& getAccumulator,
      char** groups,
      int32_t numGroups,
      VectorPtr* result);

  template <TypeKind TData>
  static void updateTemplate(
      AccumulatorType& accumulator,
      const DecodedVector& decodedValue,
      vector_size_t i) {
    using T = typename TypeTraits<TData>::NativeType;
    // Handle decimal types separately.
    if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>) {
      const auto& type = decodedValue.base()->type();
      if (type->isDecimal()) {
        auto value = decodedValue.valueAt<T>(i);
        auto scale = type->isShortDecimal() ? type->asShortDecimal().scale()
                                            : type->asLongDecimal().scale();
        double doubleValue = static_cast<double>(value) / pow(10, scale);

        accumulator.clipUpdateSum(doubleValue);
        accumulator.updateCount(1);
        return;
      }
    }
    // Handle other types.
    if constexpr (
        std::is_same_v<T, TypeTraits<TypeKind::TIMESTAMP>> ||
        std::is_same_v<T, TypeTraits<TypeKind::VARBINARY>> ||
        std::is_same_v<T, TypeTraits<TypeKind::VARCHAR>> ||
        std::is_same_v<T, facebook::velox::StringView> ||
        std::is_same_v<T, facebook::velox::Timestamp>) {
      VELOX_FAIL("Noisy function does not support this data type.");
    } else {
      // Handle not a number.
      if (std::isnan(decodedValue.valueAt<T>(i))) {
        return;
      }
      accumulator.clipUpdateSum(
          static_cast<double>(decodedValue.valueAt<T>(i)));
      accumulator.updateCount(1);
    }
  }

  static bool checkBounds(const std::vector<VectorPtr>& args) {
    // If size of args is GREATER THAN 3, it means lower and upper bounds are
    // provided.
    return args.size() > 3;
  }

  static bool checkRandomSeed(const std::vector<VectorPtr>& args) {
    // If size of args is 3 or 5, it means random seed is provided.
    return args.size() == 3 || args.size() == 5;
  }

  static const std::pair<double, std::optional<int64_t>>
  getFinalNoiseScaleAndRandomSeed(
      const std::function<bool(char*)>& isNull,
      const std::function<AccumulatorType(char*)>& getAccumulator,
      char** groups,
      int32_t numGroups);

 private:
  // Private helper functions
  static void updateNoiseScale(
      DecodedVector& decodedNoiseScale_,
      const std::vector<VectorPtr>& args,
      AccumulatorType& accumulator,
      vector_size_t i);

  static void updateBounds(
      DecodedVector& decodedLowerBound_,
      DecodedVector& decodedUpperBound_,
      const std::vector<VectorPtr>& args,
      AccumulatorType& accumulator,
      vector_size_t i);

  static void updateRandomSeed(
      DecodedVector& decodedRandomSeed_,
      AccumulatorType& accumulator,
      vector_size_t i);
};

} // namespace facebook::velox::aggregate::prestosql
