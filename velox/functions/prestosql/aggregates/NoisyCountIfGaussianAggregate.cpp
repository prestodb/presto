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
#include "velox/functions/lib/aggregates/noisy_aggregation/NoisyCountAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {

class NoisyCountIfGaussianAggregate : public exec::Aggregate {
 public:
  explicit NoisyCountIfGaussianAggregate(TypePtr resultType)
      : exec::Aggregate(std::move(resultType)) {}

  using AccumulatorType = NoisyCountAccumulator;

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
    auto* flatResult = (*result)->asFlatVector<StringView>();
    flatResult->resize(numGroups);

    // Determine the total size needed for the buffer
    int64_t numNonNullGroups = 0;
    for (auto i = 0; i < numGroups; ++i) {
      numNonNullGroups += isNull(groups[i]) ? 0 : 1;
    }
    size_t totalSize = numNonNullGroups * AccumulatorType::serializedSize();

    // Allocate the buffer once
    char* rawBuffer = flatResult->getRawStringBufferWithSpace(totalSize);
    size_t offset = 0;

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        flatResult->setNull(i, true);
      } else {
        auto* accumulator = value<AccumulatorType>(group);
        auto size = accumulator->serializedSize();

        // Write to the pre-allocated buffer
        accumulator->serialize(rawBuffer + offset);
        flatResult->setNoCopy(
            i, StringView(rawBuffer + offset, static_cast<int32_t>(size)));
        offset += size;
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* vector = (*result)->as<FlatVector<int64_t>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    // Find noise scale. Because noise scale is the same for all groups, we can
    // find it once and reuse it for all groups
    double noiseScale = -1;
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (!isNull(group)) {
        auto* accumulator = value<AccumulatorType>(group);
        noiseScale = accumulator->noiseScale;
        break;
      }
    }

    if (noiseScale < 0) {
      // Because we checked noiseScale >= 0 whenever we update noise scale,
      // this would only happen if noiseScale was never updated,
      // meaning empty groups, meaning return NULL for all groups
      for (auto i = 0; i < numGroups; ++i) {
        vector->setNull(i, true);
      }
      return;
    }

    folly::Random::DefaultGenerator rng;

    // Check if random seed is provided. If so, use it to seed the random
    bool seedFound = false;
    for (auto i = 0; i < numGroups && !seedFound; i++) {
      auto group = groups[i];
      if (!isNull(group)) {
        auto* accumulator = value<AccumulatorType>(group);
        if (accumulator->randomSeed.has_value()) {
          rng.seed(*accumulator->randomSeed);
          seedFound = true;
        }
      }
    }

    // Otherwise, generate a cryptographically secure random byte string as the
    // seed for random generator
    if (!seedFound) {
      rng.seed(folly::Random::secureRand32());
    }

    // This is to deal with the case when noiseScale == 0, which means we do not
    // need to add noise. We assume noiseScale = 0, thus no need to add noise,
    // and set dist to standard normal distribution
    std::normal_distribution<double> dist(0.0, 1.0);
    bool addNoise = false;

    if (noiseScale > 0) {
      // We need to add noise
      dist = std::normal_distribution<double>(0.0, noiseScale);
      addNoise = true;
    }

    auto* rawValues = vector->mutableRawValues();
    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        rawValues[i] = 0; // Return 0 for null group to match Java behavior
      } else {
        auto* accumulator = value<AccumulatorType>(group);

        int64_t noise = 0;
        if (addNoise) {
          double rawNoise = dist(rng);
          VELOX_CHECK_GT(rawNoise, -1.0 * std::numeric_limits<int64_t>::max());
          VELOX_CHECK_LE(rawNoise, std::numeric_limits<int64_t>::max());
          noise = static_cast<int64_t>(
              std::round(rawNoise)); // Need to round back to int64_t because
                                     // we want to return int64_t
        }

        // Check and make sure the count is within int64_t range
        int64_t trueCount = static_cast<int64_t>(accumulator->count);
        VELOX_DCHECK_LT(trueCount, std::numeric_limits<int64_t>::max());
        int64_t noisyCount = checkedPlus(trueCount, noise);

        // Post-process the noisy count to make sure it is non-negative
        if (noisyCount < 0) {
          noisyCount = 0;
        }

        rawValues[i] = noisyCount;
      }
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    // Add raw input to the accumulator
    decodedValue_.decode(*args[0], rows);
    decodedNoiseScale_.decode(*args[1], rows);

    // If intput has random seed, decode it
    if (args.size() == 3 && args[2]->isConstantEncoding()) {
      decodedRandomSeed_.decode(*args[2], rows);
    }

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedValue_.isNullAt(i) || decodedNoiseScale_.isNullAt(i)) {
        return;
      }

      auto* group = groups[i];

      auto* accumulator = value<AccumulatorType>(group);
      if (decodedValue_.valueAt<bool>(i)) {
        accumulator->increaseCount(1);
      }

      double noiseScaleValue = 0.0;
      auto noiseScaleType = args[1]->typeKind();
      if (noiseScaleType == TypeKind::DOUBLE) {
        noiseScaleValue = decodedNoiseScale_.valueAt<double>(i);
      } else if (noiseScaleType == TypeKind::BIGINT) {
        noiseScaleValue =
            static_cast<double>(decodedNoiseScale_.valueAt<uint64_t>(i));
      }
      accumulator->checkAndSetNoiseScale(noiseScaleValue);

      if (args.size() == 3 && args[2]->isConstantEncoding() &&
          !decodedRandomSeed_.isNullAt(i)) {
        accumulator->setRandomSeed(decodedRandomSeed_.valueAt<int32_t>(i));
      }
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
      if (decoded.isNullAt(i)) {
        return;
      }

      auto* group = groups[i];
      auto* accumulator = value<AccumulatorType>(group);

      auto serialized = decoded.valueAt<StringView>(i);
      auto otherAccumulator = AccumulatorType::deserialize(serialized.data());

      if (accumulator->noiseScale != otherAccumulator.noiseScale &&
          otherAccumulator.noiseScale >= 0) {
        accumulator->checkAndSetNoiseScale(otherAccumulator.noiseScale);
      }

      if (otherAccumulator.randomSeed.has_value()) {
        accumulator->setRandomSeed(*otherAccumulator.randomSeed);
      }

      accumulator->increaseCount(otherAccumulator.count);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    // Add raw input to the accumulator for Global Aggregation
    decodedValue_.decode(*args[0], rows);
    decodedNoiseScale_.decode(*args[1], rows);

    // Check if input has random seed and make sure it's constant for each row.
    if (args.size() == 3 && args[2]->isConstantEncoding()) {
      decodedRandomSeed_.decode(*args[2], rows);
    }

    auto* accumulator = value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedValue_.isNullAt(i) || decodedNoiseScale_.isNullAt(i)) {
        return;
      }
      if (decodedValue_.valueAt<bool>(i)) {
        accumulator->increaseCount(1);
      }

      double noiseScaleValue = 0.0;
      auto noiseScaleType = args[1]->typeKind();
      if (noiseScaleType == TypeKind::DOUBLE) {
        noiseScaleValue = decodedNoiseScale_.valueAt<double>(i);
      } else if (noiseScaleType == TypeKind::BIGINT) {
        noiseScaleValue =
            static_cast<double>(decodedNoiseScale_.valueAt<uint64_t>(i));
      }
      accumulator->checkAndSetNoiseScale(noiseScaleValue);

      if (args.size() == 3 && args[2]->isConstantEncoding() &&
          !decodedRandomSeed_.isNullAt(i)) {
        accumulator->setRandomSeed(decodedRandomSeed_.valueAt<int32_t>(i));
      }
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
      auto serialized = decoded.valueAt<StringView>(i);
      auto otherAccumulator = AccumulatorType::deserialize(serialized.data());

      if (accumulator->noiseScale != otherAccumulator.noiseScale &&
          otherAccumulator.noiseScale >= 0) {
        accumulator->checkAndSetNoiseScale(otherAccumulator.noiseScale);
      }

      if (otherAccumulator.randomSeed.has_value()) {
        accumulator->setRandomSeed(*otherAccumulator.randomSeed);
      }

      accumulator->increaseCount(otherAccumulator.count);
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
