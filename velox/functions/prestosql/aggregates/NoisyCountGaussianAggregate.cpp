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
#include "velox/functions/lib/aggregates/noisy_aggregation/NoisyCountAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {
class NoisyCountGaussianAggregate : public exec::Aggregate {
 public:
  explicit NoisyCountGaussianAggregate(TypePtr resultType)
      : exec::Aggregate(std::move(resultType)) {}

  using AccumulatorType = NoisyCountAccumulator;

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
    decodeInputData(rows, args);

    // Process the args data and update the accumulator for each group.
    rows.applyToSelected([&](vector_size_t i) {
      // If value is null, we do not want to update the accumulator.
      if (decodedValue_.isNullAt(i) || decodedNoiseScale_.isNullAt(i)) {
        return;
      }

      auto group = groups[i];
      auto accumulator = exec::Aggregate::value<AccumulatorType>(group);
      accumulator->increaseCount(1);

      double noiseScale = decodedNoiseScale_.valueAt<double>(i);
      accumulator->checkAndSetNoiseScale(noiseScale);
    });
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto flatResult = (*result)->asFlatVector<StringView>();
    flatResult->resize(numGroups);

    auto numOfValidGroups = 0;
    for (auto i = 0; i < numGroups; i++) {
      numOfValidGroups += !isNull(groups[i]);
    }
    size_t totalSize = numOfValidGroups * AccumulatorType::serializedSize();

    // Allocate buffer for serialized data.
    auto rawBuffer = flatResult->getRawStringBufferWithSpace(totalSize);
    size_t offset = 0;
    auto size = AccumulatorType::serializedSize();

    for (auto i = 0; i < numGroups; i++) {
      auto group = groups[i];
      if (isNull(group)) {
        flatResult->setNull(i, true);
        continue;
      }

      auto accumulator = exec::Aggregate::value<AccumulatorType>(group);

      // Write to the pre-allocated buffer.
      accumulator->serialize(rawBuffer + offset);
      flatResult->setNoCopy(
          i, StringView(rawBuffer + offset, static_cast<int32_t>(size)));
      offset += size;
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    auto decodedVector = DecodedVector(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedVector.isNullAt(i)) {
        return;
      }

      auto group = groups[i];
      auto accumulator = exec::Aggregate::value<AccumulatorType>(group);

      auto serialized = decodedVector.valueAt<StringView>(i);
      auto otherAccumulator = AccumulatorType::deserialize(serialized.data());

      accumulator->increaseCount(otherAccumulator.count);

      if (accumulator->noiseScale != otherAccumulator.noiseScale &&
          otherAccumulator.noiseScale >= 0) {
        accumulator->checkAndSetNoiseScale(otherAccumulator.noiseScale);
      }
    });
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* flatResult = (*result)->as<FlatVector<int64_t>>();
    VELOX_CHECK(flatResult);
    flatResult->resize(numGroups);

    // Find the noise scale from group.
    double noiseScale = -1;
    for (auto i = 0; i < numGroups; i++) {
      auto group = groups[i];
      if (!isNull(group)) {
        auto* accumulator = value<AccumulatorType>(group);
        noiseScale = accumulator->noiseScale;
        // In situations where the aggregated value is null but the group by key
        // is not, we skipped updating the accumulator which means the noise
        // scale of that group by is -1. We need to find a valid noise scale
        // before break.
        if (noiseScale >= 0) {
          break;
        }
      }
    }

    // If noise scale is never set, either the group is null or the input is
    // empty, To be consistent with Java, return null for all groups.
    if (noiseScale < 0) {
      for (auto i = 0; i < numGroups; ++i) {
        flatResult->setNull(i, true);
      }
      return;
    }

    folly::Random::DefaultGenerator rng;

    // Create a normal distribution with mean 0 and standard deviation noise.
    std::normal_distribution<double> distribution{0.0, 1.0};
    if (noiseScale > 0) {
      distribution = std::normal_distribution<double>(0.0, noiseScale);
    }

    for (auto i = 0; i < numGroups; i++) {
      auto group = groups[i];
      if (isNull(group)) {
        flatResult->set(i, 0); // Return 0 for null group to match Java behavior
      } else {
        auto* accumulator = value<AccumulatorType>(group);
        // If group by is not null but noise scale is invalid, it means
        // that the input data for this group are nulls, we return null for
        // this group instead of 0.
        if (accumulator->noiseScale < 0) {
          flatResult->setNull(i, true);
          continue;
        }
        auto trueCount = static_cast<int64_t>(accumulator->count);

        // Add noise to the count.
        int64_t noise = 0;
        if (noiseScale > 0) {
          noise = static_cast<int64_t>(std::round(distribution(rng)));
        }
        int64_t noisyCount = trueCount + noise;

        // Post-process the noisy count to make sure it is non-negative
        flatResult->set(i, std::max<int64_t>(noisyCount, 0));
      }
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
      if (decodedVector.isNullAt(i)) {
        return;
      }

      auto serialized = decodedVector.valueAt<StringView>(i);
      auto otherAccumulator = AccumulatorType::deserialize(serialized.data());

      accumulator->increaseCount(otherAccumulator.count);

      if (accumulator->noiseScale != otherAccumulator.noiseScale &&
          otherAccumulator.noiseScale >= 0) {
        accumulator->checkAndSetNoiseScale(otherAccumulator.noiseScale);
      }
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    decodeInputData(rows, args);
    auto accumulator = exec::Aggregate::value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedValue_.isNullAt(i) || decodedNoiseScale_.isNullAt(i)) {
        return;
      }

      accumulator->increaseCount(1);
      double noiseScale = decodedNoiseScale_.valueAt<double>(i);
      accumulator->checkAndSetNoiseScale(noiseScale);
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

  // Helper function to decode the input data.
  void decodeInputData(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    decodedValue_.decode(*args[0], rows);
    decodedNoiseScale_.decode(*args[1], rows);
  }

 private:
  DecodedVector decodedValue_;
  DecodedVector decodedNoiseScale_;
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
        VELOX_USER_CHECK_EQ(
            argTypes.size(), 2, "{} takes exactly 2 arguments", name);

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
