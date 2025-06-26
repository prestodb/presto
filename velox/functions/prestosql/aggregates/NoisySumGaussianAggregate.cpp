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

#include "velox/functions/prestosql/aggregates/NoisySumGaussianAggregate.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/noisy_aggregation/NoisySumAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {
class NoisySumGaussianAggregate : public exec::Aggregate {
 public:
  explicit NoisySumGaussianAggregate(TypePtr resultType)
      : exec::Aggregate(resultType) {}

  using AccumulatorType = NoisySumAccumulator;

  int32_t accumulatorFixedWidthSize() const override {
    return static_cast<int32_t>(sizeof(AccumulatorType));
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    decodeInputData(rows, args);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedValue_.isNullAt(i) || decodedNoiseScale_.isNullAt(i)) {
        return;
      }

      auto* accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);

      // Update noise scale.
      auto noiseScaleType = args[1]->typeKind();
      if (noiseScaleType == TypeKind::DOUBLE) {
        accumulator->checkAndSetNoiseScale(
            decodedNoiseScale_.valueAt<double>(i));
      } else if (noiseScaleType == TypeKind::BIGINT) {
        accumulator->checkAndSetNoiseScale(
            static_cast<double>(decodedNoiseScale_.valueAt<uint64_t>(i)));
      }

      // Update sum.
      accumulator->update(decodedValue_.valueAt<double>(i));
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    decodeInputData(rows, args);

    auto* accumulator = exec::Aggregate::value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      if (decodedValue_.isNullAt(i) || decodedNoiseScale_.isNullAt(i)) {
        return;
      }

      // Update noise scale.
      auto noiseScaleType = args[1]->typeKind();
      if (noiseScaleType == TypeKind::DOUBLE) {
        accumulator->checkAndSetNoiseScale(
            decodedNoiseScale_.valueAt<double>(i));
      } else if (noiseScaleType == TypeKind::BIGINT) {
        accumulator->checkAndSetNoiseScale(
            static_cast<double>(decodedNoiseScale_.valueAt<uint64_t>(i)));
      }
      // Update sum.
      accumulator->update(decodedValue_.valueAt<double>(i));
    });
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto flatResult = (*result)->asFlatVector<StringView>();
    flatResult->resize(numGroups);

    int32_t numOfValidGroups = 0;
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
      } else {
        auto accumulator = exec::Aggregate::value<AccumulatorType>(group);

        // Write to the pre-allocated buffer.
        accumulator->serialize(rawBuffer + offset);
        flatResult->setNoCopy(
            i, StringView(rawBuffer + offset, static_cast<int32_t>(size)));
        offset += size;
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto flatResult = (*result)->asFlatVector<double>();
    flatResult->resize(numGroups);

    // Find the noise scale from group.
    double noiseScale = -1.0;
    for (auto i = 0; i < numGroups; ++i) {
      if (!isNull(groups[i])) {
        auto accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
        noiseScale = accumulator->getNoiseScale();
        if (noiseScale >= 0) {
          break;
        }
      }
    }

    // None of the groups have noise scale, return early.
    if (noiseScale < 0) {
      for (auto i = 0; i < numGroups; ++i) {
        flatResult->setNull(i, true);
      }
      return;
    }

    // Initialize the random generator and seed with randomly generated seed.
    folly::Random::DefaultGenerator rng;
    rng.seed(folly::Random::secureRand32());

    std::normal_distribution<double> dist;
    bool addNoise = false;
    if (noiseScale > 0) {
      dist = std::normal_distribution<double>(0.0, noiseScale);
      addNoise = true;
    }

    for (auto i = 0; i < numGroups; i++) {
      auto group = groups[i];
      if (isNull(group)) {
        flatResult->setNull(i, true);
      } else {
        auto accumulator = exec::Aggregate::value<AccumulatorType>(group);
        // Return null for null values in the group.
        if (accumulator->getNoiseScale() < 0) {
          flatResult->setNull(i, true);
          continue;
        }
        double noise = addNoise ? dist(rng) : 0;
        flatResult->set(i, accumulator->getSum() + noise);
      }
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }

      // Update sum from intermediate result.
      auto* accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
      auto serialized = decoded.valueAt<StringView>(i);
      auto otherAccumulator = AccumulatorType::deserialize(serialized.data());
      accumulator->update(otherAccumulator.getSum());

      // Update noise scale.
      if (otherAccumulator.getNoiseScale() >= 0) {
        accumulator->checkAndSetNoiseScale(otherAccumulator.getNoiseScale());
      }
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
      if (decoded.isNullAt(i)) {
        return;
      }

      auto serialized = decoded.valueAt<StringView>(i);
      auto otherAccumulator = AccumulatorType::deserialize(serialized.data());
      accumulator->update(otherAccumulator.getSum());

      // Update noise scale.
      if (otherAccumulator.getNoiseScale() >= 0) {
        accumulator->checkAndSetNoiseScale(otherAccumulator.getNoiseScale());
      }
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

  /// Helper function to process input data. Used in addRawInput and
  /// addSingleGroupRawInput.
  void decodeInputData(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    VELOX_CHECK(args.size() >= 2);
    // Decode input values and noise scale
    decodedValue_.decode(*args[0], rows);
    decodedNoiseScale_.decode(*args[1], rows);
  }
};
} // namespace

void registerNoisySumGaussianAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  // Generate signatures for simple data types
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("double")
                           .intermediateType("varbinary")
                           .argumentType("double") // input type
                           .argumentType("double") // noise_scale type
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("double")
                           .intermediateType("varbinary")
                           .argumentType("double") // input type
                           .argumentType("bigint") // noise_scale type
                           .build());

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
        VELOX_CHECK_EQ(argTypes.size(), 2, "{} takes 2 arguments", name);

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
