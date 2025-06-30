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
    bool hasRandomSeed = checkRandomSeed(args);

    rows.applyToSelected([&](vector_size_t i) {
      auto* accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
      // Update random seed if provided.
      if (hasRandomSeed) {
        accumulator->setRandomSeed(decodedRandomSeed_.valueAt<int64_t>(i));
      }

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

      // Update sum. check input value and dispatch to corresponding type.
      auto inputType = args[0]->typeKind();
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          updateSumTemplate, inputType, accumulator, decodedValue_, i);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      [[maybe_unused]] bool mayPushdown) override {
    decodeInputData(rows, args);
    bool hasRandomSeed = checkRandomSeed(args);

    auto* accumulator = exec::Aggregate::value<AccumulatorType>(group);

    rows.applyToSelected([&](vector_size_t i) {
      // Update random seed if provided.
      if (hasRandomSeed) {
        accumulator->setRandomSeed(decodedRandomSeed_.valueAt<int64_t>(i));
      }

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

      // Update sum. check input value and dispatch to corresponding type.
      auto inputType = args[0]->typeKind();
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          updateSumTemplate, inputType, accumulator, decodedValue_, i);
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

    // Initialize the random generator and seed with random_seed if provided.
    folly::Random::DefaultGenerator rng;
    bool hasRandomSeed = false;
    for (auto i = 0; i < numGroups; ++i) {
      if (!isNull(groups[i])) {
        auto accumulator = exec::Aggregate::value<AccumulatorType>(groups[i]);
        if (accumulator->getRandomSeed().has_value()) {
          rng.seed(accumulator->getRandomSeed().value());
          hasRandomSeed = true;
          break;
        }
      }
    }

    if (!hasRandomSeed) {
      rng.seed(folly::Random::secureRand32());
    }

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

      // Update random seed.
      if (otherAccumulator.getRandomSeed().has_value()) {
        accumulator->setRandomSeed(*otherAccumulator.getRandomSeed());
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

      // Update random seed.
      if (otherAccumulator.getRandomSeed().has_value()) {
        accumulator->setRandomSeed(*otherAccumulator.getRandomSeed());
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
  DecodedVector decodedRandomSeed_;

  /// Helper function to process input data. Used in addRawInput and
  /// addSingleGroupRawInput.
  void decodeInputData(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    VELOX_CHECK(args.size() >= 2);
    // Decode input values and noise scale
    decodedValue_.decode(*args[0], rows);
    decodedNoiseScale_.decode(*args[1], rows);

    if (args.size() == 3) {
      decodedRandomSeed_.decode(*args[2], rows);
    }
  }

  bool checkRandomSeed(const std::vector<VectorPtr>& args) {
    // If size of args is 3, it means random seed is provided.
    return args.size() == 3;
  }

  // Template helper function to update accumulator, can support all numeric
  // data types. Only used in this class.
  template <TypeKind TData>
  void updateSumTemplate(
      AccumulatorType* accumulator,
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

        accumulator->update(doubleValue);
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
      VELOX_FAIL("NoisySumGaussianAggregate does not support this data type.");
    } else {
      // Handle not a number.
      if (std::isnan(decodedValue.valueAt<T>(i))) {
        return;
      }
      accumulator->update(static_cast<double>(decodedValue.valueAt<T>(i)));
    }
  }
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
    }

    // Handle decimal types separately.
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("double")
                             .intermediateType("varbinary")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType(noiseScaleType)
                             .build());
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("double")
                             .intermediateType("varbinary")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType(noiseScaleType)
                             .argumentType(randomSeedType)
                             .build());
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
            argTypes.size(), 3, "{} takes at most 3 arguments", name);

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
