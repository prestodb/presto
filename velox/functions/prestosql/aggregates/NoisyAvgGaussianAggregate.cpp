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
#include "velox/functions/lib/aggregates/noisy_aggregation/NoisyAvgAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {
class NoisyAvgGaussianAggregate : public exec::Aggregate {
 public:
  explicit NoisyAvgGaussianAggregate(TypePtr resultType)
      : exec::Aggregate(std::move(resultType)) {}

  using AccumulatorType = functions::aggregate::NoisyAvgAccumulator;

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
    decodeInputData(rows, args);

    // Process the args data and update the accumulator for each group.
    rows.applyToSelected([&](vector_size_t i) {
      auto* accumulator = value<AccumulatorType>(groups[i]);
      updateAccumulatorFromInput(args, accumulator, i);
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
      updateAccumulatorFromInput(args, accumulator, i);
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
      updateAccumulatorFromIntermediateResult(accumulator, decodedVector, i);
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
      updateAccumulatorFromIntermediateResult(accumulator, decodedVector, i);
    });
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto flatResult = (*result)->asFlatVector<StringView>();
    VELOX_CHECK(flatResult);
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
        uint64_t trueCount = accumulator->getCount();
        double trueSum = accumulator->getSum();
        VELOX_CHECK_LE(trueCount, std::numeric_limits<double>::max());
        double trueAvg = trueSum / static_cast<double>(trueCount);
        double noise = addNoise ? dist(rng) : 0;
        flatResult->set(i, trueAvg + noise);
      }
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

  void decodeInputData(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    decodedValue_.decode(*args[0], rows);
    decodedNoiseScale_.decode(*args[1], rows);
  }

  void updateAccumulatorFromInput(
      const std::vector<VectorPtr>& args,
      AccumulatorType* accumulator,
      vector_size_t i) {
    if (decodedValue_.isNullAt(i) || decodedNoiseScale_.isNullAt(i)) {
      return;
    }
    double noiseScale = 0;
    auto noiseScaleType = args[1]->typeKind();
    if (noiseScaleType == TypeKind::DOUBLE) {
      noiseScale = decodedNoiseScale_.valueAt<double>(i);
    } else if (noiseScaleType == TypeKind::BIGINT) {
      noiseScale = static_cast<double>(decodedNoiseScale_.valueAt<uint64_t>(i));
    }
    accumulator->checkAndSetNoiseScale(noiseScale);

    // Update sum and count. check input value and dispatch to corresponding
    // type.
    auto inputType = args[0]->typeKind();
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        updateTemplate, inputType, accumulator, decodedValue_, i);
  }

  void updateAccumulatorFromIntermediateResult(
      AccumulatorType* accumulator,
      DecodedVector& decodedVector,
      vector_size_t i) {
    if (decodedVector.isNullAt(i)) {
      return;
    }

    auto serialized = decodedVector.valueAt<StringView>(i);
    auto otherAccumulator = AccumulatorType::deserialize(serialized.data());
    accumulator->updateSum(otherAccumulator.getSum());
    accumulator->updateCount(otherAccumulator.getCount());
    if (otherAccumulator.getNoiseScale() >= 0) {
      accumulator->checkAndSetNoiseScale(otherAccumulator.getNoiseScale());
    }
  }

  // Template helper function to update accumulator, can support all numeric
  // data types. Only used in this class.
  template <TypeKind TData>
  void updateTemplate(
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

        accumulator->updateSum(doubleValue);
        accumulator->updateCount(1);
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
      VELOX_FAIL("NoisyAvgGaussianAggregate does not support this data type.");
    } else {
      accumulator->updateSum(static_cast<double>(decodedValue.valueAt<T>(i)));
      accumulator->updateCount(1);
    }
  }
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

  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  // Generate signatures for all type combinations.
  for (const auto& noiseScaleType : noiseScaleTypes) {
    // Handle simple types.
    for (const auto& dataType : simpleDataTypes) {
      signatures.push_back(createBuilder()
                               .argumentType(dataType)
                               .argumentType(noiseScaleType)
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
        VELOX_CHECK_EQ(
            argTypes.size(), 2, "{} takes exactly two arguments", name);
        if (exec::isPartialOutput(step)) {
          return std::make_unique<NoisyAvgGaussianAggregate>(VARBINARY());
        }
        return std::make_unique<NoisyAvgGaussianAggregate>(DOUBLE());
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
