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

#include "velox/functions/prestosql/aggregates/NoisyHelperFunctionFactory.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate::prestosql {

void NoisyHelperFunctionFactory::decodeInputData(
    DecodedVector& decodedValue_,
    DecodedVector& decodedNoiseScale_,
    DecodedVector& decodedLowerBound_,
    DecodedVector& decodedUpperBound_,
    DecodedVector& decodedRandomSeed_,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args) {
  decodedValue_.decode(*args[0], rows);
  decodedNoiseScale_.decode(*args[1], rows);

  // Decode lower and upper bounds if provided.
  if (args.size() > 3) {
    decodedLowerBound_.decode(*args[2], rows);
    decodedUpperBound_.decode(*args[3], rows);
  }

  // Decode random seed if provided.
  if (args.size() == 3) {
    decodedRandomSeed_.decode(*args[2], rows);
  }

  if (args.size() == 5) {
    decodedRandomSeed_.decode(*args[4], rows);
  }
}

// Overloaded version for functions that don't use bounds (like noisy_count)
void NoisyHelperFunctionFactory::decodeInputData(
    DecodedVector& decodedValue_,
    DecodedVector& decodedNoiseScale_,
    DecodedVector& decodedRandomSeed_,
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args) {
  decodedValue_.decode(*args[0], rows);
  decodedNoiseScale_.decode(*args[1], rows);

  // Decode random seed if provided.
  if (args.size() == 3) {
    decodedRandomSeed_.decode(*args[2], rows);
  }
}

void NoisyHelperFunctionFactory::updateAccumulatorFromInput(
    DecodedVector& decodedValue_,
    DecodedVector& decodedNoiseScale_,
    DecodedVector& decodedLowerBound_,
    DecodedVector& decodedUpperBound_,
    DecodedVector& decodedRandomSeed_,
    const std::vector<VectorPtr>& args,
    AccumulatorType& accumulator,
    vector_size_t i,
    bool hasBounds,
    bool hasRandomSeed) {
  if (decodedValue_.isNullAt(i)) {
    return;
  }

  // Update the noise scale.
  NoisyHelperFunctionFactory::updateNoiseScale(
      decodedNoiseScale_, args, accumulator, i);

  // Update the lower and upper bounds if provided.
  if (hasBounds) {
    NoisyHelperFunctionFactory::updateBounds(
        decodedLowerBound_, decodedUpperBound_, args, accumulator, i);
  }

  // Update random seed if provided.
  if (hasRandomSeed) {
    NoisyHelperFunctionFactory::updateRandomSeed(
        decodedRandomSeed_, accumulator, i);
  }

  // Update sum and count. check input value and dispatch to corresponding
  // type.
  auto inputType = args[0]->typeKind();
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      updateTemplate, inputType, accumulator, decodedValue_, i);
}

void NoisyHelperFunctionFactory::updateAccumulatorFromInput(
    DecodedVector& decodedValue_,
    DecodedVector& decodedNoiseScale_,
    DecodedVector& decodedRandomSeed_,
    const std::vector<VectorPtr>& args,
    AccumulatorType& accumulator,
    vector_size_t i,
    bool hasRandomSeed,
    bool isCountIf) {
  if (decodedValue_.isNullAt(i)) {
    return;
  }

  // Update the noise scale.
  NoisyHelperFunctionFactory::updateNoiseScale(
      decodedNoiseScale_, args, accumulator, i);

  // Update random seed if provided.
  if (hasRandomSeed) {
    NoisyHelperFunctionFactory::updateRandomSeed(
        decodedRandomSeed_, accumulator, i);
  }

  if (isCountIf) {
    if (decodedValue_.valueAt<bool>(i)) {
      accumulator.updateCount(1);
    }
  } else {
    accumulator.updateCount(1);
  }
}

void NoisyHelperFunctionFactory::updateAccumulatorFromIntermediateResult(
    AccumulatorType& accumulator,
    DecodedVector& decodedVector,
    vector_size_t i) {
  if (decodedVector.isNullAt(i)) {
    return;
  }

  auto serialized = decodedVector.valueAt<StringView>(i);
  auto otherAccumulator = AccumulatorType::deserialize(serialized.data());
  accumulator.updateSum(otherAccumulator.getSum());
  accumulator.updateCount(otherAccumulator.getCount());
  if (otherAccumulator.getNoiseScale() >= 0) {
    accumulator.checkAndSetNoiseScale(otherAccumulator.getNoiseScale());
  }
  if (otherAccumulator.getLowerBound().has_value() &&
      otherAccumulator.getUpperBound().has_value()) {
    accumulator.checkAndSetBounds(
        *otherAccumulator.getLowerBound(), *otherAccumulator.getUpperBound());
  }
  if (otherAccumulator.getRandomSeed().has_value()) {
    accumulator.setRandomSeed(*otherAccumulator.getRandomSeed());
  }
}

double NoisyHelperFunctionFactory::postProcessNoisyValue(
    double noisyValue,
    const AccumulatorType& accumulator) {
  if (accumulator.getLowerBound().has_value() &&
      accumulator.getUpperBound().has_value()) {
    if (accumulator.getLowerBound().value() >= 0) {
      noisyValue = std::max(noisyValue, 0.0);
    } else if (accumulator.getUpperBound().value() <= 0) {
      noisyValue = std::min(noisyValue, 0.0);
    }
  }
  return noisyValue;
}

void NoisyHelperFunctionFactory::extractAccumulators(
    const std::function<bool(char*)>& isNull,
    const std::function<AccumulatorType(char*)>& getAccumulator,
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
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
      auto accumulator = getAccumulator(group);

      // Write to the pre-allocated buffer.
      accumulator.serialize(rawBuffer + offset);
      flatResult->setNoCopy(
          i, StringView(rawBuffer + offset, static_cast<int32_t>(size)));
      offset += size;
    }
  }
}

const std::pair<double, std::optional<int64_t>>
NoisyHelperFunctionFactory::getFinalNoiseScaleAndRandomSeed(
    const std::function<bool(char*)>& isNull,
    const std::function<AccumulatorType(char*)>& getAccumulator,
    char** groups,
    int32_t numGroups) {
  double noiseScale = -1;
  std::optional<int64_t> randomSeed = std::nullopt;
  for (auto i = 0; i < numGroups; ++i) {
    char* group = groups[i];
    if (!isNull(group)) {
      auto accumulator = getAccumulator(group);
      noiseScale = accumulator.getNoiseScale();
      randomSeed = accumulator.getRandomSeed();
      // Only exit when a valid noise_scale is found.
      if (noiseScale >= 0) {
        break;
      }
    }
  }
  return std::make_pair(noiseScale, randomSeed);
}

void NoisyHelperFunctionFactory::updateNoiseScale(
    DecodedVector& decodedNoiseScale_,
    const std::vector<VectorPtr>& args,
    AccumulatorType& accumulator,
    vector_size_t i) {
  double noiseScale = 0;
  auto noiseScaleType = args[1]->typeKind();
  if (noiseScaleType == TypeKind::DOUBLE) {
    noiseScale = decodedNoiseScale_.valueAt<double>(i);
  } else if (noiseScaleType == TypeKind::BIGINT) {
    noiseScale = static_cast<double>(decodedNoiseScale_.valueAt<uint64_t>(i));
  }
  accumulator.checkAndSetNoiseScale(noiseScale);
}

void NoisyHelperFunctionFactory::updateBounds(
    DecodedVector& decodedLowerBound_,
    DecodedVector& decodedUpperBound_,
    const std::vector<VectorPtr>& args,
    AccumulatorType& accumulator,
    vector_size_t i) {
  double lowerBound = 0;
  double upperBound = 0;
  auto lowerBoundType = args[2]->typeKind();
  auto upperBoundType = args[3]->typeKind();
  if (lowerBoundType == TypeKind::DOUBLE) {
    lowerBound = decodedLowerBound_.valueAt<double>(i);
  } else if (lowerBoundType == TypeKind::BIGINT) {
    lowerBound = static_cast<double>(decodedLowerBound_.valueAt<int64_t>(i));
  }

  if (upperBoundType == TypeKind::DOUBLE) {
    upperBound = decodedUpperBound_.valueAt<double>(i);
  } else if (upperBoundType == TypeKind::BIGINT) {
    upperBound = static_cast<double>(decodedUpperBound_.valueAt<int64_t>(i));
  }
  accumulator.checkAndSetBounds(lowerBound, upperBound);
}

void NoisyHelperFunctionFactory::updateRandomSeed(
    DecodedVector& decodedRandomSeed_,
    AccumulatorType& accumulator,
    vector_size_t i) {
  accumulator.setRandomSeed(decodedRandomSeed_.valueAt<int64_t>(i));
}

} // namespace facebook::velox::aggregate::prestosql
