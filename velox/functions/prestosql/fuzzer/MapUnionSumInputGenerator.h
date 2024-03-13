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

#include <boost/random/uniform_int_distribution.hpp>
#include <string>

#include "velox/exec/fuzzer/InputGenerator.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/fuzzer/GeneratorSpec.h"

namespace facebook::velox::exec::test {

template <typename T>
class ValueDistribution {
 public:
  explicit ValueDistribution(size_t size) : size_{size} {}

  T operator()(FuzzerGenerator& rng) const {
    // Generate inputs within [max / size_, min / size_] so that the sum of
    // size_ values won't exceed max or below min.
    T upperBound = std::numeric_limits<T>::max() / (int64_t)size_;
    T lowerBound = std::numeric_limits<T>::min() / (int64_t)size_;
    return boost::random::uniform_int_distribution<T>(
        lowerBound, upperBound)(rng);
  }

 private:
  size_t size_;
};

class MapUnionSumInputGenerator : public InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& /*rng*/,
      memory::MemoryPool* /*pool*/) override {
    VELOX_CHECK_EQ(types.size(), 1);
    VELOX_CHECK(types[0]->isMap());
    auto mapType = types[0]->asMap();
    auto valueType = mapType.valueType();
    if (valueType->isDouble() || valueType->isReal()) {
      return {};
    }

    std::vector<VectorPtr> result;
    auto keyType = mapType.keyType();
    auto keysVector = fuzzer.fuzzNotNull(keyType);

    VectorPtr valuesVector;
    switch (valueType->kind()) {
      case TypeKind::TINYINT:
        valuesVector = generateValues<TypeKind::TINYINT>(valueType, fuzzer);
        break;
      case TypeKind::SMALLINT:
        valuesVector = generateValues<TypeKind::SMALLINT>(valueType, fuzzer);
        break;
      case TypeKind::INTEGER:
        valuesVector = generateValues<TypeKind::INTEGER>(valueType, fuzzer);
        break;
      case TypeKind::BIGINT:
        valuesVector = generateValues<TypeKind::BIGINT>(valueType, fuzzer);
        break;
      default:
        VELOX_UNREACHABLE();
    }

    return {fuzzer.fuzzMap(
        keysVector, valuesVector, fuzzer.getOptions().vectorSize)};
  }

  void reset() override {}

 private:
  template <TypeKind KIND>
  VectorPtr generateValues(const TypePtr& type, VectorFuzzer& fuzzer) {
    using T = typename TypeTraits<KIND>::NativeType;
    ValueDistribution<T> distribution{fuzzer.getOptions().vectorSize};
    ScalarGeneratorSpec<KIND, ValueDistribution<T>> generatorSpec{
        type, std::move(distribution), fuzzer.getOptions().nullRatio};

    return fuzzer.fuzz(generatorSpec);
  }
};

} // namespace facebook::velox::exec::test
