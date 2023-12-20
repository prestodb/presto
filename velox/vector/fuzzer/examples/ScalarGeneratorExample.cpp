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

#include <iostream>

#include "velox/vector/FlatVector.h"
#include "velox/vector/fuzzer/GeneratorSpec.h"
#include "velox/vector/fuzzer/examples/Utils.h"

using namespace facebook::velox;
using namespace generator_spec_maker;
using namespace generator_spec_examples;

int main() {
  // This example shows how to use our GeneratorSpec class to generate vectors
  // of scalars with user defined distributions.

  constexpr int sampleSize = 100000;
  constexpr int32_t lo = 0, hi = 10;
  constexpr double mu = 5.0, sigma = 2.0;
  constexpr double userLo = 0.01, userHi = 0.99;
  constexpr double nullProbability = 0.18;

  auto uniform = std::uniform_int_distribution<int32_t>(lo, hi);
  auto normal = std::normal_distribution<double>(mu, sigma);
  auto coinToss = std::uniform_real_distribution<double>();
  auto userDefined = [userLo, userHi, &coinToss](Rng& rng) -> double {
    auto val = coinToss(rng);
    if (val <= userLo) {
      return val;
    } else if (val > userHi) {
      return val * val;
    } else {
      return 0.0;
    }
  };

  FuzzerGenerator rng;
  memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();

  GeneratorSpecPtr uniformIntGenerator =
      RANDOM_INTEGER(uniform, nullProbability);
  GeneratorSpecPtr normalDoubleGenerator =
      RANDOM_DOUBLE(normal, nullProbability);
  GeneratorSpecPtr userDefinedGenerator =
      RANDOM_DOUBLE(userDefined, nullProbability);

  VectorPtr uniformVector =
      uniformIntGenerator->generateData(rng, pool.get(), sampleSize);
  VectorPtr normalVector =
      normalDoubleGenerator->generateData(rng, pool.get(), sampleSize);
  VectorPtr userDefinedVector =
      userDefinedGenerator->generateData(rng, pool.get(), sampleSize);

  std::cout << "Sample data generated from uniform distribution:\n"
            << plotVector(uniformVector->asFlatVector<int32_t>()) << "\n";

  std::cout << "Sample data generated from normal distribution:\n"
            << plotVector(normalVector->asFlatVector<double>()) << "\n";

  std::cout << "Sample data generated from user defined distribution:\n"
            << plotVector(userDefinedVector->asFlatVector<double>()) << "\n";
  return 0;
}
