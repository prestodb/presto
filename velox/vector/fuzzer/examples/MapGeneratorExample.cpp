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
  // of maps with user defined distributions.

  constexpr size_t sampleSize = 10000;
  constexpr int32_t lo = 0, hi = 10;
  constexpr double mu = 5.0, sigma = 2.0;
  constexpr size_t mapSizeLo = 5, mapSizeHi = 10;

  auto lengthDist =
      std::uniform_int_distribution<int32_t>(mapSizeLo, mapSizeHi);
  auto uniform = std::uniform_int_distribution<int32_t>(lo, hi);
  auto normal = std::normal_distribution<double>(mu, sigma);

  FuzzerGenerator rng;
  memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();

  GeneratorSpecPtr randomMap =
      RANDOM_MAP(RANDOM_INTEGER(uniform), RANDOM_DOUBLE(normal), lengthDist);
  VectorPtr vector = randomMap->generateData(rng, pool.get(), sampleSize);
  MapVector* mapVector = vector->as<MapVector>();
  auto uniformVector = mapVector->mapKeys();
  auto normalVector = mapVector->mapValues();

  std::cout
      << "Keys in random MapVector. Data generated from uniform distribution:\n"
      << plotVector(uniformVector->asFlatVector<int32_t>()) << "\n";

  std::cout
      << "Values in random MapVector. Data generated from normal distribution:\n"
      << plotVector(normalVector->asFlatVector<double>()) << "\n";

  return 0;
}
