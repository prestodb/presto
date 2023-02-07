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
  // of arrays with user defined distributions.

  constexpr int sampleSize = 100;
  constexpr int32_t lo = 100, hi = 1000;
  constexpr double mu = 5.0, sigma = 2.0;
  constexpr double p = 0.25;
  constexpr double nullProbability = 0.38;

  auto normal = std::normal_distribution<double>(mu, sigma);
  auto uniform = std::uniform_int_distribution<int32_t>(lo, hi);

  FuzzerGenerator rng;
  auto pool = memory::getDefaultMemoryPool();

  GeneratorSpecPtr randomArray =
      RANDOM_ARRAY(RANDOM_DOUBLE(normal, nullProbability), uniform);

  VectorPtr vector = randomArray->generateData(rng, pool.get(), sampleSize);
  ArrayVector* arrayVector = vector->as<ArrayVector>();
  VectorPtr elements = arrayVector->elements();

  std::cout
      << "Underlying elements of ArrayVector generated from normal distribution:\n"
      << plotVector(elements->asFlatVector<double>()) << "\n";

  return 0;
}
