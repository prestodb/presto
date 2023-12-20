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
  // of rows with user defined distributions.

  constexpr int sampleSize = 100000;
  constexpr int32_t lo = 0, hi = 10;
  constexpr double mu = 5.0, sigma = 2.0;
  constexpr double p = 0.25;
  constexpr size_t norm = 600;

  auto uniform = std::uniform_int_distribution<int32_t>(lo, hi);
  auto normal = std::normal_distribution<double>(mu, sigma);
  auto bernoulli = std::bernoulli_distribution(p);

  FuzzerGenerator rng;
  memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();

  GeneratorSpecPtr randomRow = RANDOM_ROW(
      {RANDOM_INTEGER(uniform),
       RANDOM_DOUBLE(normal),
       RANDOM_BOOLEAN(bernoulli)});

  VectorPtr vector = randomRow->generateData(rng, pool.get(), sampleSize);
  RowVector* rowVector = vector->as<RowVector>();

  VectorPtr uniformVector = rowVector->childAt(0);
  VectorPtr normalVector = rowVector->childAt(1);
  VectorPtr bernoulliVector = rowVector->childAt(2);

  std::cout
      << "First element of row column. Data generated from uniform distribution:\n"
      << plotVector(uniformVector->asFlatVector<int32_t>()) << "\n";

  std::cout
      << "Second element of row column. Data generated from normal distribution:\n"
      << plotVector(normalVector->asFlatVector<double>()) << "\n";

  std::cout
      << "Third element of row column. Data generated from Bernoulli distribution:\n"
      << plotVector(bernoulliVector->asFlatVector<bool>(), norm) << "\n";

  return 0;
}
