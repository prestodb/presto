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
  // This example shows how to use our EncoderSpec class to generate vectors
  // of scalars with user defined encoding distributions.

  constexpr size_t numIters = 1000;
  constexpr int sampleSize = 100;
  constexpr double mu = 5.0, sigma = 2.0;
  constexpr double nullProbability = 0.67;

  auto normal = std::normal_distribution<double>(mu, sigma);

  auto realGenerator = std::uniform_real_distribution<>(0.0, 1.0);
  auto threshold = 0.367;
  auto encoding = [&](FuzzerGenerator& rng) {
    auto val = realGenerator(rng);
    if (val <= threshold) {
      return GeneratorSpec::EncoderSpecCodes::CONSTANT;
    } else {
      return GeneratorSpec::EncoderSpecCodes::DICTIONARY;
    }
  };

  FuzzerGenerator rng;
  auto pool = memory::getDefaultMemoryPool();
  GeneratorSpecPtr encodedNormalDoubleGenerator =
      ENCODE(RANDOM_DOUBLE(normal, nullProbability), encoding, 1, 5, 0.3);

  int32_t numConst = 0, numDict = 0;
  for (size_t i = 0; i < numIters; ++i) {
    auto vec =
        encodedNormalDoubleGenerator->generateData(rng, pool.get(), sampleSize);
    if (vec->encoding() == VectorEncoding::Simple::CONSTANT) {
      ++numConst;
    } else if (vec->encoding() == VectorEncoding::Simple::DICTIONARY) {
      ++numDict;
    } else {
      std::cout << "Unxpected encoding!!!\n";
      return 0;
    }
  }

  std::cout << "Probability of constant encoding = "
            << (double)numConst / ((double)numConst + (double)numDict) << "\n";

  return 0;
}
