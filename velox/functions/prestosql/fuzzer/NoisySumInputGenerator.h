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

#include <folly/Executor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <vector>
#include "velox/exec/fuzzer/InputGenerator.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

class NoisySumInputGenerator : public InputGenerator {
 public:
  /// Generate input for the noisy_sum_gaussian function.
  /// The signature takes 2-5 arguments.
  /// noisy_sum_gaussian(col, noiseScale[[, lower, upper], randomSeed])
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    vector_size_t size = static_cast<int32_t>(fuzzer.getOptions().vectorSize);
    std::vector<VectorPtr> result;

    // Make sure to use the same value of 'noiseScale' for all batches inputs,
    // so we only set it once.
    if (!noiseScale_.has_value()) {
      noiseScale_ =
          boost::random::uniform_real_distribution<double>(0.0, 100.0)(rng);
    }

    // Process each type in the input.
    // Types of parameters in noisy_sum_gaussian(col, noiseScale, lower, upper,
    // randomSeed)
    VELOX_CHECK(types.size() >= 2);
    VELOX_CHECK(types.size() <= 5);
    for (size_t i = 0; i < types.size(); ++i) {
      const auto& type = types[i];

      // For the first argument which is a numeric column
      if (i == 0) {
        // Create a flat vector for sum aggregation. Signature only accept
        // numeric type.
        auto flatVector = fuzzer.fuzzFlat(type, size);
        result.push_back(flatVector);
      }
      // For the second argument (noise scale)
      else if (i == 1) {
        if (type->isDouble()) {
          result.push_back(
              BaseVector::createConstant(DOUBLE(), *noiseScale_, size, pool));
        } else if (type->isBigint()) {
          // Create a variant with the correct integer value
          variant intValue = static_cast<int64_t>(*noiseScale_);
          result.push_back(
              BaseVector::createConstant(BIGINT(), intValue, size, pool));
        }
      }
      // For the third argument
      else if (i == 2) {
        // If the third arg is random seed
        if (types.size() == 3) {
          if (!randomSeed_.has_value()) {
            randomSeed_ =
                boost::random::uniform_int_distribution<int64_t>(0, 12345)(rng);
          }
          result.push_back(
              BaseVector::createConstant(BIGINT(), *randomSeed_, size, pool));
        }
        // size >= 4 means it has lower and upper bound.
        else {
          // Generate lower and upper bound if they are not set.
          if (!lowerBound_.has_value()) {
            lowerBound_ = boost::random::uniform_real_distribution<double>(
                -1000.0, 1000.0)(rng);
            // Make sure lower bound is less than upper bound.
            upperBound_ = boost::random::uniform_real_distribution<double>(
                              100.0, 1000.0)(rng) +
                *lowerBound_;
          }

          if (type->isDouble()) {
            result.push_back(
                BaseVector::createConstant(DOUBLE(), *lowerBound_, size, pool));
          } else if (type->isBigint()) {
            // Create a variant with the correct integer value
            variant intValue = static_cast<int64_t>(*lowerBound_);
            result.push_back(
                BaseVector::createConstant(BIGINT(), intValue, size, pool));
          }
        }
      }
      // For the fourth argument(upper bound)
      else if (i == 3) {
        // When type size is GE 4, it is guaranteed to have lower and upper
        // bound.
        if (type->isDouble()) {
          result.push_back(
              BaseVector::createConstant(DOUBLE(), *upperBound_, size, pool));
        } else if (type->isBigint()) {
          // Create a variant with the correct integer value
          variant intValue = static_cast<int64_t>(*upperBound_);
          result.push_back(
              BaseVector::createConstant(BIGINT(), intValue, size, pool));
        }
      }
      // For the fifth argument(random seed)
      else if (i == 4) {
        VELOX_CHECK(type->isBigint());
        if (!randomSeed_.has_value()) {
          randomSeed_ =
              boost::random::uniform_int_distribution<int64_t>(0, 12345)(rng);
        }
        result.push_back(
            BaseVector::createConstant(BIGINT(), *randomSeed_, size, pool));
      }
    }

    return result;
  }
  void reset() override {
    noiseScale_.reset();
    lowerBound_.reset();
    upperBound_.reset();
    randomSeed_.reset();
  }

 private:
  std::optional<double> noiseScale_;
  std::optional<double> lowerBound_;
  std::optional<double> upperBound_;
  std::optional<int64_t> randomSeed_;
};

} // namespace facebook::velox::exec::test
