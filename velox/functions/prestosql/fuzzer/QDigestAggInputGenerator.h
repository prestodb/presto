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

#include "velox/exec/fuzzer/InputGenerator.h"

namespace facebook::velox::exec::test {

class QDigestAggInputGenerator : public InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    VELOX_CHECK_GE(types.size(), 1);
    VELOX_CHECK_LE(types.size(), 3);

    std::vector<VectorPtr> inputs;
    inputs.reserve(types.size());
    auto valuesVector = fuzzer.fuzz(types[0]);
    inputs.push_back(valuesVector);

    if (types.size() > 1) {
      VELOX_CHECK(types[1]->isBigint());
      auto weightsVector = fuzzer.fuzz(types[1]);
      inputs.push_back(weightsVector);
    }

    if (types.size() > 2) {
      VELOX_CHECK(types[2]->isDouble());
      const auto size = fuzzer.getOptions().vectorSize;
      // Make sure to use the same value of 'accuracy' for all batches in a
      // given Fuzzer iteration.
      if (!accuracy_.has_value()) {
        boost::random::uniform_real_distribution<double> dist(0.00001, 1.0);
        accuracy_ = dist(rng);
      }
      inputs.push_back(
          BaseVector::createConstant(DOUBLE(), accuracy_.value(), size, pool));
    }
    return inputs;
  }

  void reset() override {
    accuracy_.reset();
  }

 private:
  std::optional<double> accuracy_;
};

} // namespace facebook::velox::exec::test
