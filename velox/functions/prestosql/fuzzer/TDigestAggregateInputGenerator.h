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
#include <boost/random/uniform_real_distribution.hpp>

#include "velox/exec/fuzzer/InputGenerator.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

class TDigestAggregateInputGenerator : public InputGenerator {
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

    // Values vector
    VELOX_CHECK(types[0]->isDouble());
    auto valuesVector = fuzzer.fuzz(types[0]);
    inputs.push_back(valuesVector);

    // Weight is optional
    if (types.size() > 1) {
      VELOX_CHECK(types[1]->isBigint());
      auto weightsVector = fuzzer.fuzz(types[1]);
      inputs.push_back(weightsVector);
    }

    // Compression is optional
    if (types.size() > 2) {
      VELOX_CHECK(types[2]->isDouble());
      const auto size = fuzzer.getOptions().vectorSize;
      // Make sure to use the same value of 'compression' for all batches in a
      // given Fuzzer iteration.
      if (!compression_.has_value()) {
        boost::random::uniform_real_distribution<double> dist(10.0, 1000.0);
        compression_ = dist(rng);
      }
      inputs.push_back(BaseVector::createConstant(
          DOUBLE(), compression_.value(), size, pool));
    }

    return inputs;
  }

  void reset() override {
    compression_.reset();
  }

 private:
  std::optional<double> compression_;
};

} // namespace facebook::velox::exec::test
