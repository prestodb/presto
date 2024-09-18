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

namespace facebook::velox::exec::test {

class ApproxDistinctInputGenerator : public InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    if (types.size() != 2) {
      return {};
    }

    // Make sure to use the same value of 'e' for all batches in a given Fuzzer
    // iteration.
    if (!e_.has_value()) {
      // Generate value in [0.0040625, 0.26] range.
      static constexpr double kMin = 0.0040625;
      static constexpr double kMax = 0.26;
      e_ = kMin + (kMax - kMin) * boost::random::uniform_01<double>()(rng);
    }

    const auto size = fuzzer.getOptions().vectorSize;

    VELOX_CHECK(
        types.back()->isDouble(),
        "Unexpected type: {}",
        types.back()->toString());
    return {
        fuzzer.fuzz(types[0]),
        BaseVector::createConstant(DOUBLE(), e_.value(), size, pool)};
  }

  void reset() override {
    e_.reset();
  }

 private:
  std::optional<double> e_;
};

} // namespace facebook::velox::exec::test
