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

#include <boost/random/uniform_01.hpp>
#include "velox/expression/fuzzer/DecimalArgGeneratorBase.h"

namespace facebook::velox::exec::test {

class TruncateArgGenerator : public fuzzer::ArgGenerator {
 public:
  std::vector<TypePtr> generateArgs(
      const exec::FunctionSignature& signature,
      const TypePtr& returnType,
      FuzzerGenerator& rng) override {
    // Only the single-arg truncate function is supported because
    // ArgumentTypeFuzzer can generate argument types for the two-arg truncate
    // function.
    VELOX_CHECK_EQ(1, signature.argumentTypes().size());
    // Generates a decimal type following below formulas:
    // p = max(p1 - s1, 1)
    // s = 0
    const auto [p, s] = getDecimalPrecisionScale(*returnType);
    if (s != 0) {
      return {};
    }

    // p1 = s1
    if (p == 1 && coinToss(rng)) {
      const auto s1 = rand32(1, 38, rng);
      return {DECIMAL(s1, s1)};
    }

    const auto s1 = rand32(0, 38 - p, rng);
    return {DECIMAL(p + s1, s1)};
  }

 private:
  // Returns true 50% of times.
  static bool coinToss(FuzzerGenerator& rng) {
    return boost::random::uniform_01<double>()(rng) < 0.5;
  }

  static uint32_t rand32(uint32_t min, uint32_t max, FuzzerGenerator& rng) {
    return boost::random::uniform_int_distribution<uint32_t>(min, max)(rng);
  }
};

} // namespace facebook::velox::exec::test
