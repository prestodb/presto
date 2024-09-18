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
#include "velox/expression/fuzzer/ArgGenerator.h"

namespace facebook::velox::exec::test {

class FloorAndRoundArgGenerator : public fuzzer::ArgGenerator {
 public:
  std::vector<TypePtr> generateArgs(
      const exec::FunctionSignature& signature,
      const TypePtr& returnType,
      FuzzerGenerator& rng) override {
    if (signature.argumentTypes().size() == 1) {
      return generateSingleArg(returnType, rng);
    }
    VELOX_CHECK_EQ(2, signature.argumentTypes().size());
    return generateTwoArgs(returnType);
  }

 private:
  // Generates a decimal type following below formulas:
  // p = p1 - s1 + min(s1, 1)
  // s = 0
  std::vector<TypePtr> generateSingleArg(
      const TypePtr& returnType,
      FuzzerGenerator& rng) {
    const auto [p, s] = getDecimalPrecisionScale(*returnType);
    if (s != 0) {
      return {};
    }

    const auto s1 = rand32(38 - p + 1, rng);
    if (s1 == 0) {
      return {DECIMAL(p, 0)};
    }

    return {DECIMAL(p - 1 + s1, s1)};
  }

  // Generates a decimal type and an integer type. Decimal type is generated
  // following below formulas:
  // p = p1 + 1
  // s = s1
  std::vector<TypePtr> generateTwoArgs(const TypePtr& returnType) {
    auto [p, s] = getDecimalPrecisionScale(*returnType);
    if (p == 1 || p == s) {
      return {};
    }

    return {DECIMAL(p - 1, s), INTEGER()};
  }

  static uint32_t rand32(uint32_t max, FuzzerGenerator& rng) {
    return boost::random::uniform_int_distribution<uint32_t>()(rng) % max;
  }
};

} // namespace facebook::velox::exec::test
