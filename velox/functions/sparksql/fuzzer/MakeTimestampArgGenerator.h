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
#include "velox/functions/sparksql/DecimalUtil.h"

namespace facebook::velox::functions::sparksql::fuzzer {

class MakeTimestampArgGenerator : public velox::fuzzer::ArgGenerator {
 public:
  std::vector<TypePtr> generateArgs(
      const exec::FunctionSignature& signature,
      const TypePtr& returnType,
      FuzzerGenerator& rng) override {
    const auto s = 6;
    const auto p = rand32(s, ShortDecimalType::kMaxPrecision, rng);
    std::vector<TypePtr> types = {
        INTEGER(), INTEGER(), INTEGER(), INTEGER(), INTEGER(), DECIMAL(p, s)};

    if (signature.argumentTypes().size() == 6) {
      return types;
    }
    types.push_back(VARCHAR());
    return types;
  }

 private:
  static uint32_t rand32(uint32_t min, uint32_t max, FuzzerGenerator& rng) {
    return boost::random::uniform_int_distribution<uint32_t>(min, max)(rng);
  }
};

} // namespace facebook::velox::functions::sparksql::fuzzer
