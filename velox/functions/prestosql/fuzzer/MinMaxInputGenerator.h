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

class MinMaxInputGenerator : public InputGenerator {
 public:
  explicit MinMaxInputGenerator(const std::string& name)
      : indexOfN_{indexOfN(name)} {}

  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    // TODO Generate inputs free of nested nulls.
    if (types.size() <= indexOfN_) {
      return {};
    }

    // Make sure to use the same value of 'n' for all batches in a given Fuzzer
    // iteration.
    if (!n_.has_value()) {
      n_ = boost::random::uniform_int_distribution<int64_t>(0, 9'999)(rng);
    }

    const auto size = fuzzer.getOptions().vectorSize;

    std::vector<VectorPtr> inputs;
    inputs.reserve(types.size());
    for (auto i = 0; i < types.size() - 1; ++i) {
      inputs.push_back(fuzzer.fuzz(types[i]));
    }

    VELOX_CHECK(
        types.back()->isBigint(),
        "Unexpected type: {}",
        types.back()->toString());
    inputs.push_back(
        BaseVector::createConstant(BIGINT(), n_.value(), size, pool));
    return inputs;
  }

  void reset() override {
    n_.reset();
  }

 private:
  // Returns zero-based index of the 'n' argument, 1 for min and max. 2 for
  // min_by and max_by.
  static int32_t indexOfN(const std::string& name) {
    if (name == "min" || name == "max") {
      return 1;
    }

    if (name == "min_by" || name == "max_by") {
      return 2;
    }

    VELOX_FAIL("Unexpected function name: {}", name);
  }

  // Zero-based index of the 'n' argument.
  const int32_t indexOfN_;
  std::optional<int64_t> n_;
};

} // namespace facebook::velox::exec::test
