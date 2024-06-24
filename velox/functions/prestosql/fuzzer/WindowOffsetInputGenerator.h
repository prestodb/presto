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
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::exec::test {

// Generates an integer 1, 2, 3, or 9'000'000'000 as the argument at
// argumentIndex for functions.
class WindowOffsetInputGenerator : public InputGenerator {
 public:
  explicit WindowOffsetInputGenerator(int64_t argumentIndex)
      : argumentIndex_{argumentIndex} {}

  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& /*rng*/,
      memory::MemoryPool* pool) override {
    if (types.size() <= argumentIndex_) {
      return {};
    }

    const auto size = fuzzer.getOptions().vectorSize;

    std::vector<VectorPtr> inputs;
    inputs.reserve(argumentIndex_ + 1);
    for (auto i = 0; i < argumentIndex_; ++i) {
      inputs.push_back(fuzzer.fuzz(types[i]));
    }

    facebook::velox::test::VectorMaker vectorMaker{pool};
    auto baseN = vectorMaker.flatVector<int64_t>({1, 2, 3, 9'000'000'000});
    inputs.push_back(fuzzer.fuzzDictionary(baseN, size));

    return inputs;
  }

  void reset() override {}

 private:
  const int64_t argumentIndex_;
};

} // namespace facebook::velox::exec::test
