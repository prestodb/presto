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
constexpr size_t kMaxSizeOfInitBits = 3000;

class FbDatelistStringMergeAggregationInputGenerator : public InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    std::vector<VectorPtr> result;
    result.reserve(types.size());

    VELOX_CHECK(
        types.size() == 1,
        fmt::format("Unexpected types count:{}", types.size()));

    VELOX_CHECK(
        types[0]->isVarchar(), "Unexpected type: {}", types[0]->toString());

    const auto vecSize = fuzzer.getOptions().vectorSize;
    velox::test::VectorMaker vectorMaker(pool);

    std::vector<std::string> testInputStrs;
    testInputStrs.reserve(vecSize);

    for (auto i = 0; i < vecSize; ++i) {
      testInputStrs.push_back(
          fuzzer::coinToss(rng, 0.8) ? generateRandomBinaryString(rng)
                                     : generateRandomString(rng));
    }

    auto testStrings = vectorMaker.flatVector(testInputStrs);
    result.emplace_back(std::move(testStrings));
    return result;
  }

  void reset() override {
    // Nothing to reset
  }

 private:
  std::string generateRandomBinaryString(FuzzerGenerator& rng) {
    const auto length = boost::random::uniform_int_distribution<size_t>(
        0, kMaxSizeOfInitBits + 1)(rng);

    std::string binaryStr;
    binaryStr.reserve(length);

    std::bernoulli_distribution dist(0.5);

    for (size_t i = 0; i < length; ++i) {
      binaryStr.push_back(dist(rng) ? '1' : '0');
    }

    return binaryStr;
  }

  std::string generateRandomString(FuzzerGenerator& rng) {
    static const std::string charSet =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-*/";

    const auto length = boost::random::uniform_int_distribution<size_t>(
        0, kMaxSizeOfInitBits + 1)(rng);

    std::string randStr;
    randStr.reserve(length);

    boost::random::uniform_int_distribution<size_t> dist(0, charSet.size() - 1);
    for (size_t i = 0; i < length; ++i) {
      randStr.push_back(charSet.at(dist(rng)));
    }

    return randStr;
  }
};

} // namespace facebook::velox::exec::test
