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

class ClassificationAggregationInputGenerator : public InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    std::vector<VectorPtr> result;
    result.reserve(types.size());

    VELOX_CHECK(
        types.size() == 3 || types.size() == 4,
        fmt::format("Unexpected types count:{}", types.size()));

    VELOX_CHECK(
        types[0]->isBigint(), "Unexpected type: {}", types[0]->toString());
    VELOX_CHECK(
        types[2]->isDouble(), "Unexpected type: {}", types[2]->toString());

    /// The bucket must be the same everytime or else classification
    /// aggregation function considers this an invalid input. Moreover, the
    /// buckets are capped to 50'000 to prevent OOM-ing issues. The minimum is
    /// 2 since that is the minimum valid bucket count.
    if (!bucket_.has_value()) {
      bucket_ = boost::random::uniform_int_distribution<int64_t>(2, 500)(rng);
    }
    const auto size = fuzzer.getOptions().vectorSize;
    velox::test::VectorMaker vectorMaker{pool};
    auto bucket = vectorMaker.flatVector<int64_t>(
        size, [&](auto /*row*/) { return bucket_.value(); });

    auto pred = vectorMaker.flatVector<double>(size, [&](auto /*row*/) {
      /// Predictions must be > 0.
      return std::uniform_real_distribution<double>(
          0, std::numeric_limits<double>::max())(rng);
    });

    result.emplace_back(std::move(bucket));
    result.emplace_back(fuzzer.fuzz(types[1]));
    result.emplace_back(std::move(pred));
    if (types.size() == 4) {
      VELOX_CHECK(
          types[3]->isDouble(), "Unexpected type: {}", types[3]->toString());

      auto weight = vectorMaker.flatVector<double>(size, [&](auto /*row*/) {
        /// Weights must be > 0.
        return std::uniform_real_distribution<double>(
            0, std::numeric_limits<double>::max())(rng);
      });
      result.emplace_back(std::move(weight));
    }
    return result;
  }

  void reset() override {
    bucket_.reset();
  }

 private:
  std::optional<int64_t> bucket_;
};

} // namespace facebook::velox::exec::test
