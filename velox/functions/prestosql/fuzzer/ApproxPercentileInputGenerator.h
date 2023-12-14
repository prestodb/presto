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

class ApproxPercentileInputGenerator : public InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    // The arguments are: x, [w], percentile(s), [accuracy].
    //
    // First argument is always 'x'. If second argument's type is BIGINT, then
    // it is 'w'. Otherwise, it is percentile(x).

    const auto size = fuzzer.getOptions().vectorSize;

    std::vector<VectorPtr> inputs;
    inputs.reserve(types.size());
    inputs.push_back(fuzzer.fuzz(types[0]));

    if (types[1]->isBigint()) {
      velox::test::VectorMaker vectorMaker{pool};
      auto weight = vectorMaker.flatVector<int64_t>(size, [&](auto /*row*/) {
        return boost::random::uniform_int_distribution<int64_t>(1, 1'000)(rng);
      });

      inputs.push_back(weight);
    }

    const int percentileTypeIndex = types[1]->isBigint() ? 2 : 1;
    const TypePtr& percentileType = types[percentileTypeIndex];
    if (percentileType->isDouble()) {
      if (!percentile_.has_value()) {
        percentile_ = pickPercentile(fuzzer, rng);
      }

      inputs.push_back(BaseVector::createConstant(
          DOUBLE(), percentile_.value(), size, pool));
    } else {
      VELOX_CHECK(percentileType->isArray());
      VELOX_CHECK(percentileType->childAt(0)->isDouble());

      if (percentiles_.empty()) {
        percentiles_.push_back(pickPercentile(fuzzer, rng));
        percentiles_.push_back(pickPercentile(fuzzer, rng));
        percentiles_.push_back(pickPercentile(fuzzer, rng));
      }

      auto arrayVector =
          BaseVector::create<ArrayVector>(ARRAY(DOUBLE()), 1, pool);
      auto elementsVector = arrayVector->elements()->asFlatVector<double>();
      elementsVector->resize(percentiles_.size());
      for (auto i = 0; i < percentiles_.size(); ++i) {
        elementsVector->set(i, percentiles_[i]);
      }
      arrayVector->setOffsetAndSize(0, 0, percentiles_.size());

      inputs.push_back(BaseVector::wrapInConstant(size, 0, arrayVector));
    }

    if (types.size() > percentileTypeIndex + 1) {
      // Last argument is 'accuracy'.
      VELOX_CHECK(types.back()->isDouble());
      if (!accuracy_.has_value()) {
        accuracy_ = boost::random::uniform_01<double>()(rng);
      }

      inputs.push_back(
          BaseVector::createConstant(DOUBLE(), accuracy_.value(), size, pool));
    }

    return inputs;
  }

  void reset() override {
    percentile_.reset();
    percentiles_.clear();
    accuracy_.reset();
  }

 private:
  double pickPercentile(VectorFuzzer& fuzzer, FuzzerGenerator& rng) {
    // 10% of the times generate random value in [0, 1] range.
    // 90% of the times use one of the common values.
    if (fuzzer.coinToss(0.1)) {
      return boost::random::uniform_01<double>()(rng);
    }

    static const std::vector<double> kPercentiles = {
        0.1, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999, 0.9999};

    const auto index =
        boost::random::uniform_int_distribution<uint32_t>()(rng) %
        kPercentiles.size();

    return kPercentiles[index];
  }

  std::optional<double> percentile_;
  std::vector<double> percentiles_;
  std::optional<double> accuracy_;
};

} // namespace facebook::velox::exec::test
