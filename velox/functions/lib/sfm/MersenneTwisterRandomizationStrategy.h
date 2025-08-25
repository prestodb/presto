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

#include <optional>
#include <random>
#include "velox/functions/lib/sfm/RandomizationStrategy.h"

namespace facebook::velox::functions::sfm {

/// A pseudorandomness strategy used to merge SfmSketches.
class MersenneTwisterRandomizationStrategy : public RandomizationStrategy {
 public:
  explicit MersenneTwisterRandomizationStrategy(
      std::optional<int32_t> seed = {}) {
    if (seed.has_value()) {
      rng_.seed(seed.value());
    }
  }

  bool nextBoolean(double probability) override {
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng_) < probability;
  }

 private:
  std::mt19937_64 rng_;
};
} // namespace facebook::velox::functions::sfm
