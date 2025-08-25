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

#include <folly/Random.h>
#include "velox/functions/lib/sfm/RandomizationStrategy.h"

namespace facebook::velox::functions::sfm {

/// A secure randomization strategy used to enable differential privacy for
/// SfmSketch.
class SecureRandomizationStrategy : public RandomizationStrategy {
 public:
  bool nextBoolean(double probability) override {
    // folly random generate random number in [0, 1)
    return folly::Random::secureRandDouble01() < probability;
  }
};

} // namespace facebook::velox::functions::sfm
