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

#include "velox/common/base/Exceptions.h"

#include <folly/Random.h>

#include <cstdint>
#include <optional>
#include <random>

namespace facebook::velox::random {

// Set a custom seed to be returned in all following getSeed() calls.
// Should be only used in unit tests.
void setSeed(uint32_t);

// Return a true random seed unless setSeed() is called before.
uint32_t getSeed();

/// Utility class to accelerate random sampling based on Bernoulli trials.
/// Internally this keeps the number of skips for next hit.  User can consume a
/// bulk of trials calling the `nextSkip' then `consume', or call `testOne` to
/// do the trials one by one.
class RandomSkipTracker {
 public:
  explicit RandomSkipTracker(double sampleRate);

  RandomSkipTracker(const RandomSkipTracker&) = delete;
  RandomSkipTracker& operator=(const RandomSkipTracker&) = delete;

  /// Return the number of skips need to get a hit.  Must be called before
  /// calling `consume'.
  uint64_t nextSkip() {
    if (sampleRate_ == 0) {
      return std::numeric_limits<uint64_t>::max();
    }
    if (skip_.has_value()) {
      return *skip_;
    }
    skip_ = dist_(rng_);
    return *skip_;
  }

  /// Consume the remaining skips followed by at most one hit.
  void consume(uint64_t numElements) {
    if (sampleRate_ == 0) {
      return;
    }
    VELOX_DCHECK(skip_.has_value());
    if (*skip_ >= numElements) {
      *skip_ -= numElements;
    } else {
      VELOX_DCHECK_EQ(numElements - *skip_, 1);
      skip_.reset();
    }
  }

  /// Consume one trial and return the result.
  bool testOne() {
    if (sampleRate_ == 0) {
      return false;
    }
    if (nextSkip() == 0) {
      skip_.reset();
      return true;
    }
    --*skip_;
    return false;
  }

  double sampleRate() const {
    return sampleRate_;
  }

 private:
  const double sampleRate_;
  std::geometric_distribution<uint64_t> dist_;
  folly::Random::DefaultGenerator rng_;
  std::optional<uint64_t> skip_;
};

} // namespace facebook::velox::random
