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

#include "boost/math/distributions/beta.hpp"

namespace facebook::velox::functions {

namespace {

template <typename T>
struct BetaCDFFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(double& result, double a, double b, double value) {
    constexpr double kInf = std::numeric_limits<double>::infinity();

    VELOX_USER_CHECK_GT(a, 0, "a must be > 0");
    VELOX_USER_CHECK_GT(b, 0, "b must be > 0");
    VELOX_USER_CHECK_GE(value, 0, "value must be in the interval [0, 1]");
    VELOX_USER_CHECK_LE(value, 1, "value must be in the interval [0, 1]");

    if ((a == kInf) || (b == kInf)) {
      result = 0.0;
    } else {
      boost::math::beta_distribution<> dist(a, b);
      result = boost::math::cdf(dist, value);
    }
  }
};

} // namespace
} // namespace facebook::velox::functions
