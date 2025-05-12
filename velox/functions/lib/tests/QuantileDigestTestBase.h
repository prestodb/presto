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

#include <folly/Range.h>
#include <folly/base64.h>
#include <gtest/gtest.h>
#include <numeric>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::functions {

class QuantileDigestTestBase : public testing::Test {
 public:
  static constexpr double kQuantiles[] = {
      0.0001, 0.0200, 0.0300, 0.04000, 0.0500, 0.1000, 0.2000,
      0.3000, 0.4000, 0.5000, 0.6000,  0.7000, 0.8000, 0.9000,
      0.9500, 0.9600, 0.9700, 0.9800,  0.9999,
  };

  template <typename QuantileDigest, bool testSum = true>
  static void checkQuantiles(
      folly::Range<const double*> values,
      QuantileDigest& digest,
      double sumError,
      double rankError) {
    VELOX_CHECK(std::is_sorted(values.begin(), values.end()));
    if constexpr (testSum) {
      auto sum = std::accumulate(values.begin(), values.end(), 0.0);
      ASSERT_NEAR(digest.sum(), sum, sumError);
    }
    for (auto q : kQuantiles) {
      auto v = digest.estimateQuantile(q);
      ASSERT_LE(values.front(), v);
      ASSERT_LE(v, values.back());
      auto hi = std::lower_bound(values.begin(), values.end(), v);
      auto lo = hi;
      while (lo != values.begin() && v > *lo) {
        --lo;
      }
      while (std::next(hi) != values.end() && *hi == *std::next(hi)) {
        ++hi;
      }
      auto l = (lo - values.begin()) / (values.size() - 1.0);
      auto r = (hi - values.begin()) / (values.size() - 1.0);
      if (q < l) {
        ASSERT_NEAR(l, q, rankError);
      } else if (q > r) {
        ASSERT_NEAR(r, q, rankError);
      }
    }
  }

  static std::string decodeBase64(std::string_view input) {
    std::string decoded(folly::base64DecodedSize(input), '\0');
    folly::base64Decode(input, decoded.data());
    return decoded;
  }
};

#define CHECK_QUANTILES(_values, _digest, _sumError, _rankError) \
  do {                                                           \
    SCOPED_TRACE("CHECK_QUANTILES");                             \
    QuantileDigestTestBase::checkQuantiles(                      \
        (_values), (_digest), (_sumError), (_rankError));        \
  } while (false)

} // namespace facebook::velox::functions
