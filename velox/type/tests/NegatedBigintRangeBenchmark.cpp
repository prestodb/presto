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

#include <limits>
#include <set>

#include "folly/Benchmark.h"
#include "folly/Portability.h"
#include "folly/Random.h"
#include "folly/Varint.h"
#include "folly/init/Init.h"
#include "folly/lang/Bits.h"

#include "velox/dwio/common/exception/Exception.h"
#include "velox/type/Filter.h"

using namespace facebook::velox;
using namespace facebook::velox::common;

std::vector<std::vector<int64_t>> neqData;
std::vector<int64_t> randomData;
// maps each percentage of 0s to its corresponding neqData vector
std::unordered_map<int32_t, int32_t> pctIndices;
// maps each filter size to its corresponding index in filter vector
std::unordered_map<int32_t, int32_t> filterSizeIndices;

std::unique_ptr<Filter> negatedRangeNeq;
std::unique_ptr<Filter> multiRangeNeq;
std::vector<std::unique_ptr<Filter>> negatedRanges;
std::vector<std::unique_ptr<Filter>> multiRanges;

#define DEFINE_NEQ_BENCHMARKS(x)                        \
  BENCHMARK(MultiRange##x##PercentRemoved) {            \
    folly::doNotOptimizeAway(neqMultiRange(x));         \
  }                                                     \
  BENCHMARK_RELATIVE(NegatedRange##x##PercentRemoved) { \
    folly::doNotOptimizeAway(neqNegatedRange(x));       \
  }

#define DEFINE_RANGE_BENCHMARKS(x)                   \
  BENCHMARK(MultiRangeSize##x) {                     \
    folly::doNotOptimizeAway(multiRangeFilter(x));   \
  }                                                  \
  BENCHMARK_RELATIVE(NegatedRangeSize##x) {          \
    folly::doNotOptimizeAway(negatedRangeFilter(x)); \
  }

int64_t neqMultiRange(int32_t pct) {
  int count = 0;
  for (int64_t i : neqData[pctIndices[pct]]) {
    if (multiRangeNeq->testInt64(i)) {
      ++count;
    }
  }
  return count;
}

int64_t neqNegatedRange(int32_t pct) {
  int count = 0;
  for (int64_t i : neqData[pctIndices[pct]]) {
    if (negatedRangeNeq->testInt64(i)) {
      ++count;
    }
  }
  return count;
}

int64_t multiRangeFilter(int32_t size) {
  int count = 0;
  for (int64_t i : randomData) {
    if (multiRanges[filterSizeIndices[size]]->testInt64(i)) {
      ++count;
    }
  }
  return count;
}

int64_t negatedRangeFilter(int32_t size) {
  int count = 0;
  for (int64_t i : randomData) {
    if (negatedRanges[filterSizeIndices[size]]->testInt64(i)) {
      ++count;
    }
  }
  return count;
}

DEFINE_NEQ_BENCHMARKS(0)
DEFINE_NEQ_BENCHMARKS(1)
DEFINE_NEQ_BENCHMARKS(5)
DEFINE_NEQ_BENCHMARKS(10)
DEFINE_NEQ_BENCHMARKS(50)
DEFINE_NEQ_BENCHMARKS(90)
DEFINE_NEQ_BENCHMARKS(95)
DEFINE_NEQ_BENCHMARKS(99)
DEFINE_NEQ_BENCHMARKS(100)

DEFINE_RANGE_BENCHMARKS(0)
DEFINE_RANGE_BENCHMARKS(1)
DEFINE_RANGE_BENCHMARKS(10)
DEFINE_RANGE_BENCHMARKS(100)
DEFINE_RANGE_BENCHMARKS(1000)
DEFINE_RANGE_BENCHMARKS(5000)
DEFINE_RANGE_BENCHMARKS(9000)
DEFINE_RANGE_BENCHMARKS(9900)

int32_t main(int32_t argc, char* argv[]) {
  constexpr int32_t kNumValues = 1000000;
  constexpr int64_t distinctVals = 20001; // -10000 to 10000
  const std::vector<int64_t> pctZeros = {0, 1, 5, 10, 50, 90, 95, 99, 100};
  const std::vector<int64_t> rangeBounds = {
      0, 1, 10, 100, 1000, 5000, 9000, 9900};

  negatedRangeNeq = std::make_unique<NegatedBigintRange>(0, 0, false);
  std::vector<std::unique_ptr<BigintRange>> zeroRanges;
  zeroRanges.emplace_back(std::make_unique<BigintRange>(
      std::numeric_limits<int64_t>::min(), -1, false));
  zeroRanges.emplace_back(std::make_unique<BigintRange>(
      1, std::numeric_limits<int64_t>::max(), false));
  multiRangeNeq =
      std::make_unique<BigintMultiRange>(std::move(zeroRanges), false);

  for (int i = 0; i < pctZeros.size(); ++i) {
    std::vector<int64_t> data;
    data.reserve(kNumValues);
    for (int j = 0; j < kNumValues; ++j) {
      if (folly::Random::rand32() % 100 < pctZeros[i]) {
        data.emplace_back(0);
      } else {
        data.emplace_back(
            (folly::Random::rand32() % distinctVals) - (distinctVals / 2));
      }
    }
    neqData.push_back(data);
    pctIndices[pctZeros[i]] = i;
  }

  for (int i = 0; i < rangeBounds.size(); ++i) {
    filterSizeIndices[rangeBounds[i]] = i;
    negatedRanges.emplace_back(std::make_unique<NegatedBigintRange>(
        -1 * rangeBounds[i], rangeBounds[i], false));
    std::vector<std::unique_ptr<BigintRange>> ranges;
    ranges.emplace_back(std::make_unique<BigintRange>(
        std::numeric_limits<int64_t>::min(), -1 * (rangeBounds[i] + 1), false));
    ranges.emplace_back(std::make_unique<BigintRange>(
        rangeBounds[i] + 1, std::numeric_limits<int64_t>::max(), false));
    multiRanges.emplace_back(
        std::make_unique<BigintMultiRange>(std::move(ranges), false));
  }

  randomData.reserve(kNumValues);
  for (int i = 0; i < kNumValues; ++i) {
    randomData.emplace_back(
        (folly::Random::rand32() % distinctVals) - (distinctVals / 2));
  }

  LOG(INFO) << "Validating...";
  // correctness check (comment out to speed up benchmarking)
  for (int32_t pct : pctZeros) {
    VELOX_CHECK_EQ(neqMultiRange(pct), neqNegatedRange(pct));
  }
  LOG(INFO) << "Validated correctness for != filters";

  for (int32_t size : rangeBounds) {
    VELOX_CHECK_EQ(multiRangeFilter(size), negatedRangeFilter(size));
  }
  LOG(INFO) << "Validated correctness for range filters";

  folly::runBenchmarks();
  return 0;
}
