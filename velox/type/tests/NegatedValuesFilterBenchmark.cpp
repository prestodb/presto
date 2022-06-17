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

#define DEFINE_BENCHMARKS(x, y, z)                               \
  BENCHMARK(x##MultiRange##y) {                                  \
    folly::doNotOptimizeAway(filterMultiRange(z, x##Values));    \
  }                                                              \
  BENCHMARK_RELATIVE(x##NegatedValues##y) {                      \
    folly::doNotOptimizeAway(filterNegatedValues(z, x##Values)); \
  }

std::vector<int64_t> verySparseValues;
std::vector<int64_t> sparseValues;
std::vector<int64_t> mediumValues;
std::vector<int64_t> denseValues;
std::vector<int64_t> veryDenseValues;
std::vector<int64_t> extremelyDenseValues; // used for testing !=

std::vector<std::unique_ptr<Filter>> negatedValuesFilters;
std::vector<std::unique_ptr<Filter>> multiRangeFilters;

int32_t filterMultiRange(int filterNum, const std::vector<int64_t>& data) {
  int32_t count = 0;
  for (auto i = 0; i < data.size(); ++i) {
    if (multiRangeFilters[filterNum]->testInt64(data[i]))
      ++count;
  }
  return count;
}

int32_t filterNegatedValues(int filterNum, const std::vector<int64_t>& data) {
  int32_t count = 0;
  for (auto i = 0; i < data.size(); ++i) {
    if (negatedValuesFilters[filterNum]->testInt64(data[i]))
      ++count;
  }
  return count;
}

// removal percentages are expected values based on random list generation
DEFINE_BENCHMARKS(verySparse, NotEqual, 0) // remove 1 of every 1,000,000 values
DEFINE_BENCHMARKS(sparse, NotEqual, 0) // remove 1 of every 100,000 values
DEFINE_BENCHMARKS(medium, NotEqual, 0) // remove 1 of every 15,000 values
DEFINE_BENCHMARKS(dense, NotEqual, 0) // remove 1 of every 3,000 values
DEFINE_BENCHMARKS(veryDense, NotEqual, 0) // remove 1 of every 1,050 values
DEFINE_BENCHMARKS(extremelyDense, NotEqual, 0) // remove 1 of every 20 (5%)

DEFINE_BENCHMARKS(verySparse, 10, 1) // remove 1 of every 100,000 values
DEFINE_BENCHMARKS(sparse, 10, 1) // remove 1 of every 10,000 values
DEFINE_BENCHMARKS(medium, 10, 1) // remove 1 of every 1,500 values
DEFINE_BENCHMARKS(dense, 10, 1) // remove 1 of every 300 values (0.33%)
DEFINE_BENCHMARKS(veryDense, 10, 1) // remove 1 of every 105 values (1%)
DEFINE_BENCHMARKS(extremelyDense, 10, 1) // remove 1 of every 2 values (50%)

DEFINE_BENCHMARKS(verySparse, 100, 4) // remove 1 of every 10,000 values
DEFINE_BENCHMARKS(sparse, 100, 4) // remove 1 of every 1,000 values
DEFINE_BENCHMARKS(medium, 100, 4) // remove 1 of every 150 values
DEFINE_BENCHMARKS(dense, 100, 4) // remove 1 of every 30 values (3%)
DEFINE_BENCHMARKS(veryDense, 100, 4) // remove 1 of every 10.5 values (9%)

DEFINE_BENCHMARKS(verySparse, 1000, 5) // remove 1 of every 1,000 values
DEFINE_BENCHMARKS(sparse, 1000, 5) // remove 1 of every 100 values (1%)
DEFINE_BENCHMARKS(medium, 1000, 5) // remove 1 of every 15 values (7%)
DEFINE_BENCHMARKS(dense, 1000, 5) // remove 1 of every 3 values (33%)
DEFINE_BENCHMARKS(veryDense, 1000, 5) // remove 1 of every 1.05 values (95%)

DEFINE_BENCHMARKS(verySparse, 10000, 6) // remove 1 of every 100 values(1%)
DEFINE_BENCHMARKS(sparse, 10000, 6) // remove 1 of every 10 values (10%)
DEFINE_BENCHMARKS(medium, 10000, 6) // remove 1 of every 1.5 values (66%)
DEFINE_BENCHMARKS(dense, 10000, 6) // should remove all the values (100%)

// various tests to see if other factors impact the tests
BENCHMARK(greaterSpreadMultiRange) {
  // removes about 10% of the values, but they're spread apart more
  // checking to see if a greater spread in the ranges affects anything
  folly::doNotOptimizeAway(filterMultiRange(2, sparseValues));
}

BENCHMARK_RELATIVE(greaterSpreadNegatedValues) {
  folly::doNotOptimizeAway(filterNegatedValues(2, sparseValues));
}

BENCHMARK(bitmaskMultiRange) {
  // removes about 10% of the values
  folly::doNotOptimizeAway(filterMultiRange(3, veryDenseValues));
}

BENCHMARK_RELATIVE(bitmaskNegatedValues) {
  folly::doNotOptimizeAway(filterNegatedValues(3, veryDenseValues));
}

int32_t main(int32_t argc, char* argv[]) {
  constexpr int32_t kNumValues = 1000000;
  // first one represents a != filter, the rest are "NOT IN" filters
  std::vector<int32_t> kFilterSizes = {1, 10, 10, 100, 100, 1000, 10000};
  std::vector<int32_t> kFilterIntervals = {
      1000, 1000, 10000, 10, 1000, 1000, 1000};
  std::vector<std::vector<int64_t>> rejectedValues;

  folly::init(&argc, &argv);

  rejectedValues.reserve(kFilterSizes.size());
  for (auto i = 0; i < kFilterSizes.size(); ++i) {
    std::vector<int64_t> filterValues;
    filterValues.reserve(kFilterSizes[i]);

    for (auto j = 0; j < kFilterSizes[i]; ++j) {
      filterValues.emplace_back(j * kFilterIntervals[i]);
    }
    negatedValuesFilters.emplace_back(
        createNegatedBigintValues(filterValues, false));
    rejectedValues.push_back(filterValues);
  }

  for (auto i = 0; i < kFilterSizes.size(); ++i) {
    std::vector<std::unique_ptr<common::BigintRange>> subfilters;
    subfilters.reserve(kFilterSizes[i]);
    int64_t start = std::numeric_limits<int64_t>::min();
    for (auto j = 0; j < kFilterSizes[i]; ++j) {
      subfilters.emplace_back(std::make_unique<common::BigintRange>(
          start, rejectedValues[i][j] - 1, false));
      start = rejectedValues[i][j] + 1;
    }
    subfilters.emplace_back(std::make_unique<common::BigintRange>(
        start, std::numeric_limits<int64_t>::max(), false));
    multiRangeFilters.emplace_back(std::make_unique<common::BigintMultiRange>(
        std::move(subfilters), false));
  }

  veryDenseValues.resize(kNumValues);
  denseValues.resize(kNumValues);
  mediumValues.resize(kNumValues);
  sparseValues.resize(kNumValues);
  verySparseValues.resize(kNumValues);
  extremelyDenseValues.resize(kNumValues);

  for (auto i = 0; i < kNumValues; ++i) {
    extremelyDenseValues[i] = (folly::Random::rand32() % 20) * 1000;
    veryDenseValues[i] = (folly::Random::rand32() % 1050) * 1000;
    denseValues[i] = (folly::Random::rand32() % 3000) * 1000;
    mediumValues[i] = (folly::Random::rand32() % 15000) * 1000;
    sparseValues[i] = (folly::Random::rand32() % 100000) * 1000;
    verySparseValues[i] = (folly::Random::rand32() % 1000000) * 1000;
  }

  // comment out this section to speed up testing + skip verifying correctness

  VELOX_CHECK_EQ(
      filterMultiRange(0, extremelyDenseValues),
      filterNegatedValues(0, extremelyDenseValues));
  for (int i = 0; i < negatedValuesFilters.size(); ++i) {
    VELOX_CHECK_EQ(
        filterMultiRange(i, verySparseValues),
        filterNegatedValues(i, verySparseValues));
    VELOX_CHECK_EQ(
        filterMultiRange(i, sparseValues),
        filterNegatedValues(i, sparseValues));
    VELOX_CHECK_EQ(
        filterMultiRange(i, mediumValues),
        filterNegatedValues(i, mediumValues));
    VELOX_CHECK_EQ(
        filterMultiRange(i, denseValues), filterNegatedValues(i, denseValues));
    VELOX_CHECK_EQ(
        filterMultiRange(i, veryDenseValues),
        filterNegatedValues(i, veryDenseValues));
  }

  folly::runBenchmarks();
  return 0;
}
