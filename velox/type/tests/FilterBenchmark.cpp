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

std::vector<int64_t> sparseValues;
std::vector<int64_t> denseValues;
std::unique_ptr<BigintValuesUsingHashTable> filter;

int32_t run1x64(const std::vector<int64_t>& data) {
  int32_t count = 0;
  for (auto i = 0; i < data.size(); ++i) {
    count += filter->testInt64(data[i]);
  }
  return count;
}

int32_t run4x64(const std::vector<int64_t>& data) {
  constexpr int kStep = xsimd::batch<int64_t>::size;
  int32_t count = 0;
  assert(data.size() % kStep == 0);
  for (auto i = 0; i < data.size(); i += kStep) {
    auto result = filter->testValues(xsimd::load_unaligned(data.data() + i));
    count += __builtin_popcount(simd::toBitMask(result));
  }
  return count;
}

BENCHMARK(scalarDense) {
  folly::doNotOptimizeAway(run1x64(denseValues));
}

BENCHMARK_RELATIVE(simdDense) {
  folly::doNotOptimizeAway(run4x64(denseValues));
}

BENCHMARK(scalarSparse) {
  folly::doNotOptimizeAway(run1x64(sparseValues));
}

BENCHMARK_RELATIVE(simdSparse) {
  folly::doNotOptimizeAway(run4x64(sparseValues));
}

int32_t main(int32_t argc, char* argv[]) {
  constexpr int32_t kNumValues = 1000000;
  constexpr int32_t kFilterValues = 1000;
  folly::init(&argc, &argv);

  std::vector<int64_t> filterValues;
  filterValues.reserve(kFilterValues);
  for (auto i = 0; i < kFilterValues; ++i) {
    filterValues.push_back(i * 1000);
  }
  filter = std::make_unique<BigintValuesUsingHashTable>(
      filterValues.front(), filterValues.back(), filterValues, false);
  denseValues.resize(kNumValues);
  sparseValues.resize(kNumValues);
  for (auto i = 0; i < kNumValues; ++i) {
    denseValues[i] = (folly::Random::rand32() % 3000) * 1000;
    sparseValues[i] = (folly::Random::rand32() % 100000) * 1000;
  }

  VELOX_CHECK_EQ(run1x64(denseValues), run4x64(denseValues));
  VELOX_CHECK_EQ(run1x64(sparseValues), run4x64(sparseValues));
  folly::runBenchmarks();
  return 0;
}
