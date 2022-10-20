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

// valid characters for our strings
const std::string char_pool =
    "1234567890-=!@#$%^&*()_+`~qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM[]{} /\\:\"'<>,.?;|";

// maps each string length to its corresponding index in data/filters vectors
std::unordered_map<int32_t, int32_t> lengthIndices;
// maps each percentage to its corresponding index in inner filter vectors
std::unordered_map<int32_t, int32_t> percentageIndices;

std::vector<std::vector<std::unique_ptr<Filter>>> multiRangeFilters;
std::vector<std::vector<std::unique_ptr<Filter>>> negatedRangeFilters;

std::vector<std::vector<std::string>> testData;

#define DEFINE_BENCHMARKS(x, y)                               \
  BENCHMARK(MultiRangeLength##x##RemovedPct##y) {             \
    folly::doNotOptimizeAway(filterMultiRange(x, y));         \
  }                                                           \
  BENCHMARK_RELATIVE(NegatedValuesLength##x##RemovedPct##y) { \
    folly::doNotOptimizeAway(filterNegatedValues(x, y));      \
  }

// creates a random string with length at most length
std::string gen_string(int32_t length) {
  std::string res = "";
  unsigned long pool_size = char_pool.length() + 1;
  for (int i = 0; i < length; ++i) {
    int32_t x = folly::Random::rand32() % pool_size;
    if (x == 0) {
      return res; // end string early with small chance
    }
    res += char_pool[x - 1];
  }
  return res;
}

int64_t filterMultiRange(int32_t x, int32_t y) {
  int count = 0;
  for (std::string s : testData[lengthIndices[x]]) {
    if (multiRangeFilters[lengthIndices[x]][percentageIndices[y]]->testBytes(
            s.data(), s.length())) {
      ++count;
    }
  }
  return count;
}

int64_t filterNegatedValues(int32_t x, int32_t y) {
  int count = 0;
  for (std::string s : testData[lengthIndices[x]]) {
    if (negatedRangeFilters[lengthIndices[x]][percentageIndices[y]]->testBytes(
            s.data(), s.length())) {
      ++count;
    }
  }
  return count;
}

DEFINE_BENCHMARKS(2, 2)
DEFINE_BENCHMARKS(2, 6)
DEFINE_BENCHMARKS(2, 10)
DEFINE_BENCHMARKS(2, 50)
DEFINE_BENCHMARKS(2, 90)
DEFINE_BENCHMARKS(2, 94)
DEFINE_BENCHMARKS(2, 98)

DEFINE_BENCHMARKS(3, 2)
DEFINE_BENCHMARKS(3, 6)
DEFINE_BENCHMARKS(3, 10)
DEFINE_BENCHMARKS(3, 50)
DEFINE_BENCHMARKS(3, 90)
DEFINE_BENCHMARKS(3, 94)
DEFINE_BENCHMARKS(3, 98)

DEFINE_BENCHMARKS(5, 2)
DEFINE_BENCHMARKS(5, 6)
DEFINE_BENCHMARKS(5, 10)
DEFINE_BENCHMARKS(5, 50)
DEFINE_BENCHMARKS(5, 90)
DEFINE_BENCHMARKS(5, 94)
DEFINE_BENCHMARKS(5, 98)

DEFINE_BENCHMARKS(10, 2)
DEFINE_BENCHMARKS(10, 6)
DEFINE_BENCHMARKS(10, 10)
DEFINE_BENCHMARKS(10, 50)
DEFINE_BENCHMARKS(10, 90)
DEFINE_BENCHMARKS(10, 94)
DEFINE_BENCHMARKS(10, 98)

DEFINE_BENCHMARKS(100, 2)
DEFINE_BENCHMARKS(100, 6)
DEFINE_BENCHMARKS(100, 10)
DEFINE_BENCHMARKS(100, 50)
DEFINE_BENCHMARKS(100, 90)
DEFINE_BENCHMARKS(100, 94)
DEFINE_BENCHMARKS(100, 98)

int32_t main(int32_t argc, char** argv) {
  folly::init(&argc, &argv);
  constexpr int32_t kNumValues = 1000000;
  constexpr int32_t kStringPoolSize = 20000;
  const std::vector<int32_t> stringLengths = {2, 3, 5, 10, 100};
  const std::vector<int32_t> percentages = {2, 6, 10, 50, 90, 94, 98};

  // convert these vectors for lookup later
  for (int i = 0; i < percentages.size(); ++i) {
    percentageIndices.insert({percentages[i], i});
  }
  for (int i = 0; i < stringLengths.size(); ++i) {
    lengthIndices.insert({stringLengths[i], i});
  }

  for (int32_t len : stringLengths) {
    std::vector<std::string> string_pool;
    string_pool.reserve(kStringPoolSize);
    for (int j = 0; j < kStringPoolSize; ++j) {
      string_pool.push_back(gen_string(len));
    }
    std::sort(string_pool.begin(), string_pool.end());
    LOG(INFO) << "Generated string pool for length " << len;

    // generate filters
    std::vector<std::unique_ptr<Filter>> negateds;
    std::vector<std::unique_ptr<Filter>> multiRanges;
    for (int32_t pct : percentages) {
      std::string lo = string_pool[kStringPoolSize * (50 - pct / 2) / 100];
      std::string hi = string_pool[kStringPoolSize * (50 + pct / 2) / 100];

      // create NegatedBytesRange filter
      negateds.emplace_back(std::make_unique<common::NegatedBytesRange>(
          lo, false, false, hi, false, true, false));

      // create MultiRange filter
      std::vector<std::unique_ptr<common::Filter>> rangeFilters;
      rangeFilters.emplace_back(std::make_unique<common::BytesRange>(
          "", true, false, lo, false, true, false));
      rangeFilters.emplace_back(std::make_unique<common::BytesRange>(
          hi, false, false, "", true, false, false));
      multiRanges.emplace_back(std::make_unique<common::MultiRange>(
          std::move(rangeFilters), false, false));

      LOG(INFO) << "Generated filter for length " << len << " with percentage "
                << pct;
    }

    multiRangeFilters.emplace_back(std::move(multiRanges));
    negatedRangeFilters.emplace_back(std::move(negateds));

    // generate corresponding data vectors to filter from string pool
    std::vector<std::string> data;
    data.reserve(kNumValues);
    for (int i = 0; i < kNumValues; ++i) {
      data.emplace_back(string_pool[folly::Random::rand32() % kStringPoolSize]);
    }
    testData.emplace_back(data);
  }

  LOG(INFO) << "Validating...";
  // correctness check (comment out to speed up benchmarking)
  for (auto length : stringLengths) {
    for (auto size : percentages) {
      VELOX_CHECK_EQ(
          filterMultiRange(length, size), filterNegatedValues(length, size));
    }
    LOG(INFO) << "Validated for length " << length;
  }

  folly::runBenchmarks();
  return 0;
}
