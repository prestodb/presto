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
std::unordered_map<int32_t, int32_t> length_indices;
// maps each filter size to its corresponding index in inner filter vectors
std::unordered_map<int32_t, int32_t> filter_size_indices;

std::vector<std::vector<std::unique_ptr<Filter>>> multi_range_filters;
std::vector<std::vector<std::unique_ptr<Filter>>> negated_values_filters;

std::vector<std::vector<std::string>> test_data;

#define DEFINE_BENCHMARKS(x, y)                               \
  BENCHMARK(MultiRangeLength##x##FilterSize##y) {             \
    folly::doNotOptimizeAway(filterMultiRange(x, y));         \
  }                                                           \
  BENCHMARK_RELATIVE(NegatedValuesLength##x##FilterSize##y) { \
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
  for (std::string s : test_data[length_indices[x]]) {
    if (multi_range_filters[length_indices[x]][filter_size_indices[y]]
            ->testBytes(s.data(), s.length())) {
      ++count;
    }
  }
  return count;
}

int64_t filterNegatedValues(int32_t x, int32_t y) {
  int count = 0;
  for (std::string s : test_data[length_indices[x]]) {
    if (negated_values_filters[length_indices[x]][filter_size_indices[y]]
            ->testBytes(s.data(), s.length())) {
      ++count;
    }
  }
  return count;
}

DEFINE_BENCHMARKS(1, 1)
DEFINE_BENCHMARKS(1, 5)
DEFINE_BENCHMARKS(1, 10)
DEFINE_BENCHMARKS(1, 100)

DEFINE_BENCHMARKS(2, 1)
DEFINE_BENCHMARKS(2, 5)
DEFINE_BENCHMARKS(2, 10)
DEFINE_BENCHMARKS(2, 100)
DEFINE_BENCHMARKS(2, 1000)

DEFINE_BENCHMARKS(5, 1)
DEFINE_BENCHMARKS(5, 5)
DEFINE_BENCHMARKS(5, 10)
DEFINE_BENCHMARKS(5, 100)
DEFINE_BENCHMARKS(5, 1000)
DEFINE_BENCHMARKS(5, 10000)

DEFINE_BENCHMARKS(10, 1)
DEFINE_BENCHMARKS(10, 5)
DEFINE_BENCHMARKS(10, 10)
DEFINE_BENCHMARKS(10, 100)
DEFINE_BENCHMARKS(10, 1000)
DEFINE_BENCHMARKS(10, 10000)

DEFINE_BENCHMARKS(100, 1)
DEFINE_BENCHMARKS(100, 5)
DEFINE_BENCHMARKS(100, 10)
DEFINE_BENCHMARKS(100, 100)
DEFINE_BENCHMARKS(100, 1000)
DEFINE_BENCHMARKS(100, 10000)

int32_t main(int32_t argc, char* argv[]) {
  constexpr int32_t k_num_values = 1000000;
  constexpr int32_t k_string_pool_size = 20000;
  const std::vector<int32_t> string_lengths = {1, 2, 5, 10, 100};
  const std::vector<int32_t> filter_sizes = {1, 5, 10, 100, 1000, 10000};

  // convert these vectors for lookup later
  for (int i = 0; i < filter_sizes.size(); ++i) {
    filter_size_indices.insert({filter_sizes[i], i});
  }
  for (int i = 0; i < string_lengths.size(); ++i) {
    length_indices.insert({string_lengths[i], i});
  }

  for (int32_t len : string_lengths) {
    std::vector<std::string> string_pool;
    string_pool.reserve(k_string_pool_size);
    for (int j = 0; j < k_string_pool_size; ++j) {
      string_pool.push_back(gen_string(len));
    }
    LOG(INFO) << "Generated string pool with length " << len;
    // generate filters
    std::vector<std::unique_ptr<Filter>> negateds;
    std::vector<std::unique_ptr<Filter>> multi_ranges;
    for (int32_t size : filter_sizes) {
      // avoid duplication
      std::set<std::string> rejects;
      for (int j = 0; j < size; ++j) {
        rejects.insert(string_pool[j]);
      }

      // copy to a vector
      std::vector<std::string> reject_vector;
      reject_vector.reserve(size);
      for (auto it = rejects.begin(); it != rejects.end(); ++it) {
        reject_vector.emplace_back(*it);
      }

      // create NegatedBytesValues filter
      negateds.emplace_back(
          std::make_unique<common::NegatedBytesValues>(reject_vector, false));

      // create MultiRange filter
      std::vector<std::unique_ptr<common::Filter>> range_filters;
      auto front = ++(reject_vector.begin());
      auto back = reject_vector.begin();
      range_filters.emplace_back(std::make_unique<common::BytesRange>(
          "", true, true, *back, false, true, false));
      while (front != reject_vector.end()) {
        range_filters.emplace_back(std::make_unique<common::BytesRange>(
            *back, false, true, *front, false, true, false));
        ++front;
        ++back;
      }
      range_filters.emplace_back(std::make_unique<common::BytesRange>(
          *back, false, true, "", true, true, false));
      multi_ranges.emplace_back(std::make_unique<common::MultiRange>(
          std::move(range_filters), false, false));

      LOG(INFO) << "Generated filter for length " << len << " with size "
                << size;
    }

    multi_range_filters.emplace_back(std::move(multi_ranges));
    negated_values_filters.emplace_back(std::move(negateds));

    // generate corresponding data vectors to filter from string pool
    std::vector<std::string> data;
    data.reserve(k_num_values);
    for (int i = 0; i < k_num_values; ++i) {
      data.emplace_back(
          string_pool[folly::Random::rand32() % k_string_pool_size]);
    }
    test_data.emplace_back(data);
  }

  LOG(INFO) << "Validating...";
  // correctness check (comment out to speed up benchmarking)
  for (auto length : string_lengths) {
    for (auto size : filter_sizes) {
      VELOX_CHECK_EQ(
          filterMultiRange(length, size), filterNegatedValues(length, size));
    }
    LOG(INFO) << "Validated for length " << length;
  }

  folly::runBenchmarks();
  return 0;
}
