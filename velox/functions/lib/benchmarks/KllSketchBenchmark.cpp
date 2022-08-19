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

#include <random>

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/portability/GFlags.h>
#include <folly/stats/TDigest.h>

#include "velox/functions/lib/KllSketch.h"

namespace facebook::velox::functions::kll::test {
namespace {

template <typename T, typename std::enable_if_t<std::is_integral_v<T>, int> = 0>
auto distribution(unsigned len) {
  return std::uniform_int_distribution<T>(0, len);
}

template <
    typename T,
    typename std::enable_if_t<std::is_floating_point<T>::value, int> = 0>
auto distribution(unsigned) {
  return std::uniform_real_distribution<T>(0, 1);
}

template <typename T>
void populateValues(int len, std::vector<T>& out) {
  folly::Random::DefaultGenerator gen(folly::Random::rand32());
  auto dist = distribution<T>(len);
  out.resize(len);
  for (int i = 0; i < len; ++i) {
    out[i] = dist(gen);
  }
}

template <typename T>
int insertTDigest(int iters) {
  constexpr int kBufSize = 4096;
  std::vector<T> values;
  std::vector<double> buf;
  BENCHMARK_SUSPEND {
    populateValues(iters, values);
    buf.reserve(kBufSize);
  }
  folly::TDigest digest;
  for (int i = 0; i < iters;) {
    int size = std::min(iters - i, kBufSize);
    for (int j = 0; j < size; ++j) {
      buf.push_back(values[i + j]);
    }
    digest = digest.merge(buf);
    buf.clear();
    i += size;
  }
  return iters;
}

template <typename T>
int insertKllSketch(int iters) {
  std::vector<T> values;
  BENCHMARK_SUSPEND {
    populateValues(iters, values);
  }
  KllSketch<T> kll;
  for (int i = 0; i < iters; ++i) {
    kll.insert(values[i]);
  }
  return iters;
}

void mergeTDigest(int iters, int maxSize, int count) {
  std::vector<folly::TDigest> digests;
  BENCHMARK_SUSPEND {
    std::vector<double> values;
    for (int i = 0; i < count; ++i) {
      populateValues(maxSize, values);
      folly::TDigest digest;
      digests.push_back(digest.merge(values));
      values.clear();
    }
  }
  for (int i = 0; i < iters; ++i) {
    folly::TDigest::merge(digests);
  }
}

void mergeKllSketch(int iters, int maxSize, int count) {
  std::vector<KllSketch<double>> sketches;
  BENCHMARK_SUSPEND {
    std::vector<double> values;
    for (int i = 0; i < count; ++i) {
      populateValues(maxSize, values);
      KllSketch<double> kll;
      for (auto v : values) {
        kll.insert(v);
      }
      sketches.push_back(std::move(kll));
      values.clear();
    }
  }
  assert(sketches.size() >= 2); // get rid of the lint warning
  for (int i = 0; i < iters; ++i) {
    sketches[0].merge(folly::Range(&sketches[1], count - 1));
  }
}

#define DEFINE_WITH_TYPE(name, type)  \
  int name##_##type(int, int iters) { \
    return name<type>(iters);         \
  }

DEFINE_WITH_TYPE(insertTDigest, int64_t);
DEFINE_WITH_TYPE(insertTDigest, double);
DEFINE_WITH_TYPE(insertKllSketch, int64_t);
DEFINE_WITH_TYPE(insertKllSketch, double);

#undef DEFINE_WITH_TYPE

BENCHMARK_PARAM_MULTI(insertTDigest_int64_t, 1e5);
BENCHMARK_RELATIVE_PARAM_MULTI(insertKllSketch_int64_t, 1e5);
BENCHMARK_PARAM_MULTI(insertTDigest_double, 1e5);
BENCHMARK_RELATIVE_PARAM_MULTI(insertKllSketch_double, 1e5);
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM_MULTI(insertTDigest_int64_t, 1e6);
BENCHMARK_RELATIVE_PARAM_MULTI(insertKllSketch_int64_t, 1e6);
BENCHMARK_PARAM_MULTI(insertTDigest_double, 1e6);
BENCHMARK_RELATIVE_PARAM_MULTI(insertKllSketch_double, 1e6);
BENCHMARK_DRAW_LINE();
BENCHMARK_PARAM_MULTI(insertTDigest_int64_t, 1e7);
BENCHMARK_RELATIVE_PARAM_MULTI(insertKllSketch_int64_t, 1e7);
BENCHMARK_PARAM_MULTI(insertTDigest_double, 1e7);
BENCHMARK_RELATIVE_PARAM_MULTI(insertKllSketch_double, 1e7);
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(mergeTDigest, 1e6x2, 1e6, 2);
BENCHMARK_RELATIVE_NAMED_PARAM(mergeKllSketch, 1e6x2, 1e6, 2);
BENCHMARK_NAMED_PARAM(mergeTDigest, 1e6x20, 1e6, 20);
BENCHMARK_RELATIVE_NAMED_PARAM(mergeKllSketch, 1e6x20, 1e6, 20);
BENCHMARK_NAMED_PARAM(mergeTDigest, 1e6x40, 1e6, 40);
BENCHMARK_RELATIVE_NAMED_PARAM(mergeKllSketch, 1e6x40, 1e6, 40);
BENCHMARK_NAMED_PARAM(mergeTDigest, 1e6x80, 1e6, 80);
BENCHMARK_RELATIVE_NAMED_PARAM(mergeKllSketch, 1e6x80, 1e6, 80);

// ============================================================================
// [...]chmarks/ApproxPercentileBenchmark.cpp     relative  time/iter   iters/s
// ============================================================================
// insertTDigest_int64_t(1e5)                                 43.99ns    22.73M
// insertKllSketch_int64_t(1e5)                    110.48%    39.82ns    25.11M
// insertTDigest_double(1e5)                                  44.40ns    22.52M
// insertKllSketch_double(1e5)                     98.266%    45.19ns    22.13M
// ----------------------------------------------------------------------------
// insertTDigest_int64_t(1e6)                                 45.30ns    22.07M
// insertKllSketch_int64_t(1e6)                    119.63%    37.87ns    26.41M
// insertTDigest_double(1e6)                                  46.94ns    21.30M
// insertKllSketch_double(1e6)                      109.3%    42.95ns    23.28M
// ----------------------------------------------------------------------------
// insertTDigest_int64_t(1e7)                                 48.24ns    20.73M
// insertKllSketch_int64_t(1e7)                    132.98%    36.28ns    27.57M
// insertTDigest_double(1e7)                                  51.89ns    19.27M
// insertKllSketch_double(1e7)                     123.69%    41.95ns    23.84M
// ----------------------------------------------------------------------------
// mergeTDigest(1e6x2)                                         3.56us   280.64K
// mergeKllSketch(1e6x2)                           13.483%    26.43us    37.84K
// mergeTDigest(1e6x20)                                      184.50us     5.42K
// mergeKllSketch(1e6x20)                          23.704%   778.35us     1.28K
// mergeTDigest(1e6x40)                                      248.06us     4.03K
// mergeKllSketch(1e6x40)                          12.369%     2.01ms    498.62
// mergeTDigest(1e6x80)                                      739.31us     1.35K
// mergeKllSketch(1e6x80)                          15.133%     4.89ms    204.69

} // namespace
} // namespace facebook::velox::functions::kll::test

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
