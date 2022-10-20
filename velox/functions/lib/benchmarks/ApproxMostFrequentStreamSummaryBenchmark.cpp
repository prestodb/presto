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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <folly/Random.h>

#include "velox/functions/lib/ApproxMostFrequentStreamSummary.h"
#include "velox/functions/lib/ZetaDistribution.h"

namespace facebook::velox::functions {
namespace {

ZetaDistribution zetaDist() {
  return ZetaDistribution(1.0001, 100'000);
}

std::uniform_int_distribution<> uniformDist() {
  return std::uniform_int_distribution<>(1, 100'000);
}

template <typename T, typename D>
void populateValues(int len, D dist, std::vector<T>& out) {
  folly::Random::DefaultGenerator gen(folly::Random::rand32());
  out.resize(len);
  for (int i = 0; i < len; ++i) {
    out[i] = dist(gen);
  }
}

template <typename F>
int64_t insert(int iters, int size, int capacity, F genDist) {
  auto total = 1ll * iters * size;
  std::vector<int32_t> values;
  BENCHMARK_SUSPEND {
    populateValues(total, genDist(), values);
  }
  ApproxMostFrequentStreamSummary<int32_t> summary;
  summary.setCapacity(capacity);
  for (int i = 0; i < iters; ++i) {
    for (int j = 0; j < size; ++j) {
      summary.insert(values[i * size + j]);
    }
  }
  return total;
}

void merge(int iters, int size, int capacity) {
  std::vector<ApproxMostFrequentStreamSummary<int32_t>> summaries;
  std::vector<std::vector<char>> serialized;
  BENCHMARK_SUSPEND {
    std::vector<int32_t> values;
    for (int i = 0; i < iters; ++i) {
      populateValues(size, uniformDist(), values);
      ApproxMostFrequentStreamSummary<int32_t> summary;
      summary.setCapacity(capacity);
      for (auto v : values) {
        summary.insert(v);
      }
      values.clear();
      serialized.emplace_back(summary.serializedByteSize());
      summaries.push_back(std::move(summary));
    }
  }
  for (int i = 0; i < summaries.size(); ++i) {
    summaries[i].serialize(serialized[i].data());
  }
  ApproxMostFrequentStreamSummary<int32_t> summary;
  summary.setCapacity(capacity);
  for (auto& bytes : serialized) {
    summary.mergeSerialized(bytes.data());
  }
}

BENCHMARK_NAMED_PARAM_MULTI(insert, skewed_capacity256, 1e6, 256, zetaDist)
BENCHMARK_NAMED_PARAM_MULTI(insert, skewed_capacity512, 1e6, 512, zetaDist)
BENCHMARK_NAMED_PARAM_MULTI(insert, skewed_capacity1024, 1e6, 1024, zetaDist)
BENCHMARK_NAMED_PARAM_MULTI(insert, skewed_capacity2048, 1e6, 2048, zetaDist)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM_MULTI(insert, uniform_capacity256, 1e6, 256, uniformDist)
BENCHMARK_NAMED_PARAM_MULTI(insert, uniform_capacity512, 1e6, 512, uniformDist)
BENCHMARK_NAMED_PARAM_MULTI(
    insert,
    uniform_capacity1024,
    1e6,
    1024,
    uniformDist)
BENCHMARK_NAMED_PARAM_MULTI(
    insert,
    uniform_capacity2048,
    1e6,
    2048,
    uniformDist)
BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(merge, capacity256, 1e6, 256)
BENCHMARK_NAMED_PARAM(merge, capacity512, 1e6, 512)
BENCHMARK_NAMED_PARAM(merge, capacity1024, 1e6, 1024)
BENCHMARK_NAMED_PARAM(merge, capacity2048, 1e6, 2048)

} // namespace
} // namespace facebook::velox::functions

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
