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

#include <folly/Random.h>
#include <folly/init/Init.h>
#include "folly/Benchmark.h"
#include "velox/type/Type.h"

namespace facebook::velox {
namespace {

void runStringViewCreate(uint32_t iterations, uint32_t len) {
  folly::BenchmarkSuspender suspender;

  const std::string chars =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  if (len > chars.size()) {
    len = chars.size();
  }
  size_t sum = 0;
  suspender.dismiss();

  for (auto i = 0; i < iterations; i++) {
    StringView str(chars.data() + i % (chars.size() - len), len);
    sum += str.data()[0];
  }

  folly::doNotOptimizeAway(sum);
}

constexpr auto INLINE_SIZE = StringView::kInlineSize;
constexpr auto NON_INLINE_SIZE = StringView::kInlineSize + 10;

// Short strings which can be inlined.
BENCHMARK_PARAM(runStringViewCreate, INLINE_SIZE);

// Larger strings which won't be inlined.
BENCHMARK_PARAM(runStringViewCreate, NON_INLINE_SIZE);
} // namespace
} // namespace facebook::velox

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  folly::runBenchmarks();
  return 0;
}
