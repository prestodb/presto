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

#include <gflags/gflags.h>

#include "velox/common/memory/Memory.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

namespace {

using namespace facebook::velox;

std::unique_ptr<memory::MemoryPool> pool{memory::getDefaultScopedMemoryPool()};

VectorFuzzer::Options getOpts(size_t n, double nullRatio = 0) {
  VectorFuzzer::Options opts;
  opts.vectorSize = n;
  opts.nullRatio = nullRatio;
  return opts;
}

BENCHMARK_MULTI(flatInteger, n) {
  VectorFuzzer fuzzer(getOpts(n), pool.get(), FLAGS_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(BIGINT()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatIntegerHalfNull, n) {
  VectorFuzzer fuzzer(getOpts(n, 0.5), pool.get(), FLAGS_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(BIGINT()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatDouble, n) {
  VectorFuzzer fuzzer(getOpts(n), pool.get(), FLAGS_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(DOUBLE()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatBool, n) {
  VectorFuzzer fuzzer(getOpts(n), pool.get(), FLAGS_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(BOOLEAN()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatVarcharAscii, n) {
  auto opts = getOpts(n);
  opts.charEncodings = {UTF8CharList::ASCII};

  VectorFuzzer fuzzer(opts, pool.get(), FLAGS_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(VARCHAR()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatVarcharUtf8, n) {
  auto opts = getOpts(n);
  opts.charEncodings = {UTF8CharList::EXTENDED_UNICODE};

  VectorFuzzer fuzzer(opts, pool.get(), FLAGS_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzFlat(VARCHAR()));
  return n;
}

BENCHMARK_DRAW_LINE();

BENCHMARK_RELATIVE_MULTI(constantInteger, n) {
  VectorFuzzer fuzzer(getOpts(n), pool.get(), FLAGS_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzConstant(BIGINT()));
  return n;
}

BENCHMARK_RELATIVE_MULTI(dictionaryInteger, n) {
  VectorFuzzer fuzzer(getOpts(n), pool.get(), FLAGS_fuzzer_seed);
  folly::doNotOptimizeAway(fuzzer.fuzzDictionary(fuzzer.fuzzFlat(BIGINT())));
  return n;
}

BENCHMARK_DRAW_LINE();

BENCHMARK_RELATIVE_MULTI(flatArray, n) {
  VectorFuzzer fuzzer(getOpts(n), pool.get(), FLAGS_fuzzer_seed);
  const size_t elementsSize = n * fuzzer.getOptions().containerLength;
  folly::doNotOptimizeAway(
      fuzzer.fuzzArray(fuzzer.fuzzFlat(BIGINT(), elementsSize), n));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatMap, n) {
  VectorFuzzer fuzzer(getOpts(n), pool.get(), FLAGS_fuzzer_seed);
  const size_t elementsSize = n * fuzzer.getOptions().containerLength;
  folly::doNotOptimizeAway(fuzzer.fuzzMap(
      fuzzer.fuzzFlat(BIGINT(), elementsSize),
      fuzzer.fuzzFlat(BIGINT(), elementsSize),
      n));
  return n;
}

BENCHMARK_RELATIVE_MULTI(flatMapArrayNested, n) {
  VectorFuzzer fuzzer(getOpts(n), pool.get(), FLAGS_fuzzer_seed);
  const size_t elementsSize = n * fuzzer.getOptions().containerLength;

  folly::doNotOptimizeAway(fuzzer.fuzzMap(
      fuzzer.fuzzFlat(BIGINT(), elementsSize),
      fuzzer.fuzzArray(
          fuzzer.fuzzFlat(BIGINT(), elementsSize * 10), elementsSize),
      n));
  return n;
}

} // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}
