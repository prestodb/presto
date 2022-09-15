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

#include "velox/vector/SimpleVector.h"
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::test {
namespace {

template <typename T>
VectorGeneratedData<T>
testData(size_t length, size_t cardinality, bool sequences, size_t iteration) {
  int32_t seqCount =
      sequences ? std::max(2, (int32_t)(length / cardinality)) : 0;
  int32_t seqLength = sequences ? std::max(2, (int32_t)(length / seqCount)) : 0;
  return genTestDataWithSequences<T>(
      length,
      cardinality,
      false /* isSorted */,
      true /* includeNulls */,
      seqCount,
      seqLength,
      false /* useFullTypeRange */,
      length + iteration /* seed */);
}

void BM_StdVector_hashAll(
    uint32_t iterations,
    size_t numRows,
    size_t cardinality,
    bool sequences) {
  folly::BenchmarkSuspender suspender;
  for (size_t k = 0; k < iterations; ++k) {
    auto source = std::make_unique<std::vector<int64_t>>();
    auto nulls = std::make_unique<std::vector<bool>>();
    auto data = testData<int64_t>(numRows, cardinality, sequences, k);
    source->reserve(numRows);
    nulls->reserve(numRows);

    for (auto val : data.data()) {
      if (val == std::nullopt) {
        nulls->push_back(true);
        source->push_back(0);
      } else {
        nulls->push_back(false);
        source->push_back(*val);
      }
    }
    suspender.dismiss();

    folly::hasher<int64_t> hasher;
    auto hashes = std::make_unique<std::vector<size_t>>();
    hashes->reserve(numRows);
    for (size_t i = 0; i < numRows; ++i) {
      if (nulls->at(i)) {
        hashes->push_back(0);
      } else {
        hashes->push_back(hasher(source->at(i)));
      }
    }
    folly::doNotOptimizeAway(hashes);

    suspender.rehire();
  }
}

template <typename T>
void vectorBenchmark(
    uint32_t iterations,
    size_t length,
    size_t cardinality,
    bool sequences,
    VectorEncoding::Simple encoding) {
  folly::BenchmarkSuspender suspender;
  auto pool = memory::getDefaultScopedMemoryPool();
  VectorMaker vectorMaker(pool.get());

  for (size_t k = 0; k < iterations; ++k) {
    auto data = testData<T>(length, cardinality, sequences, k);
    auto vector = vectorMaker.encodedVector<T>(encoding, data.data());

    suspender.dismiss();

    auto hashes = vector->hashAll();
    folly::doNotOptimizeAway(hashes);

    suspender.rehire();
  }
}

void BM_Flat_hashAll(
    uint32_t iterations,
    size_t numRows,
    size_t cardinality,
    bool sequences) {
  vectorBenchmark<int64_t>(
      iterations,
      numRows,
      cardinality,
      sequences,
      VectorEncoding::Simple::FLAT);
}

void BM_Dictionary_hashAll(
    uint32_t iterations,
    size_t numRows,
    size_t cardinality,
    bool sequences) {
  vectorBenchmark<int64_t>(
      iterations,
      numRows,
      cardinality,
      sequences,
      VectorEncoding::Simple::DICTIONARY);
}

void BM_Biased_hashAll(
    uint32_t iterations,
    size_t numRows,
    size_t cardinality,
    bool sequences) {
  vectorBenchmark<int64_t>(
      iterations,
      numRows,
      cardinality,
      sequences,
      VectorEncoding::Simple::BIASED);
}

void BM_Sequence_hashAll(
    uint32_t iterations,
    size_t numRows,
    size_t cardinality) {
  vectorBenchmark<int64_t>(
      iterations, numRows, cardinality, true, VectorEncoding::Simple::SEQUENCE);
}

void BM_Constant_hashAll(uint32_t iterations, size_t numRows) {
  vectorBenchmark<int64_t>(
      iterations, numRows, 1, false, VectorEncoding::Simple::CONSTANT);
}
} // namespace

// 100k rows===============
BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    100k_rows_100k_uni_noseq,
    100000,
    100000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    100k_rows_100k_uni_noseq,
    100000,
    100000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    100k_rows_100k_uni_noseq,
    100000,
    100000,
    false);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    100k_rows_10k_uni_noseq,
    100000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    100k_rows_10k_uni_noseq,
    100000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    100k_rows_10k_uni_noseq,
    100000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    100k_rows_10k_uni_noseq,
    100000,
    10000,
    false);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    100k_rows_10k_uni_seq,
    100000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    100k_rows_10k_uni_seq,
    100000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    100k_rows_10k_uni_seq,
    100000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    100k_rows_10k_uni_seq,
    100000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Sequence_hashAll,
    100k_rows_10k_uni_seq,
    100000,
    10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    100k_rows_1_uni_noseq,
    100000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    100k_rows_1_uni_noseq,
    100000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    100k_rows_1_uni_noseq,
    100000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Constant_hashAll,
    100k_rows_1_uni_noseq,
    100000);
BENCHMARK_DRAW_LINE();

// 1M rows===============
BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    1M_rows_1M_uni_noseq,
    1000000,
    1000000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    1M_rows_1M_uni_noseq,
    1000000,
    1000000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    1M_rows_1M_uni_noseq,
    1000000,
    1000000,
    false);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    1M_rows_10k_uni_noseq,
    1000000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    1M_rows_10k_uni_noseq,
    1000000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    1M_rows_10k_uni_noseq,
    1000000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    1M_rows_10k_uni_noseq,
    1000000,
    10000,
    false);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    1M_rows_10k_uni_seq,
    1000000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    1M_rows_10k_uni_seq,
    1000000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    1M_rows_10k_uni_seq,
    1000000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    1M_rows_10k_uni_seq,
    1000000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Sequence_hashAll,
    1M_rows_10k_uni_seq,
    1000000,
    10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    1M_rows_1_uni_noseq,
    1000000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    1M_rows_1_uni_noseq,
    1000000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    1M_rows_1_uni_noseq,
    1000000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Constant_hashAll,
    1M_rows_1_uni_noseq,
    1000000);
BENCHMARK_DRAW_LINE();

// 2M rows===============
BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    2M_rows_2M_uni_noseq,
    2000000,
    2000000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    2M_rows_2M_uni_noseq,
    2000000,
    2000000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    2M_rows_2M_uni_noseq,
    2000000,
    2000000,
    false);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    2M_rows_10k_uni_noseq,
    2000000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    2M_rows_10k_uni_noseq,
    2000000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    2M_rows_10k_uni_noseq,
    2000000,
    10000,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    2M_rows_10k_uni_noseq,
    2000000,
    10000,
    false);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    2M_rows_10k_uni_seq,
    2000000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    2M_rows_10k_uni_seq,
    2000000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Biased_hashAll,
    2M_rows_10k_uni_seq,
    2000000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    2M_rows_10k_uni_seq,
    2000000,
    10000,
    true);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Sequence_hashAll,
    2M_rows_10k_uni_seq,
    2000000,
    10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    BM_StdVector_hashAll,
    2M_rows_1_uni_noseq,
    2000000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Flat_hashAll,
    2M_rows_1_uni_noseq,
    2000000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Dictionary_hashAll,
    2M_rows_1_uni_noseq,
    2000000,
    1,
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    BM_Constant_hashAll,
    2M_rows_1_uni_noseq,
    2000000);
BENCHMARK_DRAW_LINE();

} // namespace facebook::velox::test

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
