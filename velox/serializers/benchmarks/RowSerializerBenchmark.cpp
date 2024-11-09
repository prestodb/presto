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

#include "velox/serializers/CompactRowSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::test {
namespace {
class RowSerializerBenchmark {
 public:
  void compactRowVectorSerde(
      const RowTypePtr& rowType,
      vector_size_t rangeSize) {
    serializer::CompactRowVectorSerde::registerVectorSerde();
    serialize(rowType, rangeSize);
    deregisterVectorSerde();
  }

 private:
  void serialize(const RowTypePtr& rowType, vector_size_t rangeSize) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    std::vector<IndexRange> indexRanges;
    for (auto begin = 0; begin < data->size(); begin += rangeSize) {
      indexRanges.push_back(
          IndexRange{begin, std::min(rangeSize, data->size() - begin)});
    }
    suspender.dismiss();

    Scratch scratch;
    auto group = std::make_unique<VectorStreamGroup>(pool_.get(), nullptr);
    group->createStreamTree(rowType, data->size());
    group->append(
        data, folly::Range(indexRanges.data(), indexRanges.size()), scratch);

    std::stringstream stream;
    OStreamOutputStream outputStream(&stream);
    group->flush(&outputStream);
  }

  RowVectorPtr makeData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;

    const auto seed = 1; // For reproducibility.
    VectorFuzzer fuzzer(options, pool_.get(), seed);

    return fuzzer.fuzzInputRow(rowType);
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

#define VECTOR_SERDE_BENCHMARKS(name, rowType)       \
  BENCHMARK(compact_serialize_1_##name) {            \
    RowSerializerBenchmark benchmark;                \
    benchmark.compactRowVectorSerde(rowType, 1);     \
  }                                                  \
  BENCHMARK(compact_serialize_10_##name) {           \
    RowSerializerBenchmark benchmark;                \
    benchmark.compactRowVectorSerde(rowType, 10);    \
  }                                                  \
  BENCHMARK(compact_serialize_100_##name) {          \
    RowSerializerBenchmark benchmark;                \
    benchmark.compactRowVectorSerde(rowType, 100);   \
  }                                                  \
  BENCHMARK(compact_serialize_1000_##name) {         \
    RowSerializerBenchmark benchmark;                \
    benchmark.compactRowVectorSerde(rowType, 1'000); \
  }

VECTOR_SERDE_BENCHMARKS(
    fixedWidth5,
    ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}));

VECTOR_SERDE_BENCHMARKS(
    fixedWidth10,
    ROW({
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        DOUBLE(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
    }));

VECTOR_SERDE_BENCHMARKS(
    fixedWidth20,
    ROW({
        BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
        BIGINT(), BIGINT(), BIGINT(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(),
        DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), BIGINT(), BIGINT(),
    }));

VECTOR_SERDE_BENCHMARKS(
    decimal,
    ROW({BIGINT(), DECIMAL(12, 2), DECIMAL(38, 18)}));

VECTOR_SERDE_BENCHMARKS(strings1, ROW({BIGINT(), VARCHAR()}));

VECTOR_SERDE_BENCHMARKS(
    strings5,
    ROW({
        BIGINT(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
    }));

VECTOR_SERDE_BENCHMARKS(arrays, ROW({BIGINT(), ARRAY(BIGINT())}));

VECTOR_SERDE_BENCHMARKS(nestedArrays, ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}));

VECTOR_SERDE_BENCHMARKS(maps, ROW({BIGINT(), MAP(BIGINT(), REAL())}));

VECTOR_SERDE_BENCHMARKS(
    structs,
    ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}));

} // namespace
} // namespace facebook::velox::test

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  facebook::velox::memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
