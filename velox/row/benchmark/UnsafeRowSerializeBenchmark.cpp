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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/ContainerRowSerde.h"
#include "velox/row/CompactRow.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::row {
namespace {

class SerializeBenchmark {
 public:
  RowVectorPtr makeData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;

    const auto seed = 1; // For reproducibility.
    VectorFuzzer fuzzer(options, pool_.get(), seed);

    return fuzzer.fuzzInputRow(rowType);
  }

  void runUnsafe(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    suspender.dismiss();

    UnsafeRowFast fast(data);

    size_t totalSize = 0;
    if (auto fixedRowSize = UnsafeRowFast::fixedRowSize(rowType)) {
      totalSize += fixedRowSize.value() * data->size();
    } else {
      for (auto i = 0; i < data->size(); ++i) {
        auto rowSize = fast.rowSize(i);
        totalSize += rowSize;
      }
    }

    auto buffer = AlignedBuffer::allocate<char>(totalSize, pool());
    auto rawBuffer = buffer->asMutable<char>();

    size_t offset = 0;
    for (auto i = 0; i < data->size(); ++i) {
      auto rowSize = fast.serialize(i, rawBuffer + offset);
      offset += rowSize;
    }

    VELOX_CHECK_EQ(totalSize, offset);
  }

  void runCompact(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    suspender.dismiss();

    CompactRow compact(data);

    size_t totalSize = 0;
    if (auto fixedRowSize = CompactRow::fixedRowSize(rowType)) {
      totalSize += fixedRowSize.value() * data->size();
    } else {
      for (auto i = 0; i < data->size(); ++i) {
        auto rowSize = compact.rowSize(i);
        totalSize += rowSize;
      }
    }

    auto buffer = AlignedBuffer::allocate<char>(totalSize, pool());
    auto rawBuffer = buffer->asMutable<char>();

    size_t offset = 0;
    for (auto i = 0; i < data->size(); ++i) {
      auto rowSize = compact.serialize(i, rawBuffer + offset);
      offset += rowSize;
    }

    VELOX_CHECK_EQ(totalSize, offset);
  }

  void runContainer(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    suspender.dismiss();

    HashStringAllocator allocator(pool());
    ByteStream out(&allocator);
    auto position = allocator.newWrite(out);
    for (auto i = 0; i < data->size(); ++i) {
      exec::ContainerRowSerde::serialize(*data, i, out);
    }
    allocator.finishWrite(out, 0);

    VELOX_CHECK_GT(out.size(), 0);
  }

 private:
  memory::MemoryPool* pool() {
    return pool_.get();
  }

  std::shared_ptr<memory::MemoryPool> pool_{memory::addDefaultLeafMemoryPool()};
};

#define SERDE_BENCHMARKS(name, rowType) \
  BENCHMARK(unsafe_##name) {            \
    SerializeBenchmark benchmark;       \
    benchmark.runUnsafe(rowType);       \
  }                                     \
                                        \
  BENCHMARK(compact_##name) {           \
    SerializeBenchmark benchmark;       \
    benchmark.runCompact(rowType);      \
  }                                     \
                                        \
  BENCHMARK(container_##name) {         \
    SerializeBenchmark benchmark;       \
    benchmark.runContainer(rowType);    \
  }

SERDE_BENCHMARKS(
    fixedWidth5,
    ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}));

SERDE_BENCHMARKS(
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

SERDE_BENCHMARKS(
    fixedWidth20,
    ROW({
        BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
        BIGINT(), BIGINT(), BIGINT(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(),
        DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), BIGINT(), BIGINT(),
    }));

SERDE_BENCHMARKS(strings1, ROW({BIGINT(), VARCHAR()}));

SERDE_BENCHMARKS(
    strings5,
    ROW({
        BIGINT(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
    }));

SERDE_BENCHMARKS(arrays, ROW({BIGINT(), ARRAY(BIGINT())}));

SERDE_BENCHMARKS(nestedArrays, ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}));

SERDE_BENCHMARKS(maps, ROW({BIGINT(), MAP(BIGINT(), REAL())}));

SERDE_BENCHMARKS(
    structs,
    ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}));

} // namespace
} // namespace facebook::velox::row

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
