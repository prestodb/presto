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

BENCHMARK(unsafe_fixedWidth5) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}));
}

BENCHMARK_RELATIVE(container_fixedWidth5) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(
      ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}));
}

BENCHMARK(unsafe_fixedWidth10) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(ROW({
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
}

BENCHMARK_RELATIVE(container_fixedWidth10) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(ROW({
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
}

BENCHMARK(unsafe_fixedWidth20) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(ROW({
      BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
      BIGINT(), BIGINT(), BIGINT(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(),
      DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), BIGINT(), BIGINT(),
  }));
}

BENCHMARK_RELATIVE(container_fixedWidth20) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(ROW({
      BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
      BIGINT(), BIGINT(), BIGINT(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(),
      DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), BIGINT(), BIGINT(),
  }));
}

BENCHMARK(unsafe_strings1) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(ROW({BIGINT(), VARCHAR()}));
}

BENCHMARK_RELATIVE(container_strings1) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(ROW({BIGINT(), VARCHAR()}));
}

BENCHMARK(unsafe_strings5) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(ROW({
      BIGINT(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
  }));
}

BENCHMARK_RELATIVE(container_strings5) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(ROW({
      BIGINT(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
  }));
}

BENCHMARK(unsafe_arrays) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(ROW({BIGINT(), ARRAY(BIGINT())}));
}

BENCHMARK_RELATIVE(container_arrays) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(ROW({BIGINT(), ARRAY(BIGINT())}));
}

BENCHMARK(unsafe_nestedArrays) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}));
}

BENCHMARK_RELATIVE(container_nestedArrays) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}));
}

BENCHMARK(unsafe_maps) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(ROW({BIGINT(), MAP(BIGINT(), REAL())}));
}

BENCHMARK_RELATIVE(container_maps) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(ROW({BIGINT(), MAP(BIGINT(), REAL())}));
}

BENCHMARK(unsafe_structs) {
  SerializeBenchmark benchmark;
  benchmark.runUnsafe(
      ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}));
}

BENCHMARK_RELATIVE(container_structs) {
  SerializeBenchmark benchmark;
  benchmark.runContainer(
      ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}));
}

} // namespace
} // namespace facebook::velox::row

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
