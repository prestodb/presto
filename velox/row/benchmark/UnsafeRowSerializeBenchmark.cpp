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

#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::row {
namespace {

class SerializeBenchmark {
 public:
  void run(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;

    VectorFuzzer::Options options;
    options.vectorSize = 1'000;

    const auto seed = 1; // For reproducibility.
    VectorFuzzer fuzzer(options, pool_.get(), seed);

    auto data = fuzzer.fuzzInputRow(rowType);

    suspender.dismiss();

    UnsafeRowFast fast(data);

    size_t totalSize = 0;
    if (auto fixedRowSize =
            UnsafeRowFast::fixedRowSize(asRowType(data->type()))) {
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

 private:
  memory::MemoryPool* pool() {
    return pool_.get();
  }

  std::shared_ptr<memory::MemoryPool> pool_{memory::addDefaultLeafMemoryPool()};
};

BENCHMARK(fixedWidth5) {
  SerializeBenchmark benchmark;
  benchmark.run(ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}));
}

BENCHMARK(fixedWidth10) {
  SerializeBenchmark benchmark;
  benchmark.run(ROW({
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

BENCHMARK(fixedWidth20) {
  SerializeBenchmark benchmark;
  benchmark.run(ROW({
      BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
      BIGINT(), BIGINT(), BIGINT(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(),
      DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), BIGINT(), BIGINT(),
  }));
}

BENCHMARK(strings1) {
  SerializeBenchmark benchmark;
  benchmark.run(ROW({BIGINT(), VARCHAR()}));
}

BENCHMARK(strings5) {
  SerializeBenchmark benchmark;
  benchmark.run(ROW({
      BIGINT(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
      VARCHAR(),
  }));
}

BENCHMARK(arrays) {
  SerializeBenchmark benchmark;
  benchmark.run(ROW({BIGINT(), ARRAY(BIGINT())}));
}

BENCHMARK(nestedArrays) {
  SerializeBenchmark benchmark;
  benchmark.run(ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}));
}

BENCHMARK(maps) {
  SerializeBenchmark benchmark;
  benchmark.run(ROW({BIGINT(), MAP(BIGINT(), REAL())}));
}

BENCHMARK(structs) {
  SerializeBenchmark benchmark;
  benchmark.run(
      ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}));
}

} // namespace
} // namespace facebook::velox::row

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
