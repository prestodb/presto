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

#include "folly/Benchmark.h"
#include "folly/init/Init.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/writer/ColumnWriter.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;

bool isNotNull(int32_t pos, int32_t run_len) {
  return pos == 0 || pos % run_len != 0;
}

float genData(float pos) {
  return float(pos * (float)3.14);
}

int32_t size = 0;
int32_t run_len = 1;

void FloatColumnWriterBenchmarkbase() {
  auto type = CppToType<float>::create();
  auto typeWithId = TypeWithId::create(type, 1);

  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  folly::BenchmarkSuspender braces;
  VectorPtr vector;

  // Generate sample data
  {
    // Prepare input
    BufferPtr values = AlignedBuffer::allocate<float>(size, &pool);
    auto valuesPtr = values->asMutable<float>();

    BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();

    uint32_t nullCount = 0;
    for (size_t i = 0; i < size; ++i) {
      bool isPresent = isNotNull(i, run_len);
      bits::setNull(nullsPtr, i, !isPresent);
      if (isPresent) {
        valuesPtr[i] = genData(i);
      } else {
        ++nullCount;
      }
    }

    vector = std::make_shared<FlatVector<float>>(
        &pool,
        nullCount == 0 ? nullptr : nulls,
        size,
        values,
        std::vector<BufferPtr>{});
  }

  // Writer
  {
    // write
    auto config = std::make_shared<Config>();
    WriterContext context{config, memory::getDefaultScopedMemoryPool()};
    auto writer = BaseColumnWriter::create(context, *typeWithId, 0);
    braces.dismiss();
    writer->write(vector, Ranges::of(0, size));
  }
}

BENCHMARK(FloatColumnWriterBenchmark2, n) {
  size = 10000;
  run_len = 2;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark5, n) {
  size = 10000;
  run_len = 5;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark10, n) {
  size = 10000;
  run_len = 10;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark25, n) {
  size = 10000;
  run_len = 25;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark50, n) {
  size = 10000;
  run_len = 50;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark100, n) {
  size = 10000;
  run_len = 100;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark500, n) {
  size = 10000;
  run_len = 500;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark1000, n) {
  size = 10000;
  run_len = 1000;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark2000, n) {
  size = 10000;
  run_len = 2000;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark5000, n) {
  size = 10000;
  run_len = 5000;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

BENCHMARK(FloatColumnWriterBenchmark10000, n) {
  size = 10000;
  run_len = 10000;
  for (size_t i = 0; i < n; i++) {
    FloatColumnWriterBenchmarkbase();
  }
}

int32_t main(int32_t argc, char* argv[]) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
