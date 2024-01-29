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

constexpr vector_size_t kVectorSize = 10000;
vector_size_t kNumIterations = 1000;

float genData(float pos) {
  return float(pos * (float)3.14);
}

bool isNotNull(int32_t pos, int32_t nullEvery) {
  return pos == 0 || pos % nullEvery != 0;
}

void runBenchmark(int nullEvery) {
  folly::BenchmarkSuspender braces;

  auto type = CppToType<float>::create();
  auto typeWithId = TypeWithId::create(type, 1);
  auto pool = memory::memoryManager()->addLeafPool();
  VectorPtr vector;
  // Prepare input
  BufferPtr values = AlignedBuffer::allocate<float>(kVectorSize, pool.get());
  auto valuesPtr = values->asMutable<float>();

  BufferPtr nulls =
      AlignedBuffer::allocate<char>(bits::nbytes(kVectorSize), pool.get());
  auto* nullsPtr = nulls->asMutable<uint64_t>();

  uint32_t nullCount = 0;
  for (size_t i = 0; i < kVectorSize; ++i) {
    bool isPresent = isNotNull(i, nullEvery);
    bits::setNull(nullsPtr, i, !isPresent);
    if (isPresent) {
      valuesPtr[i] = genData(i);
    } else {
      ++nullCount;
    }
  }

  vector = std::make_shared<FlatVector<float>>(
      pool.get(),
      REAL(),
      nullCount == 0 ? nullptr : nulls,
      kVectorSize,
      values,
      std::vector<BufferPtr>{});

  // write
  braces.dismiss();

  for (auto i = 0; i < kNumIterations; i++) {
    auto config = std::make_shared<dwrf::Config>();
    WriterContext context{
        config,
        memory::memoryManager()->addRootPool("FloatColumnWriterBenchmark")};
    auto writer = BaseColumnWriter::create(context, *typeWithId, 0);
    writer->write(vector, common::Ranges::of(0, kVectorSize));
  }
}

BENCHMARK(FloatColumnWriterBenchmark2) {
  runBenchmark(2);
}

BENCHMARK(FloatColumnWriterBenchmark5) {
  runBenchmark(5);
}

BENCHMARK(FloatColumnWriterBenchmark10) {
  runBenchmark(10);
}

BENCHMARK(FloatColumnWriterBenchmark25) {
  runBenchmark(25);
}

BENCHMARK(FloatColumnWriterBenchmark50) {
  runBenchmark(50);
}

BENCHMARK(FloatColumnWriterBenchmark100) {
  runBenchmark(100);
}

BENCHMARK(FloatColumnWriterBenchmark500) {
  runBenchmark(500);
}

BENCHMARK(FloatColumnWriterBenchmark1000) {
  runBenchmark(1000);
}

BENCHMARK(FloatColumnWriterBenchmark2000) {
  runBenchmark(2000);
}

BENCHMARK(FloatColumnWriterBenchmark5000) {
  runBenchmark(5000);
}

BENCHMARK(FloatColumnWriterBenchmark10000) {
  runBenchmark(10000);
}

int32_t main(int32_t argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
