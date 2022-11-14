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

#include "velox/buffer/Buffer.h"
#include "velox/dwio/common/BufferUtil.h"
#include "velox/dwio/parquet/reader/NestedStructureDecoder.h"
#include "velox/vector/TypeAliases.h"

#include <folly/Benchmark.h>

using namespace facebook::velox;
using namespace facebook::velox::parquet;

class NestedStructureDecoderBenchmark {
 public:
  NestedStructureDecoderBenchmark(uint64_t numValues)
      : numValues_(numValues),
        definitionLevels_(new unsigned char[numValues]()),
        repetitionLevels_(new unsigned char[numValues]()),
        pool_(memory::getDefaultMemoryPool()) {}

  void setUp(uint16_t maxDefinition, uint16_t maxRepetition) {
    dwio::common::ensureCapacity<uint64_t>(
        nullsBuffer_, numValues_ / 64 + 1, pool_.get());
    dwio::common::ensureCapacity<vector_size_t>(
        offsetsBuffer_, numValues_ + 1, pool_.get());
    dwio::common::ensureCapacity<vector_size_t>(
        lengthsBuffer_, numValues_, pool_.get());

    populateInputs(maxDefinition, maxRepetition);
  }

  uint64_t numValues_;

  std::unique_ptr<uint8_t> definitionLevels_;
  std::unique_ptr<uint8_t> repetitionLevels_;
  std::shared_ptr<memory::MemoryPool> pool_;

  BufferPtr offsetsBuffer_;
  BufferPtr lengthsBuffer_;
  BufferPtr nullsBuffer_;

 private:
  void populateInputs(uint16_t maxDefinition, uint16_t maxRepetition) {
    auto def = definitionLevels_.get();
    auto rep = repetitionLevels_.get();
    for (int i = 0; i < numValues_; i++) {
      def[i] = rand() % maxDefinition;
      rep[i] = rand() % maxRepetition;
    }
  }
};

BENCHMARK(randomDefs) {
  folly::BenchmarkSuspender suspender;

  auto numValues = 1'000'000;
  auto maxDefinition = 9;
  auto maxRepetition = 4;

  NestedStructureDecoderBenchmark benchmark(numValues);
  benchmark.setUp(maxDefinition, maxRepetition);

  suspender.dismiss();

  int64_t numCollections = NestedStructureDecoder::readOffsetsAndNulls(
      benchmark.definitionLevels_.get(),
      benchmark.repetitionLevels_.get(),
      numValues,
      maxDefinition / 2,
      maxRepetition / 2,
      benchmark.offsetsBuffer_,
      benchmark.lengthsBuffer_,
      benchmark.nullsBuffer_,
      *benchmark.pool_);

  folly::doNotOptimizeAway(numCollections);
}

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
