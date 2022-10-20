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

#include "folly/init/Init.h"
#include "velox/dwio/common/ChainedBuffer.h"

using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;

BENCHMARK(DataBufferOps, iters) {
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  constexpr size_t size = 1024 * 1024 * 16;
  for (size_t i = 0; i < iters; ++i) {
    DataBuffer<int32_t> buf{pool};
    buf.reserve(size);
    for (size_t j = 0; j < size; ++j) {
      buf.unsafeAppend(j);
    }
    for (size_t j = 0; j < size; ++j) {
      folly::doNotOptimizeAway(buf[j]);
    }
  }
}

BENCHMARK(ChainedBufferOps, iters) {
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  constexpr size_t size = 1024 * 1024 * 16;
  for (size_t i = 0; i < iters; ++i) {
    ChainedBuffer<int32_t> buf{pool, size, size * 4};
    for (size_t j = 0; j < size; ++j) {
      buf.unsafeAppend(j);
    }
    for (size_t j = 0; j < size; ++j) {
      folly::doNotOptimizeAway(buf[j]);
    }
  }
}

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
