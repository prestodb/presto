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
#include <folly/Varint.h>
#include <folly/init/Init.h>
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/Range.h"
#include "velox/dwio/dwrf/common/DataBufferHolder.h"
#include "velox/dwio/dwrf/common/EncoderUtil.h"
#include "velox/dwio/dwrf/common/IntEncoder.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::memory;

static size_t generateAutoId(int64_t startId, int64_t count) {
  size_t capacity = count * folly::kMaxVarintLength64;
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  DataBufferHolder holder{*scopedPool, capacity};
  auto output = std::make_unique<BufferedOutputStream>(holder);
  auto encoder =
      createDirectEncoder<true>(std::move(output), true, sizeof(int64_t));

  for (int64_t i = 0; i < count; i++) {
    encoder->writeValue(startId + i);
  }
  return encoder->flush();
}

static size_t generateAutoId2(int64_t startId, int64_t count) {
  size_t capacity = count * folly::kMaxVarintLength64;
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  DataBufferHolder holder{*scopedPool, capacity};
  auto output = std::make_unique<BufferedOutputStream>(holder);
  auto encoder =
      createDirectEncoder<true>(std::move(output), true, sizeof(int64_t));

  int64_t buffer[1024];
  int64_t currentId = startId;
  int64_t countRemaining = count;
  while (countRemaining > 0) {
    int64_t bufCount = std::min(countRemaining, (int64_t)1024);
    for (int64_t i = 0; i < bufCount; ++i) {
      buffer[i] = currentId++;
    }
    encoder->add(buffer, common::Ranges::of(0, bufCount), nullptr);
    countRemaining -= bufCount;
  }
  return encoder->flush();
}

FOLLY_ALWAYS_INLINE static int32_t findSetBitsOld(uint64_t value) {
  if (value < (1ul << 14)) {
    if (value < (1ul << 7)) {
      return 1;
    }
    return 2;
  } else if (value < (1ul << 42)) {
    if (value < (1ul << 21)) {
      return 3;
    } else if (value < (1ul << 28)) {
      return 4;
    } else if (value < (1ul << 35)) {
      return 5;
    }
    return 6;
  } else {
    if (value < (1ul << 49)) {
      return 7;
    } else if (value < (1ul << 56)) {
      return 8;
    } else if (value < (1ul << 63)) {
      return 9;
    }
    return 10;
  }
}

FOLLY_ALWAYS_INLINE static int32_t findSetBitsNew(uint64_t value) {
  int32_t leadingZeros = __builtin_clzll(value | 1);
  DCHECK(leadingZeros <= 63);
  // bytes in varint can be calculated as (70 - leadingZeros)/7;
  switch (leadingZeros) {
    case 0:
      return 10;
    case 1 ... 7:
      return 9;
    case 8 ... 14:
      return 8;
    case 15 ... 21:
      return 7;
    case 22 ... 28:
      return 6;
    case 29 ... 35:
      return 5;
    case 36 ... 42:
      return 4;
    case 43 ... 49:
      return 3;
    case 50 ... 56:
      return 2;
    case 57 ... 63:
      return 1;
  }
  DWIO_RAISE(folly::sformat(
      "Unexpected leading zeros {} for value {}", leadingZeros, value));
}

BENCHMARK(findSetBitsOld, iters) {
  uint64_t value = 0;
  for (int64_t i = 0; i < iters; i++) {
    for (int64_t j = 0; j < iters; j++) {
      value += INT16_MAX;
      auto result = findSetBitsOld(value);
      folly::doNotOptimizeAway(result);
    }
  }
}

BENCHMARK_RELATIVE(findSetBitsNew, iters) {
  uint64_t value = 0;
  for (int64_t i = 0; i < iters; i++) {
    for (int64_t j = 0; j < iters; j++) {
      value += INT16_MAX;
      auto result = findSetBitsNew(value);
      folly::doNotOptimizeAway(result);
    }
  }
}

BENCHMARK(findSetBitsOld_low, iters) {
  uint64_t value = 0;
  for (int64_t i = 0; i < iters; i++) {
    for (int64_t j = 0; j < iters; j++) {
      value++;
      auto result = findSetBitsOld(value);
      folly::doNotOptimizeAway(result);
      value = value & 0xff;
    }
  }
}

BENCHMARK_RELATIVE(findSetBitsNew_low, iters) {
  uint64_t value = 0;
  for (int64_t i = 0; i < iters; i++) {
    for (int64_t j = 0; j < iters; j++) {
      value++;
      auto result = findSetBitsNew(value);
      folly::doNotOptimizeAway(result);
      value = value & 0xff;
    }
  }
}

BENCHMARK(GenerateAutoIdOld_0, iters) {
  for (int64_t i = 0; i < iters; i++) {
    auto result = generateAutoId(0, 100'000);
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(GenerateAutoIdNew_0, iters) {
  for (int64_t i = 0; i < iters; i++) {
    auto result = generateAutoId2(0, 100'000);
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK(GenerateAutoIdOld_32, iters) {
  for (int64_t i = 0; i < iters; i++) {
    auto result = generateAutoId(INT32_MAX, 100'000);
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(GenerateAutoIdNew_32, iters) {
  for (int64_t i = 0; i < iters; i++) {
    auto result = generateAutoId2(INT32_MAX, 100'000);
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK(GenerateAutoIdOld_64, iters) {
  for (int64_t i = 0; i < iters; i++) {
    auto result = generateAutoId(INT64_MAX - 500'000, 100'000);
    folly::doNotOptimizeAway(result);
  }
}

BENCHMARK_RELATIVE(GenerateAutoIdNew_64, iters) {
  for (int64_t i = 0; i < iters; i++) {
    auto result = generateAutoId2(INT64_MAX - 500'000, 100'000);
    folly::doNotOptimizeAway(result);
  }
}

int32_t main(int32_t argc, char* argv[]) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
