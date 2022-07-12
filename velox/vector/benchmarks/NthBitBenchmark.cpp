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

static constexpr uint8_t kOneBitmasks[] = {1, 2, 4, 8, 16, 32, 64, 128};
static constexpr uint8_t kZeroBitmasks[] =
    {254, 253, 251, 247, 239, 223, 191, 127};

template <typename index_type, bool Value>
FOLLY_NOINLINE void setNthBit_array(uint8_t* resultBufferPtr, index_type idx) {
  if constexpr (Value) {
    // Set bit
    resultBufferPtr[idx / 8] |= kOneBitmasks[idx % 8];
  } else {
    // Clear bit
    resultBufferPtr[idx / 8] &= kZeroBitmasks[idx % 8];
  }
}

template <typename index_type, bool Value>
FOLLY_NOINLINE void setNthBit_shift(uint8_t* resultBufferPtr, index_type idx) {
  if constexpr (Value) {
    // Set bit
    resultBufferPtr[idx / 8] |= (1 << (idx % 8));
  } else {
    // Clear bit
    resultBufferPtr[idx / 8] &= ~(1 << (idx % 8));
  }
}

template <typename index_type, bool Value>
void setNthBit_array_Test(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  std::vector<uint8_t> vec(numEntries);
  uint8_t* vecPtr = vec.data();
  suspender.dismiss();

  for (index_type i = 0; i < iterations; ++i) {
    setNthBit_array<index_type, Value>(vecPtr, i % numEntries);
  }

  folly::doNotOptimizeAway(vec);
  suspender.rehire();
}

template <typename index_type, bool Value>
void setNthBit_shift_Test(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  std::vector<uint8_t> vec(numEntries);
  uint8_t* vecPtr = vec.data();
  suspender.dismiss();

  for (index_type i = 0; i < iterations; ++i) {
    setNthBit_shift<index_type, Value>(vecPtr, i % numEntries);
  }

  folly::doNotOptimizeAway(vec);
  suspender.rehire();
}

void BM_setNthBit_array_true_signed(uint32_t iterations, size_t numEntries) {
  setNthBit_array_Test<int32_t, true>(iterations, numEntries);
}

void BM_setNthBit_array_true_unsigned(uint32_t iterations, size_t numEntries) {
  setNthBit_array_Test<uint32_t, true>(iterations, numEntries);
}

void BM_setNthBit_shift_true_signed(uint32_t iterations, size_t numEntries) {
  setNthBit_shift_Test<int32_t, true>(iterations, numEntries);
}

void BM_setNthBit_shift_true_unsigned(uint32_t iterations, size_t numEntries) {
  setNthBit_shift_Test<uint32_t, true>(iterations, numEntries);
}

void BM_setNthBit_array_false_signed(uint32_t iterations, size_t numEntries) {
  setNthBit_array_Test<int32_t, false>(iterations, numEntries);
}

void BM_setNthBit_array_false_unsigned(uint32_t iterations, size_t numEntries) {
  setNthBit_array_Test<uint32_t, false>(iterations, numEntries);
}

void BM_setNthBit_shift_false_signed(uint32_t iterations, size_t numEntries) {
  setNthBit_shift_Test<int32_t, false>(iterations, numEntries);
}

void BM_setNthBit_shift_false_unsigned(uint32_t iterations, size_t numEntries) {
  setNthBit_shift_Test<uint32_t, false>(iterations, numEntries);
}

BENCHMARK_PARAM(BM_setNthBit_shift_true_unsigned, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setNthBit_array_true_unsigned, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setNthBit_array_true_signed, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setNthBit_shift_true_signed, 1000000);
BENCHMARK_DRAW_LINE();

BENCHMARK_PARAM(BM_setNthBit_array_false_unsigned, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setNthBit_array_false_signed, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setNthBit_shift_false_signed, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setNthBit_shift_false_unsigned, 1000000);
BENCHMARK_DRAW_LINE();

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
