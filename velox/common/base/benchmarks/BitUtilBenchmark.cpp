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

#include <optional>

#include "velox/common/base/BitUtil.h"
DECLARE_bool(bmi2); // NOLINT

namespace facebook {
namespace velox {
namespace test {

enum class Value {
  Variable,
  True,
  False,
};

enum class Interface { DynamicValue, SetClear };

template <Value value>
inline void callSetBitWithSetClearInterface(uint8_t* vecPtr, uint32_t i) {
  if constexpr (value == Value::True) {
    bits::setBit(vecPtr, i);
  } else if constexpr (value == Value::False) {
    bits::clearBit(vecPtr, i);
  } else {
    throw std::runtime_error("Invalid ValueToSet");
  }
}

template <Value value>
inline void callSetBitWithSetDynamicValue(uint8_t* vecPtr, uint32_t i) {
  if constexpr (value == Value::True) {
    bits::setBit(vecPtr, i, true);
  } else if constexpr (value == Value::False) {
    bits::setBit(vecPtr, i, false);
  } else {
    bits::setBit(vecPtr, i, i % 2 == 0);
  }
}

template <Interface interface, Value value>
void setNtBitBenchmark(uint32_t iterations, size_t numEntries) {
  folly::BenchmarkSuspender suspender;
  std::vector<uint8_t> vec(numEntries);
  uint8_t* vecPtr = vec.data();
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    if constexpr (interface == Interface::DynamicValue) {
      callSetBitWithSetDynamicValue<value>(vecPtr, i);
    } else {
      callSetBitWithSetClearInterface<value>(vecPtr, i);
    }
  }

  folly::doNotOptimizeAway(vec);
  suspender.rehire();
}

void BM_setBit(uint32_t iterations, size_t numEntries) {
  setNtBitBenchmark<Interface::SetClear, Value::True>(iterations, numEntries);
}

void BM_clearNthBit(uint32_t iterations, size_t numEntries) {
  setNtBitBenchmark<Interface::SetClear, Value::False>(iterations, numEntries);
}

void BM_setBit_true(uint32_t iterations, size_t numEntries) {
  setNtBitBenchmark<Interface::DynamicValue, Value::True>(
      iterations, numEntries);
}

void BM_setBit_false(uint32_t iterations, size_t numEntries) {
  setNtBitBenchmark<Interface::DynamicValue, Value::False>(
      iterations, numEntries);
}

void BM_setBit_variable(uint32_t iterations, size_t numEntries) {
  setNtBitBenchmark<Interface::DynamicValue, Value::Variable>(
      iterations, numEntries);
}

BENCHMARK_PARAM(BM_setBit, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_clearNthBit, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setBit_true, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setBit_false, 1000000);
BENCHMARK_RELATIVE_PARAM(BM_setBit_variable, 1000000);

void setupScatterBits(
    std::vector<uint64_t>& source,
    int32_t& numSource,
    std::vector<uint64_t>& mask,
    std::vector<uint64_t>& target) {
  // Set 3/4 of the bits.
  mask.resize(1000, 0xeeeeeeeeeeeeeeee);
  target.resize(1000);
  source.resize(1000, 0x1234567890abcdef);
  assert(!mask.empty()); // howtoeven does not recognize resize() above.
  numSource = __builtin_popcountll(mask[0]) * mask.size();
}

void runScatterBits(int32_t n, bool isSimple) {
  int32_t numSource;
  std::vector<uint64_t> source;
  std::vector<uint64_t> mask;
  std::vector<uint64_t> target;
  BENCHMARK_SUSPEND {
    FLAGS_bmi2 = !isSimple; // NOLINT
    setupScatterBits(source, numSource, mask, target);
  }
  auto sourceBits = reinterpret_cast<const char*>(source.data());
  auto targetBits = reinterpret_cast<char*>(target.data());
  for (auto i = 0; i < n; ++i) {
    bits::scatterBits(
        numSource, target.size() * 64, sourceBits, mask.data(), targetBits);
  }
  FLAGS_bmi2 = true; // NOLINT
}

BENCHMARK(BM_scatterBitsSimple, n) {
  runScatterBits(n, true);
}

BENCHMARK_RELATIVE(BM_scatterBits, n) {
  runScatterBits(n, false);
}

void BM_forEachBit(
    uint32_t iterations,
    size_t numBits,
    std::optional<size_t> bitToClear = std::nullopt) {
  folly::BenchmarkSuspender suspender;
  uint32_t size = bits::nwords(numBits);
  std::vector<uint64_t> data(size);
  uint64_t* rawData = data.data();
  bits::fillBits(rawData, 0, numBits, true);
  if (bitToClear.has_value()) {
    bits::setBit(rawData, bitToClear.value(), false);
  }
  suspender.dismiss();

  for (uint32_t i = 0; i < iterations; ++i) {
    bits::forEachBit(rawData, 0, numBits, true, [](auto row) {
      return folly::doNotOptimizeAway(row * row);
    });
  }
  suspender.rehire();
}

namespace {
constexpr static auto kNumRuns = 1000;
constexpr static auto numBits = 10000;
} // namespace
BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(forEachBitAllTrue) {
  BM_forEachBit(kNumRuns, numBits);
  return kNumRuns;
}

BENCHMARK_RELATIVE_MULTI(forEachBitLastBitFalse) {
  BM_forEachBit(kNumRuns, numBits, numBits - 1);
  return kNumRuns;
}

BENCHMARK_RELATIVE_MULTI(forEachBitFirstBitFalse) {
  BM_forEachBit(kNumRuns, numBits, 1);
  return kNumRuns;
}

} // namespace test
} // namespace velox
} // namespace facebook

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
