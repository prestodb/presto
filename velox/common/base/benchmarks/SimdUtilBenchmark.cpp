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

#include "velox/common/base/SimdUtil.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <random>

namespace facebook::velox {
namespace {

#define VELOX_BENCHMARK(_type, _name, ...) \
  [[maybe_unused]] _type _name(FOLLY_PP_STRINGIZE(_name), __VA_ARGS__)

template <typename T>
class LeadingMask {
 public:
  LeadingMask(const char* name, std::default_random_engine& gen) {
    std::uniform_int_distribution<> dist(0, xsimd::batch<T>::size + 1);
    for (int i = 0; i < kSize; ++i) {
      inputs_[i] = dist(gen);
    }
    folly::addBenchmark(__FILE__, name, [this] { return run(); });
  }

 private:
  unsigned run() {
    xsimd::batch_bool<T> ans = {};
    for (int i = 0; i < kSize; ++i) {
      ans = ans ^ simd::leadingMask<T>(inputs_[i]);
    }
    folly::doNotOptimizeAway(ans);
    return kSize;
  }

  static constexpr int kSize = 4 << 10;
  int8_t inputs_[kSize];
};

template <typename T>
class FromBitMask {
 public:
  FromBitMask(const char* name, std::default_random_engine& gen) {
    std::uniform_int_distribution<uint64_t> dist(
        0, (1ull << xsimd::batch<T>::size) - 1);
    for (int i = 0; i < kSize; ++i) {
      inputs_[i] = dist(gen);
    }
    folly::addBenchmark(__FILE__, name, [this] { return run(); });
  }

 private:
  unsigned run() {
    xsimd::batch_bool<T> ans = {};
    for (int i = 0; i < kSize; ++i) {
      ans = ans ^ simd::fromBitMask<T>(inputs_[i]);
    }
    folly::doNotOptimizeAway(ans);
    return kSize;
  }

  static constexpr int kSize = 2 << 10;
  uint64_t inputs_[kSize];
};

} // namespace
} // namespace facebook::velox

int main(int argc, char* argv[]) {
  using namespace facebook::velox;
  folly::Init follyInit(&argc, &argv);
  std::default_random_engine gen(std::random_device{}());
  VELOX_BENCHMARK(LeadingMask<int32_t>, leadingMaskInt32, gen);
  VELOX_BENCHMARK(LeadingMask<int64_t>, leadingMaskInt64, gen);
  VELOX_BENCHMARK(FromBitMask<int32_t>, fromBitMaskInt32, gen);
  VELOX_BENCHMARK(FromBitMask<int64_t>, fromBitMaskInt64, gen);
  folly::runBenchmarks();
  return 0;
}
