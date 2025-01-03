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

#include "velox/common/base/SortingNetwork.h"

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#define VELOX_BENCHMARK(_type, _name, ...) \
  [[maybe_unused]] _type _name(FOLLY_PP_STRINGIZE(_name), __VA_ARGS__)

namespace facebook::velox {
namespace {

template <typename T>
class SortingNetworkBenchmark {
 public:
  SortingNetworkBenchmark(const char* name, int minLen, int maxLen) {
    int totalLen = 0;
    for (int i = 0; i < kNumSorts; ++i) {
      lengths_.push_back(folly::Random::rand32(minLen, maxLen));
      totalLen += lengths_.back();
    }
    data_.resize(totalLen);
    generateData();
    folly::addBenchmark(__FILE__, fmt::format("{}_std", name), [this] {
      return run([](auto* indices, int len, auto lt) {
        std::sort(indices, indices + len, lt);
      });
    });
    folly::addBenchmark(
        __FILE__, fmt::format("%{}_sorting_network", name), [this] {
          return run([](auto* indices, int len, auto lt) {
            sortingNetwork(indices, len, lt);
          });
        });
  }

 private:
  static constexpr int kNumSorts = 10'000;

  void generateData();

  template <typename Sort>
  unsigned run(Sort sort) const {
    std::vector<int32_t> indices;
    BENCHMARK_SUSPEND {
      for (int i = 0; i < kNumSorts; ++i) {
        indices.reserve(lengths_[i]);
      }
    }
    auto* buf = data_.data();
    for (int i = 0; i < kNumSorts; ++i) {
      indices.resize(lengths_[i]);
      std::iota(indices.begin(), indices.end(), 0);
      sort(indices.data(), lengths_[i], [&](auto i, auto j) {
        return buf[i] < buf[j];
      });
      buf += lengths_[i];
    }
    folly::doNotOptimizeAway(indices);
    return kNumSorts;
  }

  std::vector<int8_t> lengths_;
  std::vector<T> data_;
};

template <>
void SortingNetworkBenchmark<int32_t>::generateData() {
  for (int i = 0; i < data_.size(); ++i) {
    data_[i] = folly::Random::rand32();
  }
}

template <>
void SortingNetworkBenchmark<std::string>::generateData() {
  for (auto& s : data_) {
    s.resize(folly::Random::rand32(4, 32));
    for (int i = 0; i < s.size(); ++i) {
      s[i] = folly::Random::rand32(128);
    }
  }
}

struct ThreeWords {
  std::array<uint64_t, 3> value;

  bool operator<(const ThreeWords& other) const {
    return value < other.value;
  }
};

template <>
void SortingNetworkBenchmark<ThreeWords>::generateData() {
  for (auto& x : data_) {
    for (auto& y : x.value) {
      y = folly::Random::rand64();
    }
  }
}

} // namespace
} // namespace facebook::velox

int main(int argc, char* argv[]) {
  using namespace facebook::velox;
  folly::Init follyInit(&argc, &argv);
  VELOX_BENCHMARK(SortingNetworkBenchmark<int32_t>, int32_2, 2, 4);
  VELOX_BENCHMARK(SortingNetworkBenchmark<int32_t>, int32_4, 4, 8);
  VELOX_BENCHMARK(SortingNetworkBenchmark<int32_t>, int32_8, 8, 16);
  VELOX_BENCHMARK(SortingNetworkBenchmark<std::string>, string_2, 2, 4);
  VELOX_BENCHMARK(SortingNetworkBenchmark<std::string>, string_4, 4, 8);
  VELOX_BENCHMARK(SortingNetworkBenchmark<std::string>, string_8, 8, 16);
  VELOX_BENCHMARK(SortingNetworkBenchmark<ThreeWords>, ThreeWords_2, 2, 4);
  VELOX_BENCHMARK(SortingNetworkBenchmark<ThreeWords>, ThreeWords_4, 4, 8);
  VELOX_BENCHMARK(SortingNetworkBenchmark<ThreeWords>, ThreeWords_8, 8, 16);
  folly::runBenchmarks();
  return 0;
}
