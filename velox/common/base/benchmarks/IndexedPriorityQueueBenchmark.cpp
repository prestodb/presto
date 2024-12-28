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

#include "velox/common/base/IndexedPriorityQueue.h"

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#define VELOX_BENCHMARK(_type, _name, ...) \
  [[maybe_unused]] _type _name(FOLLY_PP_STRINGIZE(_name), __VA_ARGS__)

namespace facebook::velox {
namespace {

template <typename T>
class IndexedPriorityQueueBenchmark {
 public:
  IndexedPriorityQueueBenchmark(const char* name, int numValues)
      : values_(numValues), priorities_(2 * numValues) {
    generateValues();
    for (int i = 0; i < 2 * numValues; ++i) {
      priorities_[i] = folly::Random::rand32();
    }
    folly::addBenchmark(__FILE__, fmt::format("{}_add", name), [this] {
      IndexedPriorityQueue<T, true> queue;
      return add(queue);
    });
    folly::addBenchmark(__FILE__, fmt::format("{}_update", name), [this] {
      IndexedPriorityQueue<T, true> queue;
      BENCHMARK_SUSPEND {
        add(queue);
      }
      return update(queue);
    });
    folly::addBenchmark(__FILE__, fmt::format("{}_pop", name), [this] {
      IndexedPriorityQueue<T, true> queue;
      BENCHMARK_SUSPEND {
        add(queue);
        update(queue);
      }
      return pop(queue);
    });
  }

 private:
  void generateValues();

  unsigned add(IndexedPriorityQueue<T, true>& queue) const {
    for (int i = 0; i < values_.size(); ++i) {
      queue.addOrUpdate(values_[i], priorities_[i]);
    }
    return values_.size();
  }

  unsigned update(IndexedPriorityQueue<T, true>& queue) const {
    for (int i = 0; i < values_.size(); ++i) {
      queue.addOrUpdate(values_[i], priorities_[i + values_.size()]);
    }
    return values_.size();
  }

  unsigned pop(IndexedPriorityQueue<T, true>& queue) const {
    while (!queue.empty()) {
      queue.pop();
    }
    return values_.size();
  }

  std::vector<T> values_;
  std::vector<uint32_t> priorities_;
};

template <>
void IndexedPriorityQueueBenchmark<int64_t>::generateValues() {
  std::iota(values_.begin(), values_.end(), 0);
}

} // namespace
} // namespace facebook::velox

int main(int argc, char* argv[]) {
  using namespace facebook::velox;
  folly::Init follyInit(&argc, &argv);
  VELOX_BENCHMARK(IndexedPriorityQueueBenchmark<int64_t>, int64_1000, 1000);
  VELOX_BENCHMARK(IndexedPriorityQueueBenchmark<int64_t>, int64_10000, 10'000);
  VELOX_BENCHMARK(
      IndexedPriorityQueueBenchmark<int64_t>, int64_100000, 100'000);
  VELOX_BENCHMARK(
      IndexedPriorityQueueBenchmark<int64_t>, int64_1000000, 1'000'000);
  folly::runBenchmarks();
  return 0;
}
