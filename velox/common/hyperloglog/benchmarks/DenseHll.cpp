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
#include "velox/common/hyperloglog/DenseHll.h"
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/common/memory/HashStringAllocator.h"

#define XXH_INLINE_ALL
#include <xxhash.h>

using namespace facebook::velox;

namespace {

template <typename T>
uint64_t hashOne(T value) {
  return XXH64(&value, sizeof(value), 0);
}

// A benchmark for DenseHll::mergeWith(serialized) API.
//
// Measures the time it takes to merge 2 serialized digests using different
// values for hash bits. Larger values of hash bits corresponds to larger
// digests that are more accurate, but slower to merge. The default number of
// hash bits is 11, while in practice 16 is common.
class DenseHllBenchmark {
 public:
  explicit DenseHllBenchmark(memory::MemoryPool* pool) : pool_(pool) {
    for (auto hashBits : {11, 12, 16}) {
      serializedHlls_[hashBits].push_back(makeSerializedHll(hashBits, 1));
      serializedHlls_[hashBits].push_back(makeSerializedHll(hashBits, 2));
    }
  }

  void run(int hashBits) {
    folly::BenchmarkSuspender suspender;

    HashStringAllocator allocator(pool_);
    common::hll::DenseHll hll(hashBits, &allocator);

    suspender.dismiss();

    for (const auto& serialized : serializedHlls_.at(hashBits)) {
      hll.mergeWith(serialized.data());
    }
  }

 private:
  std::string makeSerializedHll(int hashBits, int32_t step) {
    HashStringAllocator allocator(pool_);
    common::hll::DenseHll hll(hashBits, &allocator);
    for (int32_t i = 0; i < 1'000'000; ++i) {
      auto hash = hashOne(i * step);
      hll.insertHash(hash);
    }
    return serialize(hll);
  }

  static std::string serialize(common::hll::DenseHll& denseHll) {
    auto size = denseHll.serializedSize();
    std::string serialized;
    serialized.resize(size);
    denseHll.serialize(serialized.data());
    return serialized;
  }

  memory::MemoryPool* pool_;

  // List of serialized HLLs to use for merging, keyed by the number of hash
  // bits.
  std::unordered_map<int, std::vector<std::string>> serializedHlls_;
};

} // namespace

std::unique_ptr<DenseHllBenchmark> benchmark;

BENCHMARK(mergeSerialized11) {
  benchmark->run(11);
}

BENCHMARK(mergeSerialized12) {
  benchmark->run(12);
}

BENCHMARK(mergeSerialized16) {
  benchmark->run(16);
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  memory::MemoryManager::initialize({});
  auto rootPool = memory::memoryManager()->addRootPool();
  auto pool = rootPool->addLeafChild("bm");
  benchmark = std::make_unique<DenseHllBenchmark>(pool.get());

  folly::runBenchmarks();
  return 0;
}
