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
#include "velox/exec/VectorHasher.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
class BenchmarkBase {
 public:
  VectorMaker vectorMaker() const {
    return vectorMaker_;
  }

  BufferPtr makeIndices(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t)> indexAt) {
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (auto i = 0; i < size; i++) {
      rawIndices[i] = indexAt(i);
    }
    return indices;
  }

  VectorPtr makeDictionary(vector_size_t size, const VectorPtr& base) {
    auto baseSize = base->size();
    return BaseVector::wrapInDictionary(
        BufferPtr(nullptr),
        makeIndices(
            size, [baseSize](vector_size_t row) { return row % baseSize; }),
        size,
        base);
  }

 private:
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  VectorMaker vectorMaker_{pool_.get()};
};

template <typename T>
void benchmarkComputeValueIds(bool withNulls) {
  folly::BenchmarkSuspender suspender;
  vector_size_t size = 1'000;
  BenchmarkBase base;
  VectorHasher hasher(CppToType<T>::create(), 0);
  auto values = base.vectorMaker().flatVector<T>(
      size,
      [](vector_size_t row) { return row % 17; },
      withNulls ? test::VectorMaker::nullEvery(7) : nullptr);

  raw_vector<uint64_t> hashes(size);
  SelectivityVector rows(size);
  hasher.decode(*values, rows);
  hasher.computeValueIds(rows, hashes);
  hasher.enableValueRange(1, 0);
  suspender.dismiss();

  for (int i = 0; i < 10'000; i++) {
    hasher.decode(*values, rows);
    bool ok = hasher.computeValueIds(rows, hashes);
    folly::doNotOptimizeAway(ok);
  }
}
} // namespace

// Uses SIMD acceleration
BENCHMARK(computeValueIdsBigintNoNulls) {
  benchmarkComputeValueIds<int64_t>(false);
}

// Doesn't use SIMD acceleration
BENCHMARK_RELATIVE(computeValueIdsBigintWithNulls) {
  benchmarkComputeValueIds<int64_t>(true);
}

BENCHMARK(computeValueIdsIntegerNoNulls) {
  benchmarkComputeValueIds<int32_t>(false);
}

BENCHMARK_RELATIVE(computeValueIdsIntegerWithNulls) {
  benchmarkComputeValueIds<int32_t>(true);
}

BENCHMARK(computeValueIdsSmallintNoNulls) {
  benchmarkComputeValueIds<int16_t>(false);
}

BENCHMARK_RELATIVE(computeValueIdsSmallintWithNulls) {
  benchmarkComputeValueIds<int16_t>(true);
}

void benchmarkComputeValueIdsForStrings(bool flattenDictionaries) {
  folly::BenchmarkSuspender suspender;
  BenchmarkBase base;
  auto b0 = base.vectorMaker().flatVector({"2021-02-02", "2021-02-01"});
  auto b1 = base.vectorMaker().flatVector({"red", "green"});
  auto b2 = base.vectorMaker().flatVector(
      {"apple", "orange", "grapefruit", "banana", "star fruit", "potato"});
  auto b3 = base.vectorMaker().flatVector(
      {"pine", "birch", "elm", "maple", "chestnut"});

  std::vector<VectorPtr> baseVectors = {b0, b1, b2, b3};

  std::vector<VectorPtr> dictionaryVectors;
  dictionaryVectors.reserve(baseVectors.size());

  vector_size_t size = 1'000;
  for (auto& baseVector : baseVectors) {
    dictionaryVectors.emplace_back(base.makeDictionary(size, baseVector));
  }

  std::vector<VectorPtr> vectors;
  if (flattenDictionaries) {
    vectors.reserve(dictionaryVectors.size());
    for (auto& dictionaryVector : dictionaryVectors) {
      vectors.emplace_back(VectorMaker::flatten(dictionaryVector));
    }
  } else {
    vectors = dictionaryVectors;
  }

  std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
  hashers.reserve(4);
  for (int i = 0; i < 4; i++) {
    hashers.emplace_back(exec::VectorHasher::create(vectors[i]->type(), i));
  }

  SelectivityVector allRows(size);
  uint64_t multiplier = 1;
  for (int i = 0; i < 4; i++) {
    auto hasher = hashers[i].get();
    raw_vector<uint64_t> result(size);
    hasher->decode(*vectors[i], allRows);
    auto ok = hasher->computeValueIds(allRows, result);
    folly::doNotOptimizeAway(ok);

    multiplier = hasher->enableValueIds(multiplier, 0);
  }
  suspender.dismiss();

  raw_vector<uint64_t> result(size);
  for (int i = 0; i < 10'000; i++) {
    for (int j = 0; j < 4; j++) {
      auto hasher = hashers[j].get();
      auto vector = vectors[j];
      hasher->decode(*vector, allRows);
      bool ok = hasher->computeValueIds(allRows, result);
      folly::doNotOptimizeAway(ok);
    }
  }
}

BENCHMARK(computeValueIdsDictionaryStrings) {
  benchmarkComputeValueIdsForStrings(false);
}

BENCHMARK_RELATIVE(computeValueIdsFlatStrings) {
  benchmarkComputeValueIdsForStrings(true);
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
