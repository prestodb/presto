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

#include "velox/vector/tests/VectorMaker.h"

namespace facebook::velox {
namespace {
BufferPtr makeIndices(
    vector_size_t size,
    memory::MemoryPool* pool,
    std::function<vector_size_t(vector_size_t)> indexAt) {
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (vector_size_t i = 0; i < size; i++) {
    rawIndices[i] = indexAt(i);
  }

  return indices;
}

size_t runBenchmark(
    const VectorPtr& data,
    const SelectivityVector& selected,
    const TypePtr& type,
    memory::MemoryPool* pool,
    vector_size_t copyBatchSize) {
  VectorPtr result;
  folly::BenchmarkSuspender suspender;
  BaseVector::ensureWritable(selected, type, pool, result);
  suspender.dismiss();

  size_t numIters = 100;
  vector_size_t size = data->size();
  for (auto i = 0; i < numIters; i++) {
    BaseVector::prepareForReuse(result, size);
    for (int j = 0; j < data->size(); j += copyBatchSize) {
      result->copy(data.get(), j, j, copyBatchSize);
    }
  }

  return size * numIters;
}

size_t runBenchmark(
    const VectorPtr& data,
    const SelectivityVector& selected,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  return runBenchmark(data, selected, type, pool, data->size());
}

BENCHMARK_MULTI(copyArray) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto arrayVector = vectorMaker.arrayVector<int32_t>(
      size,
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; });
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(arrayVector, selected, ARRAY(INTEGER()), pool.get());
}

BENCHMARK_MULTI(copyArrayWithNulls) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto arrayVector = vectorMaker.arrayVector<int32_t>(
      size,
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; },
      [](auto row) { return row % 13 == 0; });
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(arrayVector, selected, ARRAY(INTEGER()), pool.get());
}

BENCHMARK_MULTI(copyArrayDictionaryEncoded) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  const vector_size_t numElements = size / 10 * 45; // 0 + 1 + 2 + ... + 9 = 45
  auto elements = vectorMaker.flatVector<int32_t>(
      numElements, [](auto row) { return row % 23; });
  auto indices = makeIndices(numElements, pool.get(), [numElements](auto row) {
    return (row * 13) % numElements; // 13 and numElements are coprime so this
                                     // should shuffle them.
  });
  auto elementsDictionaryEncoded = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, numElements, elements);
  std::vector<vector_size_t> offsets;
  vector_size_t offset = 0;
  for (int i = 0; i < size + 1; i++) {
    offsets.push_back(offset);
    offset += i % 10;
  }
  auto arrayVector =
      vectorMaker.arrayVector(offsets, elementsDictionaryEncoded);

  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(arrayVector, selected, ARRAY(INTEGER()), pool.get());
}

BENCHMARK_MULTI(copyArrayOneAtATime) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto arrayVector = vectorMaker.arrayVector<int32_t>(
      size,
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; });
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(arrayVector, selected, ARRAY(INTEGER()), pool.get(), 1);
}

BENCHMARK_MULTI(copyArrayTwoAtATime) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto arrayVector = vectorMaker.arrayVector<int32_t>(
      size,
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; });
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(arrayVector, selected, ARRAY(INTEGER()), pool.get(), 2);
}

BENCHMARK_MULTI(copyArrayOfVarchar) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 50'000;
  auto offsets = allocateOffsets(size + 1, pool.get());
  auto sizes = allocateSizes(size, pool.get());
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();
  for (int i = 0; i < size; ++i) {
    rawSizes[i] = i % 10;
    // Leave gap between the ranges so we don't merge them.
    rawOffsets[i + 1] = rawOffsets[i] + rawSizes[i] + 1;
  }
  char string[51];
  std::fill(std::begin(string), std::prev(std::end(string)), 'x');
  *std::prev(std::end(string)) = '\0';
  auto elements = vectorMaker.flatVector<StringView>(
      rawOffsets[size], [&](auto) { return string; });
  auto type = ARRAY(VARCHAR());
  auto arrayVector = std::make_shared<ArrayVector>(
      pool.get(), type, nullptr, size, offsets, sizes, elements);
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(arrayVector, selected, type, pool.get());
}

BENCHMARK_MULTI(copyArrayOfArray) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto offsets = allocateOffsets(size + 1, pool.get());
  auto sizes = allocateSizes(size, pool.get());
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();
  for (int i = 0; i < size; ++i) {
    rawSizes[i] = i % 10;
    // Leave gap between the ranges so we don't merge them.
    rawOffsets[i + 1] = rawOffsets[i] + rawSizes[i] + 1;
  }
  auto elements = vectorMaker.arrayVector<int32_t>(
      rawOffsets[size],
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; });
  auto type = ARRAY(ARRAY(INTEGER()));
  auto arrayVector = std::make_shared<ArrayVector>(
      pool.get(), type, nullptr, size, offsets, sizes, elements);
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(arrayVector, selected, type, pool.get());
}

BENCHMARK_MULTI(copyMap) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto mapVector = vectorMaker.mapVector<int32_t, int32_t>(
      size,
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; },
      [](auto row) { return row % 37; });
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(
      mapVector, selected, MAP(INTEGER(), INTEGER()), pool.get());
}

BENCHMARK_MULTI(copyMapWithNulls) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto mapVector = vectorMaker.mapVector<int32_t, int32_t>(
      size,
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; },
      [](auto row) { return row % 37; },
      [](auto row) { return row % 13 == 0; });
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(
      mapVector, selected, MAP(INTEGER(), INTEGER()), pool.get());
}

BENCHMARK_MULTI(copyMapDictionaryEncoded) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  const vector_size_t numElements = size / 10 * 45; // 0 + 1 + 2 + ... + 9 = 45
  auto values = vectorMaker.flatVector<int32_t>(
      numElements, [](auto row) { return row % 23; });
  auto indices = makeIndices(numElements, pool.get(), [numElements](auto row) {
    return (row * 13) % numElements; // 13 and numElements are coprime so this
                                     // should shuffle them.
  });
  auto keysDictionaryEncoded = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, numElements, values);
  auto valuesDictionaryEncoded = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, numElements, values);
  std::vector<vector_size_t> offsets;
  vector_size_t offset = 0;
  for (int i = 0; i < size + 1; i++) {
    offsets.push_back(offset);
    offset += i % 10;
  }
  auto mapVector = vectorMaker.mapVector(
      offsets, keysDictionaryEncoded, valuesDictionaryEncoded);

  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(
      mapVector, selected, MAP(INTEGER(), INTEGER()), pool.get());
}

BENCHMARK_MULTI(copyMapOneAtATime) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto mapVector = vectorMaker.mapVector<int32_t, int32_t>(
      size,
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; },
      [](auto row) { return row % 37; });
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(
      mapVector, selected, MAP(INTEGER(), INTEGER()), pool.get(), 1);
}

BENCHMARK_MULTI(copyMapTwoAtATime) {
  folly::BenchmarkSuspender suspender;
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  test::VectorMaker vectorMaker{pool.get()};

  const vector_size_t size = 1'000;
  auto mapVector = vectorMaker.mapVector<int32_t, int32_t>(
      size,
      [](auto row) { return row % 10; },
      [](auto row) { return row % 23; },
      [](auto row) { return row % 37; });
  SelectivityVector selected(size);
  suspender.dismiss();

  return runBenchmark(
      mapVector, selected, MAP(INTEGER(), INTEGER()), pool.get(), 2);
}
} // namespace
} // namespace facebook::velox

int main(int /*argc*/, char** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}
