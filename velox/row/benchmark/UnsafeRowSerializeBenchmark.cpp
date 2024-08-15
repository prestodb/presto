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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/ContainerRowSerde.h"
#include "velox/row/CompactRow.h"
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::row {
namespace {

class SerializeBenchmark {
 public:
  void serializeUnsafe(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    suspender.dismiss();

    UnsafeRowFast fast(data);
    auto totalSize = computeTotalSize(fast, rowType, data->size());
    auto buffer = AlignedBuffer::allocate<char>(totalSize, pool());
    auto serialized = serialize(fast, data->size(), buffer);
    VELOX_CHECK_EQ(serialized.size(), data->size());
  }

  void deserializeUnsafe(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    UnsafeRowFast fast(data);
    auto totalSize = computeTotalSize(fast, rowType, data->size());
    auto buffer = AlignedBuffer::allocate<char>(totalSize, pool());
    auto serialized = serialize(fast, data->size(), buffer);
    suspender.dismiss();

    auto copy = UnsafeRowDeserializer::deserialize(serialized, rowType, pool());
    VELOX_CHECK_EQ(copy->size(), data->size());
  }

  void serializeCompact(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    suspender.dismiss();

    CompactRow compact(data);
    auto totalSize = computeTotalSize(compact, rowType, data->size());
    auto buffer = AlignedBuffer::allocate<char>(totalSize, pool());
    auto serialized = serialize(compact, data->size(), buffer);
    VELOX_CHECK_EQ(serialized.size(), data->size());
  }

  void deserializeCompact(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    CompactRow compact(data);
    auto totalSize = computeTotalSize(compact, rowType, data->size());
    auto buffer = AlignedBuffer::allocate<char>(totalSize, pool());
    auto serialized = serialize(compact, data->size(), buffer);
    suspender.dismiss();

    auto copy = CompactRow::deserialize(serialized, rowType, pool());
    VELOX_CHECK_EQ(copy->size(), data->size());
  }

  void serializeContainer(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    suspender.dismiss();

    HashStringAllocator allocator(pool());
    auto position = serialize(data, allocator);
    VELOX_CHECK_NOT_NULL(position.header);
  }

  void deserializeContainer(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);

    HashStringAllocator allocator(pool());
    auto position = serialize(data, allocator);
    VELOX_CHECK_NOT_NULL(position.header);
    suspender.dismiss();

    auto copy = BaseVector::create(rowType, data->size(), pool());

    auto in = HashStringAllocator::prepareRead(position.header);
    for (auto i = 0; i < data->size(); ++i) {
      exec::ContainerRowSerde::deserialize(*in, i, copy.get());
    }

    VELOX_CHECK_EQ(copy->size(), data->size());
  }

 private:
  RowVectorPtr makeData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;

    const auto seed = 1; // For reproducibility.
    VectorFuzzer fuzzer(options, pool_.get(), seed);

    return fuzzer.fuzzInputRow(rowType);
  }

  size_t computeTotalSize(
      UnsafeRowFast& unsafeRow,
      const RowTypePtr& rowType,
      vector_size_t numRows) {
    size_t totalSize = 0;
    if (auto fixedRowSize = UnsafeRowFast::fixedRowSize(rowType)) {
      totalSize += fixedRowSize.value() * numRows;
    } else {
      for (auto i = 0; i < numRows; ++i) {
        auto rowSize = unsafeRow.rowSize(i);
        totalSize += rowSize;
      }
    }
    return totalSize;
  }

  std::vector<std::optional<std::string_view>> serialize(
      UnsafeRowFast& unsafeRow,
      vector_size_t numRows,
      BufferPtr& buffer) {
    std::vector<std::optional<std::string_view>> serialized;
    auto rawBuffer = buffer->asMutable<char>();

    size_t offset = 0;
    for (auto i = 0; i < numRows; ++i) {
      auto rowSize = unsafeRow.serialize(i, rawBuffer + offset);
      serialized.push_back(std::string_view(rawBuffer + offset, rowSize));
      offset += rowSize;
    }

    VELOX_CHECK_EQ(buffer->size(), offset);
    return serialized;
  }

  size_t computeTotalSize(
      CompactRow& compactRow,
      const RowTypePtr& rowType,
      vector_size_t numRows) {
    size_t totalSize = 0;
    if (auto fixedRowSize = CompactRow::fixedRowSize(rowType)) {
      totalSize += fixedRowSize.value() * numRows;
    } else {
      for (auto i = 0; i < numRows; ++i) {
        auto rowSize = compactRow.rowSize(i);
        totalSize += rowSize;
      }
    }
    return totalSize;
  }

  std::vector<std::string_view>
  serialize(CompactRow& compactRow, vector_size_t numRows, BufferPtr& buffer) {
    std::vector<std::string_view> serialized;
    auto rawBuffer = buffer->asMutable<char>();

    size_t offset = 0;
    for (auto i = 0; i < numRows; ++i) {
      auto rowSize = compactRow.serialize(i, rawBuffer + offset);
      serialized.push_back(std::string_view(rawBuffer + offset, rowSize));
      offset += rowSize;
    }

    VELOX_CHECK_EQ(buffer->size(), offset);
    return serialized;
  }

  HashStringAllocator::Position serialize(
      const RowVectorPtr& data,
      HashStringAllocator& allocator) {
    ByteOutputStream out(&allocator);
    auto position = allocator.newWrite(out);
    const exec::ContainerRowSerdeOptions options{};
    for (auto i = 0; i < data->size(); ++i) {
      exec::ContainerRowSerde::serialize(*data, i, out, options);
    }
    allocator.finishWrite(out, 0);
    return position;
  }

  memory::MemoryPool* pool() {
    return pool_.get();
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

#define SERDE_BENCHMARKS(name, rowType)      \
  BENCHMARK(unsafe_serialize_##name) {       \
    SerializeBenchmark benchmark;            \
    benchmark.serializeUnsafe(rowType);      \
  }                                          \
                                             \
  BENCHMARK(compact_serialize_##name) {      \
    SerializeBenchmark benchmark;            \
    benchmark.serializeCompact(rowType);     \
  }                                          \
                                             \
  BENCHMARK(container_serialize_##name) {    \
    SerializeBenchmark benchmark;            \
    benchmark.serializeContainer(rowType);   \
  }                                          \
                                             \
  BENCHMARK(unsafe_deserialize_##name) {     \
    SerializeBenchmark benchmark;            \
    benchmark.deserializeUnsafe(rowType);    \
  }                                          \
                                             \
  BENCHMARK(compact_deserialize_##name) {    \
    SerializeBenchmark benchmark;            \
    benchmark.deserializeCompact(rowType);   \
  }                                          \
                                             \
  BENCHMARK(container_deserialize_##name) {  \
    SerializeBenchmark benchmark;            \
    benchmark.deserializeContainer(rowType); \
  }

SERDE_BENCHMARKS(
    fixedWidth5,
    ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}));

SERDE_BENCHMARKS(
    fixedWidth10,
    ROW({
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        DOUBLE(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
    }));

SERDE_BENCHMARKS(
    fixedWidth20,
    ROW({
        BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
        BIGINT(), BIGINT(), BIGINT(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(),
        DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), BIGINT(), BIGINT(),
    }));

BENCHMARK(decimalsSerialize) {
  SerializeBenchmark benchmark;
  benchmark.serializeUnsafe(ROW({BIGINT(), DECIMAL(12, 2), DECIMAL(38, 18)}));
}

BENCHMARK(decimalsDeserialize) {
  SerializeBenchmark benchmark;
  benchmark.deserializeUnsafe(ROW({BIGINT(), DECIMAL(12, 2), DECIMAL(38, 18)}));
}

SERDE_BENCHMARKS(strings1, ROW({BIGINT(), VARCHAR()}));

SERDE_BENCHMARKS(
    strings5,
    ROW({
        BIGINT(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
    }));

SERDE_BENCHMARKS(arrays, ROW({BIGINT(), ARRAY(BIGINT())}));

SERDE_BENCHMARKS(nestedArrays, ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}));

SERDE_BENCHMARKS(maps, ROW({BIGINT(), MAP(BIGINT(), REAL())}));

SERDE_BENCHMARKS(
    structs,
    ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}));

} // namespace
} // namespace facebook::velox::row

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  facebook::velox::memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
