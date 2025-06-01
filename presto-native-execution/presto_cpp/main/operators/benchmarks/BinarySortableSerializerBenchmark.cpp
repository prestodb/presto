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

#include "velox/vector/fuzzer/VectorFuzzer.h"

#include "presto_cpp/main/operators/BinarySortableSerializer.h"

using namespace facebook::velox;
namespace facebook::presto::operators {
namespace {
class BinarySortableSerializerBenchmark {
 public:
  void serializeWithStringVectorBuffer(
      const RowTypePtr& rowType,
      const std::vector<velox::core::SortOrder>& ordering) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    auto fields = getFields(rowType);
    suspender.dismiss();

    BinarySortableSerializer binarySortableSerializer(data, ordering, fields);

    auto vector =
        velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
            velox::VARBINARY(), data->size(), pool());
    // Create a ResizableVectorBuffer with initial and max capacity.
    velox::StringVectorBuffer buffer(vector.get(), 1024, 1 << 20);
    serialize(binarySortableSerializer, data->size(), &buffer);
    VELOX_CHECK_EQ(vector->size(), data->size());
  }

 private:
  RowVectorPtr makeData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;

    const auto seed = 1; // For reproducibility.
    VectorFuzzer fuzzer(options, pool_.get(), seed);

    return fuzzer.fuzzInputRow(rowType);
  }

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
  getFields(const RowTypePtr& rowType) {
    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fieldAccessExprs;
    for (const auto& fieldName : rowType->names()) {
      auto fieldExpr = std::make_shared<velox::core::FieldAccessTypedExpr>(
          rowType->findChild(fieldName), fieldName);
      fieldAccessExprs.push_back(fieldExpr);
    }
    return fieldAccessExprs;
  }

  void serialize(
      BinarySortableSerializer binarySortableSerializer,
      vector_size_t numRows,
      velox::StringVectorBuffer* out) {
    for (auto i = 0; i < numRows; ++i) {
      binarySortableSerializer.serialize(i, out);
      out->flushRow(i);
    }
  }

  memory::MemoryPool* pool() {
    return pool_.get();
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

BENCHMARK(decimalsSerialize) {
  BinarySortableSerializerBenchmark benchmark;

  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  benchmark.serializeWithStringVectorBuffer(
      ROW({BIGINT(), DECIMAL(12, 2), DECIMAL(38, 18)}), ordering);
}

BENCHMARK(string1Serialize) {
  BinarySortableSerializerBenchmark benchmark;

  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  benchmark.serializeWithStringVectorBuffer(
      ROW({BIGINT(), VARCHAR()}), ordering);
}

BENCHMARK(strings5Serialize) {
  BinarySortableSerializerBenchmark benchmark;

  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  benchmark.serializeWithStringVectorBuffer(
      ROW({BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}),
      ordering);
}

BENCHMARK(arraysSerialize) {
  BinarySortableSerializerBenchmark benchmark;

  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  benchmark.serializeWithStringVectorBuffer(
      ROW({BIGINT(), ARRAY(BIGINT())}), ordering);
}

BENCHMARK(nestedArraysSerialize) {
  BinarySortableSerializerBenchmark benchmark;

  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  benchmark.serializeWithStringVectorBuffer(
      ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}), ordering);
}

BENCHMARK(structsSerialize) {
  BinarySortableSerializerBenchmark benchmark;

  auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  benchmark.serializeWithStringVectorBuffer(
      ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}),
      ordering);
}

} // namespace
} // namespace facebook::presto::operators

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
