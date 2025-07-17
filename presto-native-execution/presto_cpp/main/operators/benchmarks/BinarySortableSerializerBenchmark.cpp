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
#include <numeric>

#include "velox/vector/fuzzer/VectorFuzzer.h"

#include "presto_cpp/main/operators/BinarySortableSerializer.h"

using namespace facebook::velox;
namespace facebook::presto::operators {
namespace {

class BinarySortableSerializerBenchmark {
 public:
  BinarySortableSerializerBenchmark(
      const RowTypePtr& rowType,
      const std::vector<velox::core::SortOrder>& ordering)
      : ordering_(ordering) {
    folly::BenchmarkSuspender suspender;
    data_ = makeData(rowType);
    fields_ = getFields(rowType);
    outputVec_ =
        velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
            velox::VARBINARY(), data_->size(), pool());
    suspender.dismiss();
  }

  void serializeWithStringVectorBuffer() {
    BinarySortableSerializer binarySortableSerializer(
        data_, ordering_, fields_);
    // Create a ResizableVectorBuffer with initial and max capacity.
    velox::StringVectorBuffer buffer(outputVec_.get(), 0, 1 << 20);
    serialize(binarySortableSerializer, data_->size(), &buffer);
    VELOX_CHECK_EQ(outputVec_->size(), data_->size());
  }

  void serializeWithSizeCalculation() {
    BinarySortableSerializer binarySortableSerializer(
        data_, ordering_, fields_);
    const auto bufferSize =
        calculateSizeBatched(binarySortableSerializer, data_->size());
    // Create a ResizableVectorBuffer with bufferSize.
    velox::StringVectorBuffer buffer(outputVec_.get(), bufferSize, bufferSize);
    serialize(binarySortableSerializer, data_->size(), &buffer);
    VELOX_CHECK_EQ(outputVec_->size(), data_->size());
  }

  void calculateSerializedSize() {
    BinarySortableSerializer binarySortableSerializer(
        data_, ordering_, fields_);
    calculateSize(binarySortableSerializer, data_->size());
  }

  void calculateSerializedSizeBatched() {
    BinarySortableSerializer binarySortableSerializer(
        data_, ordering_, fields_);
    calculateSizeBatched(binarySortableSerializer, data_->size());
  }

  memory::MemoryPool* pool() {
    return pool_.get();
  }

 private:
  RowVectorPtr makeData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;
    options.stringLength = 10'000;

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
      const BinarySortableSerializer& binarySortableSerializer,
      vector_size_t numRows,
      velox::StringVectorBuffer* out) {
    for (auto i = 0; i < numRows; ++i) {
      binarySortableSerializer.serialize(i, out);
      out->flushRow(i);
    }
  }

  size_t calculateSize(
      const BinarySortableSerializer& binarySortableSerializer,
      vector_size_t numRows) {
    size_t size = 0;
    for (auto i = 0; i < numRows; ++i) {
      size += binarySortableSerializer.serializedSizeInBytes(i);
    }
    return size;
  }

  size_t calculateSizeBatched(
      const BinarySortableSerializer& binarySortableSerializer,
      vector_size_t numRows) {
    // Create size variables and pointer array for batched call
    std::vector<velox::vector_size_t> sizes(numRows, 0);
    std::vector<velox::vector_size_t*> sizePointers(numRows);
    for (velox::vector_size_t i = 0; i < numRows; ++i) {
      sizePointers[i] = &sizes[i];
    }

    // Create scratch memory for batched size calculation
    velox::Scratch scratch;

    // Call batched size calculation
    binarySortableSerializer.serializedSizeInBytes(
        0, numRows, sizePointers.data(), scratch);

    // Sum up all sizes
    size_t totalSize = 0;
    for (const auto& size : sizes) {
      totalSize += size;
    }
    return totalSize;
  }
  const std::vector<velox::core::SortOrder> ordering_;

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("test")};

  RowVectorPtr data_;
  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields_;
  std::shared_ptr<FlatVector<StringView>> outputVec_;
};

BENCHMARK_DRAW_TEXT("=============Serialize decimal=============");
BENCHMARK(decimalsSizeCalculation) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), DECIMAL(12, 2), DECIMAL(38, 18)}), ordering);

  benchmark.calculateSerializedSize();
}

BENCHMARK_RELATIVE(decimalsSizeCalculationBatched) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), DECIMAL(12, 2), DECIMAL(38, 18)}), ordering);

  benchmark.calculateSerializedSizeBatched();
}

BENCHMARK_COUNTERS(decimalsSerialize, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), DECIMAL(12, 2), DECIMAL(38, 18)}), ordering);

  benchmark.serializeWithStringVectorBuffer();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_COUNTERS_RELATIVE(decimalsSerializeWithSizeCalculation, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), DECIMAL(12, 2), DECIMAL(38, 18)}), ordering);

  benchmark.serializeWithSizeCalculation();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_DRAW_TEXT("=============Serialize string1=============");

BENCHMARK(string1SizeCalculation) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), VARCHAR()}), ordering);

  benchmark.calculateSerializedSize();
}

BENCHMARK_RELATIVE(string1SizeCalculationBatched) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), VARCHAR()}), ordering);

  benchmark.calculateSerializedSizeBatched();
}

BENCHMARK_COUNTERS(string1Serialize, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), VARCHAR()}), ordering);

  benchmark.serializeWithStringVectorBuffer();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_COUNTERS_RELATIVE(string1SerializeWithSizeCalculation, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), VARCHAR()}), ordering);

  benchmark.serializeWithSizeCalculation();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_DRAW_TEXT("=============Serialize string5=============");

BENCHMARK(strings5SizeCalculation) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}),
      ordering);

  benchmark.calculateSerializedSize();
}

BENCHMARK_RELATIVE(strings5SizeCalculationBatched) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}),
      ordering);

  benchmark.calculateSerializedSizeBatched();
}

BENCHMARK_COUNTERS(strings5Serialize, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}),
      ordering);

  benchmark.serializeWithStringVectorBuffer();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_COUNTERS_RELATIVE(strings5SerializeWithSizeCalculation, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR()}),
      ordering);

  benchmark.serializeWithSizeCalculation();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_DRAW_TEXT("=============Serialize array=============");

BENCHMARK(arraysSizeCalculation) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ARRAY(BIGINT())}), ordering);

  benchmark.calculateSerializedSize();
}

BENCHMARK_RELATIVE(arraysSizeCalculationBatched) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ARRAY(BIGINT())}), ordering);

  benchmark.calculateSerializedSizeBatched();
}

BENCHMARK_COUNTERS(arraysSerialize, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ARRAY(BIGINT())}), ordering);

  benchmark.serializeWithStringVectorBuffer();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_COUNTERS_RELATIVE(arraysSerializeWithSizeCalculation, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ARRAY(BIGINT())}), ordering);

  benchmark.serializeWithSizeCalculation();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_DRAW_TEXT("=============Serialize nested array=============");

BENCHMARK(nestedArraysSizeCalculation) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}), ordering);

  benchmark.calculateSerializedSize();
}

BENCHMARK_RELATIVE(nestedArraysSizeCalculationBatched) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}), ordering);

  benchmark.calculateSerializedSizeBatched();
}

BENCHMARK_COUNTERS(nestedArraysSerialize, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}), ordering);

  benchmark.serializeWithStringVectorBuffer();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_COUNTERS_RELATIVE(
    nestedArraysSerializeWithSizeCalculation,
    counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}), ordering);

  benchmark.serializeWithSizeCalculation();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_DRAW_TEXT("=============Serialize struct=============");

BENCHMARK(structsSizeCalculation) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}),
      ordering);

  benchmark.calculateSerializedSize();
}

BENCHMARK_RELATIVE(structsSizeCalculationBatched) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}),
      ordering);

  benchmark.calculateSerializedSizeBatched();
}

BENCHMARK_COUNTERS(structsSerialize, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}),
      ordering);

  benchmark.serializeWithStringVectorBuffer();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
}

BENCHMARK_COUNTERS_RELATIVE(structsSerializeWithSizeCalculation, counters) {
  const auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsFirst),
      velox::core::SortOrder(velox::core::kDescNullsFirst)};

  BinarySortableSerializerBenchmark benchmark(
      ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}),
      ordering);

  benchmark.serializeWithSizeCalculation();
  counters["memUsage"] = benchmark.pool()->stats().usedBytes;
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
