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
#include <random>

#include "velox/row/UnsafeRowBatchDeserializer.h"
#include "velox/row/UnsafeRowDeserializer.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/VectorMaker.h"

namespace facebook::spark::benchmarks {
namespace {
using namespace facebook::velox;
using namespace facebook::velox::row;
using facebook::velox::test::VectorMaker;

class Deserializer {
 public:
  virtual ~Deserializer() = default;
  virtual void deserialize(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type) = 0;
};

class UnsaferowDeserializer : public Deserializer {
 public:
  UnsaferowDeserializer() {}

  void deserialize(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type) override {
    UnsafeRowDynamicVectorDeserializer::deserializeComplex(
        data, type, pool_.get());
  }

 private:
  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
  ;
};

class UnsaferowBatchDeserializer : public Deserializer {
 public:
  UnsaferowBatchDeserializer() {}

  void deserialize(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type) override {
    UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
        data, type, pool_.get());
  }

 private:
  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
};

class BenchmarkHelper {
 public:
  std::tuple<std::vector<std::optional<std::string_view>>, TypePtr>
  randomUnsaferows(int nFields, int nRows, bool stringOnly) {
    RowTypePtr rowType;
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(nFields);
    types.reserve(nFields);
    for (int32_t i = 0; i < nFields; ++i) {
      names.push_back("");
      if (stringOnly) {
        types.push_back(VARCHAR());
      } else {
        auto idx = folly::Random::rand32() % allTypes_.size();
        types.push_back(allTypes_[idx]);
      }
    }
    rowType =
        TypeFactory<TypeKind::ROW>::create(std::move(names), std::move(types));

    VectorFuzzer::Options opts;
    opts.vectorSize = 1;
    opts.nullChance = 10;
    opts.stringVariableLength = true;
    opts.stringLength = 20;
    // Spark uses microseconds to store timestamp
    opts.useMicrosecondPrecisionTimestamp = true;

    auto seed = folly::Random::rand32();
    VectorFuzzer fuzzer(opts, pool_.get(), seed);
    const auto& inputVector = fuzzer.fuzzRow(rowType);
    std::vector<std::optional<std::string_view>> results;
    results.reserve(nRows);
    // Serialize rowVector into bytes.
    for (int32_t i = 0; i < nRows; ++i) {
      BufferPtr bufferPtr =
          AlignedBuffer::allocate<char>(1024, pool_.get(), true);
      char* buffer = bufferPtr->asMutable<char>();
      auto rowSize = UnsafeRowDynamicSerializer::serialize(
          rowType, inputVector, buffer, /*idx=*/0);
      results.push_back(std::string_view(buffer, rowSize.value()));
    }
    return {results, rowType};
  }

 private:
  std::vector<TypePtr> allTypes_{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      TIMESTAMP(),
      ARRAY(INTEGER()),
      MAP(VARCHAR(), ARRAY(INTEGER())),
      ROW({INTEGER()})};

  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
};

int deserialize(
    int nIters,
    int nFields,
    int nRows,
    bool variable,
    std::unique_ptr<Deserializer> deserializer) {
  folly::BenchmarkSuspender suspender;
  BenchmarkHelper helper;
  auto [data, rowType] = helper.randomUnsaferows(nFields, nRows, variable);
  suspender.dismiss();

  for (int i = 0; i < nIters; i++) {
    deserializer->deserialize(data, rowType);
  }

  return nIters * nFields * nRows;
}

BENCHMARK_NAMED_PARAM_MULTI(
    deserialize,
    row_10_100k_string_only,
    10,
    100000,
    true,
    std::make_unique<UnsaferowDeserializer>());
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(
    deserialize,
    batch_10_100k_string_only,
    10,
    100000,
    true,
    std::make_unique<UnsaferowBatchDeserializer>());

BENCHMARK_NAMED_PARAM_MULTI(
    deserialize,
    row_100_100k_string_only,
    100,
    100000,
    true,
    std::make_unique<UnsaferowDeserializer>());
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(
    deserialize,
    batch_100_100k_string_only,
    100,
    100000,
    true,
    std::make_unique<UnsaferowBatchDeserializer>());

BENCHMARK_NAMED_PARAM_MULTI(
    deserialize,
    row_10_100k_all_types,
    10,
    100000,
    true,
    std::make_unique<UnsaferowDeserializer>());
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(
    deserialize,
    batch_10_100k_all_types,
    10,
    100000,
    false,
    std::make_unique<UnsaferowBatchDeserializer>());

BENCHMARK_NAMED_PARAM_MULTI(
    deserialize,
    row_100_100k_all_types,
    100,
    100000,
    false,
    std::make_unique<UnsaferowDeserializer>());
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(
    deserialize,
    batch_100_100k_all_types,
    100,
    100000,
    false,
    std::make_unique<UnsaferowBatchDeserializer>());

} // namespace
} // namespace facebook::spark::benchmarks

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
