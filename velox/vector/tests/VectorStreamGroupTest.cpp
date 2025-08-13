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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cstdint>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"
#include "velox/vector/VectorStream.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox {
namespace {
class VectorStreamGroupTest
    : public testing::Test,
      public velox::test::VectorTestBase,
      public testing::WithParamInterface<VectorSerde::Kind> {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    if (isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
      deregisterNamedVectorSerde(VectorSerde::Kind::kPresto);
    }
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();

    if (isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
      deregisterNamedVectorSerde(VectorSerde::Kind::kCompactRow);
    }
    serializer::CompactRowVectorSerde::registerNamedVectorSerde();

    if (isRegisteredNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)) {
      deregisterNamedVectorSerde(VectorSerde::Kind::kUnsafeRow);
    }
    serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
  }

  std::unique_ptr<ByteInputStream> prepareInput(std::string& string) {
    const int32_t size = string.size();
    std::vector<ByteRange> ranges;
    for (int32_t i = 0; i < 10; ++i) {
      int32_t start = i * (size / 10);
      int32_t end = (i == 9) ? size : (i + 1) * (size / 10);
      ranges.emplace_back();
      ranges.back().buffer = reinterpret_cast<uint8_t*>(&string[start]);
      ranges.back().size = end - start;
      ranges.back().position = 0;
    }
    return std::make_unique<BufferInputStream>(std::move(ranges));
  }
};

TEST_P(VectorStreamGroupTest, clear) {
  const auto dataType = ROW({BIGINT()});
  const int numBatches = 10;
  const int vectorSize = 100;
  VectorFuzzer::Options opts;
  opts.vectorSize = vectorSize;
  VectorFuzzer fuzzer(opts, pool());
  std::vector<RowVectorPtr> inputVectors;
  inputVectors.reserve(10);
  for (int i = 0; i < numBatches; ++i) {
    inputVectors.push_back(fuzzer.fuzzInputRow(dataType));
  }
  VectorStreamGroup vectorGroup(pool(), getNamedVectorSerde(GetParam()));
  vectorGroup.createStreamTree(dataType, numBatches * vectorSize);
  for (int i = 0; i < numBatches; ++i) {
    vectorGroup.append(inputVectors[i]);
  }
  std::stringstream output;
  OStreamOutputStream outputStream(&output);
  vectorGroup.flush(&outputStream);
  vectorGroup.clear();
  if (GetParam() == VectorSerde::Kind::kPresto) {
    // We expect two pages for header after clear.
    ASSERT_EQ(
        vectorGroup.size(),
        memory::AllocationTraits::pageBytes(
            vectorGroup.testingAllocationQuantum()));
  } else {
    ASSERT_EQ(vectorGroup.size(), 0);
  }
  auto outputStr = output.str();
  RowVectorPtr resultRow;
  auto deserializeInput = prepareInput(outputStr);
  VectorStreamGroup::read(
      deserializeInput.get(),
      pool(),
      dataType,
      getNamedVectorSerde(GetParam()),
      &resultRow,
      nullptr);
  ASSERT_EQ(resultRow->size(), numBatches * vectorSize);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    VectorStreamGroupTest,
    VectorStreamGroupTest,
    testing::ValuesIn(
        {VectorSerde::Kind::kPresto,
         VectorSerde::Kind::kCompactRow,
         VectorSerde::Kind::kUnsafeRow}));
} // namespace
} // namespace facebook::velox
