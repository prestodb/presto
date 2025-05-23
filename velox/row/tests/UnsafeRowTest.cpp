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

#include <gtest/gtest.h>

#include <folly/Random.h>
#include <folly/init/Init.h>

#include "velox/row/UnsafeRowFast.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::row {
namespace {

using namespace facebook::velox::test;

class UnsafeRowTest : public ::testing::Test, public VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void doFuzzTest(const RowTypePtr& rowType) {
    VectorFuzzer::Options opts;
    opts.vectorSize = 100;
    opts.nullRatio = 0.1;
    opts.dictionaryHasNulls = false;
    opts.stringVariableLength = true;
    opts.stringLength = 20;
    opts.containerVariableLength = true;
    opts.complexElementsMaxSize = 10'000;

    // Spark uses microseconds to store timestamp
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMicroSeconds,
    opts.containerLength = 10;

    VectorFuzzer fuzzer(opts, pool_.get());

    const auto iterations = 200;
    for (size_t i = 0; i < iterations; ++i) {
      auto seed = folly::Random::rand32();

      LOG(INFO) << "seed: " << seed;
      SCOPED_TRACE(fmt::format("seed: {}", seed));

      fuzzer.reSeed(seed);
      const auto& inputVector = fuzzer.fuzzInputRow(rowType);
      testRoundTrip(inputVector);

      if (Test::HasFailure()) {
        break;
      }
    }
  }

  void testRoundTrip(const RowVectorPtr& data) {
    SCOPED_TRACE(data->toString());

    auto rowType = asRowType(data->type());
    auto numRows = data->size();
    std::vector<size_t> rowSize(numRows);
    std::vector<size_t> offsets(numRows);

    UnsafeRowFast row(data);

    size_t totalSize = 0;
    if (auto fixedRowSize = UnsafeRowFast::fixedRowSize(rowType)) {
      totalSize = fixedRowSize.value() * numRows;
      for (auto i = 0; i < numRows; ++i) {
        rowSize[i] = fixedRowSize.value();
        offsets[i] = fixedRowSize.value() * i;
      }
    } else {
      for (auto i = 0; i < numRows; ++i) {
        rowSize[i] = row.rowSize(i);
        offsets[i] = totalSize;
        totalSize += rowSize[i];
      }
    }

    std::vector<vector_size_t> rows(numRows);
    std::iota(rows.begin(), rows.end(), 0);
    std::vector<vector_size_t> serializedRowSizes(numRows);
    std::vector<vector_size_t*> serializedRowSizesPtr(numRows);
    for (auto i = 0; i < numRows; ++i) {
      serializedRowSizesPtr[i] = &serializedRowSizes[i];
    }
    row.serializedRowSizes(
        folly::Range(rows.data(), numRows), serializedRowSizesPtr.data());
    for (auto i = 0; i < numRows; ++i) {
      // The serialized row includes the size of the row.
      ASSERT_EQ(serializedRowSizes[i], row.rowSize(i) + sizeof(uint32_t));
    }

    BufferPtr buffer = AlignedBuffer::allocate<char>(totalSize, pool_.get(), 0);
    auto* rawBuffer = buffer->asMutable<char>();
    size_t offset = 0;
    std::vector<char*> serialized;
    for (auto i = 0; i < numRows; ++i) {
      auto size = row.serialize(i, rawBuffer + offset);
      serialized.push_back(rawBuffer + offset);
      offset += size;

      VELOX_CHECK_EQ(size, row.rowSize(i), "Row {}: {}", i, data->toString(i));
    }
    VELOX_CHECK_EQ(offset, totalSize);

    VectorPtr outputVector =
        UnsafeRowFast::deserialize(serialized, rowType, pool_.get());
    assertEqualVectors(data, outputVector);
  }

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
};

TEST_F(UnsafeRowTest, fuzz) {
  auto rowType = ROW({
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      VARCHAR(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      UNKNOWN(),
      DECIMAL(20, 2),
      DECIMAL(12, 4),
      // Arrays.
      ARRAY(BOOLEAN()),
      ARRAY(TINYINT()),
      ARRAY(SMALLINT()),
      ARRAY(INTEGER()),
      ARRAY(BIGINT()),
      ARRAY(REAL()),
      ARRAY(DOUBLE()),
      ARRAY(VARCHAR()),
      ARRAY(VARBINARY()),
      ARRAY(UNKNOWN()),
      ARRAY(DECIMAL(20, 2)),
      ARRAY(DECIMAL(12, 4)),
      // Nested arrays.
      ARRAY(ARRAY(INTEGER())),
      ARRAY(ARRAY(BIGINT())),
      ARRAY(ARRAY(VARCHAR())),
      ARRAY(ARRAY(UNKNOWN())),
      // Maps.
      MAP(BIGINT(), REAL()),
      MAP(BIGINT(), BIGINT()),
      MAP(BIGINT(), VARCHAR()),
      MAP(BIGINT(), DECIMAL(20, 2)),
      MAP(BIGINT(), DECIMAL(12, 4)),
      MAP(INTEGER(), MAP(BIGINT(), DOUBLE())),
      MAP(VARCHAR(), BOOLEAN()),
      MAP(INTEGER(), MAP(BIGINT(), ARRAY(REAL()))),
      MAP(INTEGER(), ROW({ARRAY(INTEGER()), INTEGER()})),
      // Timestamp and date types.
      TIMESTAMP(),
      DATE(),
      ARRAY(TIMESTAMP()),
      ARRAY(DATE()),
      MAP(DATE(), ARRAY(TIMESTAMP())),
      // Structs.
      ROW(
          {BOOLEAN(),
           INTEGER(),
           TIMESTAMP(),
           DECIMAL(20, 2),
           VARCHAR(),
           ARRAY(BIGINT())}),
      ROW(
          {BOOLEAN(),
           ROW({INTEGER(), TIMESTAMP()}),
           VARCHAR(),
           ARRAY(BIGINT())}),
      ARRAY({ROW({BIGINT(), VARCHAR()})}),
      MAP(BIGINT(), ROW({BOOLEAN(), TINYINT(), REAL()})),
  });

  doFuzzTest(rowType);
}

TEST_F(UnsafeRowTest, nestedMaps) {
  auto innerMaps = makeRowVector(
      {makeNullableArrayVector<int64_t>({
           {{1, 2, std::nullopt, 3}},
           {{4, 5}},
           {{1}},
           std::nullopt,
           {{3, 2, 4, 5}},
           {{6}},
       }),
       makeNullableFlatVector<int32_t>({1, 2, 3, std::nullopt, 5, 6})},
      [](auto row) { return row == 3; });
  auto keys = makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6});
  auto values = makeMapVector({0, 2, 2, 3, 4, 5}, keys, innerMaps, {1});
  auto data = makeRowVector({values});
  testRoundTrip(data);
}

} // namespace
} // namespace facebook::velox::row
