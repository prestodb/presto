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

#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/row/UnsafeRowFast.h"
#include "velox/row/UnsafeRowSerializers.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::row {
namespace {

using namespace facebook::velox::test;

class UnsafeRowFuzzTests : public ::testing::Test {
 public:
  UnsafeRowFuzzTests() {
    clearBuffers();
  }

  void clearBuffers() {
    for (auto& buffer : buffers_) {
      std::memset(buffer, 0, kBufferSize);
    }
  }

  void doTest(
      const RowTypePtr& rowType,
      std::function<std::vector<std::optional<std::string_view>>(
          const RowVectorPtr& data)> serializeFunc) {
    VectorFuzzer::Options opts;
    opts.vectorSize = kNumBuffers;
    opts.nullRatio = 0.1;
    opts.containerHasNulls = false;
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
      clearBuffers();

      auto seed = folly::Random::rand32();

      LOG(INFO) << "seed: " << seed;
      SCOPED_TRACE(fmt::format("seed: {}", seed));

      fuzzer.reSeed(seed);
      const auto& inputVector = fuzzer.fuzzInputRow(rowType);

      // Serialize rowVector into bytes.
      auto serialized = serializeFunc(inputVector);

      // Deserialize previous bytes back to row vector
      VectorPtr outputVector =
          UnsafeRowDeserializer::deserialize(serialized, rowType, pool_.get());

      assertEqualVectors(inputVector, outputVector);
    }
  }

  static constexpr uint64_t kBufferSize = 50 << 10; // 50kb
  static constexpr uint64_t kNumBuffers = 100;

  std::array<char[kBufferSize], kNumBuffers> buffers_{};

  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::addDefaultLeafMemoryPool();
};

TEST_F(UnsafeRowFuzzTests, simpleTypeRoundTripTest) {
  auto rowType = ROW({
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      TIMESTAMP(),
      ROW({VARCHAR(), INTEGER()}),
      ARRAY(INTEGER()),
      ARRAY(INTEGER()),
      MAP(VARCHAR(), ARRAY(INTEGER())),
  });

  doTest(rowType, [&](const RowVectorPtr& data) {
    std::vector<std::optional<std::string_view>> serialized;
    serialized.reserve(data->size());
    for (auto i = 0; i < data->size(); ++i) {
      UnsafeRowSerializer::preloadVector(data);
      auto rowSize = UnsafeRowSerializer::serialize(data, buffers_[i], i);
      VELOX_CHECK(rowSize.has_value())
      VELOX_CHECK_LE(rowSize.value(), kBufferSize);

      auto rowSizeMeasured = UnsafeRowSerializer::getSizeRow(data.get(), i);
      EXPECT_EQ(rowSize.value(), rowSizeMeasured);
      serialized.push_back(std::string_view(buffers_[i], rowSize.value()));
    }
    return serialized;
  });
}

TEST_F(UnsafeRowFuzzTests, fast) {
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
      // Nested arrays.
      ARRAY(ARRAY(INTEGER())),
      ARRAY(ARRAY(BIGINT())),
      ARRAY(ARRAY(VARCHAR())),
      // Maps.
      MAP(BIGINT(), REAL()),
      MAP(BIGINT(), BIGINT()),
      MAP(BIGINT(), VARCHAR()),
      MAP(INTEGER(), MAP(BIGINT(), DOUBLE())),
      MAP(VARCHAR(), BOOLEAN()),
      MAP(INTEGER(), MAP(BIGINT(), ARRAY(REAL()))),
      // Timestamp and date types.
      TIMESTAMP(),
      DATE(),
      ARRAY(TIMESTAMP()),
      ARRAY(DATE()),
      MAP(DATE(), ARRAY(TIMESTAMP())),
      // Structs.
      ROW({BOOLEAN(), INTEGER(), TIMESTAMP(), VARCHAR(), ARRAY(BIGINT())}),
      ROW(
          {BOOLEAN(),
           ROW({INTEGER(), TIMESTAMP()}),
           VARCHAR(),
           ARRAY(BIGINT())}),
      ARRAY({ROW({BIGINT(), VARCHAR()})}),
      MAP(BIGINT(), ROW({BOOLEAN(), TINYINT(), REAL()})),
  });

  doTest(rowType, [&](const RowVectorPtr& data) {
    std::vector<std::optional<std::string_view>> serialized;
    serialized.reserve(data->size());

    UnsafeRowFast fast(data);
    for (auto i = 0; i < data->size(); ++i) {
      UnsafeRowSerializer::preloadVector(data);
      auto rowSize = fast.serialize(i, buffers_[i]);
      VELOX_CHECK_LE(rowSize, kBufferSize);

      EXPECT_EQ(rowSize, fast.rowSize(i)) << i << ", " << data->toString(i);

      auto rowSizeMeasured = UnsafeRowSerializer::getSizeRow(data.get(), i);
      EXPECT_EQ(rowSize, rowSizeMeasured);
      serialized.push_back(std::string_view(buffers_[i], rowSize));
    }
    return serialized;
  });
}

} // namespace
} // namespace facebook::velox::row
