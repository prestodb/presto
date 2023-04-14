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
#include "velox/row/UnsafeRowSerializers.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
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

  static constexpr uint64_t kBufferSize = 20 << 10; // 20k

  std::array<char[kBufferSize], 100> buffers_{};

  std::shared_ptr<memory::MemoryPool> pool_ = memory::getDefaultMemoryPool();
};

TEST_F(UnsafeRowFuzzTests, simpleTypeRoundTripTest) {
  auto rowType = ROW(
      {BOOLEAN(),
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
       MAP(VARCHAR(), ARRAY(INTEGER()))});

  VectorFuzzer::Options opts;
  opts.vectorSize = 100;
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

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  SCOPED_TRACE(fmt::format("seed: {}", seed));
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  const auto iterations = 1000;
  for (size_t i = 0; i < iterations; ++i) {
    clearBuffers();

    const auto& inputVector = fuzzer.fuzzInputRow(rowType);
    // Serialize rowVector into bytes.
    std::vector<std::optional<std::string_view>> serialized;
    serialized.reserve(inputVector->size());
    for (auto j = 0; j < inputVector->size(); ++j) {
      UnsafeRowSerializer::preloadVector(inputVector);
      auto rowSize =
          UnsafeRowSerializer::serialize(inputVector, buffers_[j], j);

      auto rowSizeMeasured =
          UnsafeRowSerializer::getSizeRow(inputVector.get(), j);
      EXPECT_EQ(rowSize.value_or(0), rowSizeMeasured);
      serialized.push_back(std::string_view(buffers_[j], rowSize.value()));
    }

    // Deserialize previous bytes back to row vector
    VectorPtr outputVector =
        UnsafeRowDeserializer::deserialize(serialized, rowType, pool_.get());

    assertEqualVectors(inputVector, outputVector);
  }
}

} // namespace
} // namespace facebook::velox::row
