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

#include "velox/row/UnsafeRowBatchDeserializer.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::row {
namespace {

using namespace facebook::velox::test;

class UnsafeRowFuzzTests : public ::testing::Test {
 public:
  UnsafeRowFuzzTests() {
    clearBuffer();
  }

  void clearBuffer() {
    std::memset(buffer_, 0, BUFFER_SIZE);
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
  BufferPtr bufferPtr_ =
      AlignedBuffer::allocate<char>(BUFFER_SIZE, pool_.get(), true);
  char* buffer_ = bufferPtr_->asMutable<char>();
  static constexpr uint64_t BUFFER_SIZE = 20 << 10; // 20k
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
  opts.vectorSize = 1;
  opts.nullRatio = 0.1;
  opts.containerHasNulls = false;
  opts.dictionaryHasNulls = false;
  opts.stringVariableLength = true;
  opts.stringLength = 20;
  // Spark uses microseconds to store timestamp
  opts.useMicrosecondPrecisionTimestamp = true;
  opts.containerLength = 65;

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  const auto iterations = 1000;
  for (size_t i = 0; i < iterations; ++i) {
    clearBuffer();
    const auto& inputVector = fuzzer.fuzzRow(rowType);
    // Serialize rowVector into bytes.
    auto rowSize = UnsafeRowDynamicSerializer::serialize(
        rowType, inputVector, buffer_, /*idx=*/0);

    // Deserialize previous bytes back to row vector
    VectorPtr outputVector =
        UnsafeRowDynamicVectorBatchDeserializer::deserializeComplex(
            std::string_view(buffer_, rowSize.value()), rowType, pool_.get());

    assertEqualVectors(
        inputVector, outputVector, fmt::format(" (seed {}).", seed));
  }
}

} // namespace
} // namespace facebook::velox::row
