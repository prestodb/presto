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

#include "velox/serializers/UnsafeRowSerde.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::row {
namespace {

using namespace facebook::velox::test;

/// This test evaluate UnsafeRowBatchSerde
class UnsafeRowBatchSerdeFuzzTests : public ::testing::Test {
 public:
  UnsafeRowBatchSerdeFuzzTests() {}

  void resetBuffer() {
    blockUsedSize_ = 0;
  }

  void addRowToBuffer(
      const std::string_view& serializedKeys,
      const std::string_view& serializedRow) {
    // The key value pairs (keys, row) are added to the buffer fashion
    // <keys size> <serialized keys> <row size> <serialized row>
    if (blockUsedSize_ + serializedKeys.size() + serializedRow.size() +
            2 * sizeof(size_t) >
        blockSize_) {
      // Reallocate block buffer if the rows are overflowing
      size_t newBlockSize =
          (blockSize_ + serializedKeys.size() + serializedRow.size()) * 2;
      AlignedBuffer::reallocate<char>(&bufferPtr_, newBlockSize, 0);
      buffer_ = bufferPtr_->asMutable<char>();
      blockSize_ = newBlockSize;
    }
    // Add the row (and keys) to the block
    auto buffer = buffer_ + blockUsedSize_;
    *((size_t*)buffer) = serializedKeys.size();
    // Counting for the size value
    buffer += sizeof(size_t);
    std::memcpy(buffer, serializedKeys.data(), serializedKeys.size());
    buffer += serializedKeys.size();
    *((size_t*)buffer) = serializedRow.size();
    buffer += sizeof(size_t);
    std::memcpy(buffer, serializedRow.data(), serializedRow.size());
    blockUsedSize_ +=
        serializedKeys.size() + serializedRow.size() + 2 * sizeof(size_t);
  }

  bool collectRowPointers(
      const std::string_view& block,
      bool includesKeys,
      std::vector<std::optional<std::string_view>>& rowPointers) {
    char* buffer = const_cast<char*>(block.data());
    char* originalBuffer = buffer;

    // Precess until the end of block
    ssize_t remainingSize = block.size();
    while (remainingSize > 0) {
      // Skip keys if any
      if (includesKeys) {
        size_t keysSize = *((size_t*)buffer);
        if (keysSize < 0 || keysSize > remainingSize) {
          return false;
        }
        buffer += sizeof(keysSize);
        buffer += keysSize;
      }
      // Read the row size
      size_t rowSize = *((size_t*)buffer);
      if (rowSize < 0 || rowSize > remainingSize) {
        return false;
      }
      buffer += sizeof(rowSize);
      rowPointers.emplace_back(std::string_view(buffer, rowSize));
      buffer += rowSize;
      remainingSize = block.size() - (buffer - originalBuffer);
      if (remainingSize < 0) {
        return false;
      }
    }
    return true;
  }

  void testSerde(
      const RowVectorPtr& inputVector,
      std::optional<std::vector<uint64_t>> keys,
      RowVectorPtr& outputVector) {
    // Creating a serializer if there are keys
    std::unique_ptr<velox::batch::VectorKeySerializer> keySerializer =
        keys.has_value()
        ? std::make_unique<velox::batch::UnsafeRowKeySerializer>()
        : nullptr;

    // Creating serializer/deserializer
    velox::batch::UnsafeRowVectorSerde serde(pool_.get(), keySerializer, keys);

    // Serializing row and keys
    std::string_view serializedKeys;
    std::string_view serializedRow;
    // Serialize one row at a time and add to block
    serde.reset(inputVector);
    for (vector_size_t index = 0; index < inputVector->size(); index++) {
      auto err =
          serde.serializeRow(inputVector, index, serializedKeys, serializedRow);
      ASSERT_EQ(err, velox::batch::BatchSerdeStatus::Success);
      addRowToBuffer(serializedKeys, serializedRow);
    }

    // Collect row pointers
    std::vector<std::optional<std::string_view>> rowPointers;
    auto collected = collectRowPointers(
        std::string_view(buffer_, blockUsedSize_), true, rowPointers);
    ASSERT_TRUE(collected);

    // Use block deserializer to deserialize all the rows
    auto err = serde.deserializeVector(
        rowPointers,
        std::dynamic_pointer_cast<const RowType>(inputVector->type()),
        &outputVector);
    ASSERT_EQ(err, velox::batch::BatchSerdeStatus::Success);
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
  BufferPtr bufferPtr_ =
      AlignedBuffer::allocate<char>(kInitialBlockSize, pool_.get(), true);
  char* buffer_ = bufferPtr_->asMutable<char>();
  size_t blockSize_ = kInitialBlockSize;
  size_t blockUsedSize_ = 0;
  static constexpr size_t kInitialBlockSize = 10 << 10; // 10k
};

TEST_F(UnsafeRowBatchSerdeFuzzTests, simpleTypeRoundTripTest) {
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
  opts.vectorSize = 5;
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
  SCOPED_TRACE(fmt::format("seed: {}", seed));
  VectorFuzzer fuzzer(opts, pool_.get(), seed);

  const auto iterations = 5;
  for (size_t i = 0; i < iterations; ++i) {
    resetBuffer();
    const auto& inputVector = fuzzer.fuzzRow(rowType);
    RowVectorPtr outputVector;
    testSerde(
        inputVector,
        std::vector<uint64_t>{0, 1, 9, 10} /* 2 fix and 2 var-size keys */,
        outputVector);
    // Compare the input and output vectors
    assertEqualVectors(inputVector, outputVector);
  }
}

} // namespace
} // namespace facebook::velox::row
