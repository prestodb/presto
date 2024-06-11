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
#include <cstdint>
#include "folly/CPortability.h"

#include "folly/Random.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/dwrf/test/utils/E2EWriterTestUtil.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"

using namespace ::testing;
using namespace facebook::velox::type::fbhive;
using namespace facebook::velox::dwrf;
using facebook::velox::memory::MemoryPool;
using folly::Random;

namespace facebook::velox::dwrf {

// Exclude flush policy tests from tsan.
#ifndef FOLLY_SANITIZE_THREAD

namespace {
constexpr size_t kSizeKB = 1024;
constexpr size_t kSizeMB = 1024 * 1024;

struct FlushPolicyTestCase {
 public:
  const uint64_t stripeSize;
  const uint64_t dictSize;
  const uint32_t numStripesLower;
  const uint32_t numStripesUpper;
  const uint32_t seed;
  const int64_t memoryBudget = std::numeric_limits<int64_t>::max();

  std::string debugString() const {
    return fmt::format(
        "inputStripeSize {}, dictSize {}, inputNumStripesLower {}, inputNumStripesUpper {}, seed {}, memoryBudget {}",
        stripeSize,
        dictSize,
        numStripesLower,
        numStripesUpper,
        seed,
        memoryBudget);
  }
};

struct FlatMapFlushPolicyTestCase : public FlushPolicyTestCase {
 public:
  const bool enableDictionary;
  const bool enableDictionarySharing;
};
} // namespace

void testWriterDefaultFlushPolicy(
    ::facebook::velox::memory::MemoryPool& pool,
    const std::shared_ptr<const Type>& type,
    const std::vector<VectorPtr>& batches,
    size_t numStripesLower,
    size_t numStripesUpper,
    const std::shared_ptr<Config>& config,
    int64_t memoryBudget) {
  E2EWriterTestUtil::testWriter(
      pool,
      type,
      batches,
      numStripesLower,
      numStripesUpper,
      config,
      /*flushPolicyFactory=*/nullptr,
      /*layoutPlannerFactory=*/nullptr,
      memoryBudget,
      false);
}

class E2EWriterTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(E2EWriterTest, FlushPolicySimpleEncoding) {
  const size_t batchCount = 200;
  const size_t batchSize = 1000;
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "byte_val:tinyint,"
      "float_val:float,"
      "double_val:double,"
      "timestamp_val:timestamp,"
      ">");

  // Final file size ~3.5MB
  const uint32_t seed = 1411367325;
  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = 256 * kSizeKB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 13,
          .numStripesUpper = 13,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 512 * kSizeKB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 7,
          .numStripesUpper = 7,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 1 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 4,
          .numStripesUpper = 4,
          .seed = seed});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// Many streams are not yet allocated prior to the first flush, hence first
// flush is delayed if we rely on stream usage to estimate stripe size.
TEST_F(E2EWriterTest, FlushPolicyDictionaryEncoding) {
  const size_t batchCount = 500;
  const size_t batchSize = 1000;
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "string_val:string,"
      "binary_val:binary,"
      ">");

  // Final file size ~10.7MB
  const uint32_t seed = 1630160118;
  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = 1 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 11,
          .numStripesUpper = 11,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 2 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 5,
          .numStripesUpper = 5,
          .seed = seed});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }

  // Final file size ~11MB
  auto dictionaryEncodingTestCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = 256 * kSizeKB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 42,
          .numStripesUpper = 43,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 512 * kSizeKB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 22,
          .numStripesUpper = 22,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 4 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 3,
          .numStripesUpper = 3,
          .seed = seed});

  for (const auto& testCase : dictionaryEncodingTestCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }

  // Final file size ~11MB
  const uint32_t dictionaryFlushSeed = 1719796763;
  auto dictionarySizeTestCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = std::numeric_limits<int64_t>::max(),
          .dictSize = 20 * kSizeKB,
          .numStripesLower = 500,
          .numStripesUpper = 500,
          .seed = dictionaryFlushSeed},
      FlushPolicyTestCase{
          .stripeSize = std::numeric_limits<int64_t>::max(),
          .dictSize = 40 * kSizeKB,
          .numStripesLower = 250,
          .numStripesUpper = 250,
          .seed = dictionaryFlushSeed});

  for (const auto& testCase : dictionarySizeTestCases) {
    // Force writing with dictionary encoding.
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);

    config->set(Config::MAX_DICTIONARY_SIZE, testCase.dictSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// stream usage seems to have a delta that is close to compression block size?
TEST_F(E2EWriterTest, FlushPolicyNestedTypes) {
  const size_t batchCount = 10;
  const size_t batchSize = 1000;
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "array_val:array<float>,"
      "map_val:map<int,double>,"
      "map_val1:map<bigint,double>,"
      "map_val2:map<bigint,map<string, int>>,"
      "struct_val:struct<a:float,b:double>"
      ">");

  // Final file size ~1.4MB
  const uint32_t seed = 3850165650;
  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = 256 * kSizeKB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 5,
          .numStripesUpper = 5,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 512 * kSizeKB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 3,
          .numStripesUpper = 3,
          .seed = seed});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// Flat map has 1.5 orders of magnitude inflated stream memory usage.
TEST_F(E2EWriterTest, FlushPolicyFlatMap) {
  const size_t batchCount = 10;
  const size_t batchSize = 500;
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  // A mixture of columns where dictionary sharing is not necessarily
  // turned on.
  auto type = parser.parse(
      "struct<"
      "map_val:map<int,int>,"
      "dense_features:map<int,float>,"
      "sparse_features:map<bigint,array<bigint>>,"
      "id_score_list_features:map<bigint,map<bigint, float>>,"
      ">");

  const uint32_t seed = 1321904009;
  auto testCases = folly::make_array<FlatMapFlushPolicyTestCase>(
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 256 * kSizeKB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 6,
           .numStripesUpper = 6,
           .seed = seed},
          /*enableDictionary=*/true,
          /*enableDictionarySharing=*/false},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 512 * kSizeKB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 4,
           .numStripesUpper = 4,
           .seed = seed},
          /*enableDictionary=*/true,
          /*enableDictionarySharing=*/false},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 2 * kSizeMB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 2,
           .numStripesUpper = 2,
           .seed = seed},
          /*enableDictionary=*/true,
          /*enableDictionarySharing=*/false},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 256 * kSizeKB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 6,
           .numStripesUpper = 6,
           .seed = seed},
          /*enableDictionary=*/false,
          /*enableDictionarySharing=*/false},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 512 * kSizeKB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 4,
           .numStripesUpper = 4,
           .seed = seed},
          /*enableDictionary=*/false,
          /*enableDictionarySharing=*/false},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 2 * kSizeMB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 2,
           .numStripesUpper = 2,
           .seed = seed},
          /*enableDictionary=*/false,
          /*enableDictionarySharing=*/false});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    config->set(Config::MAX_DICTIONARY_SIZE, testCase.dictSize);
    config->set(Config::FLATTEN_MAP, true);
    config->set(Config::MAP_FLAT_COLS, {0, 1, 2, 3});
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(
        Config::MAP_FLAT_DISABLE_DICT_ENCODING, !testCase.enableDictionary);
    if (testCase.enableDictionary) {
      // Force dictionary encoding for integer columns.
      config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    } else {
      config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 0.0f);
    }
    config->set(Config::MAP_FLAT_DICT_SHARE, testCase.enableDictionarySharing);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);

    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

TEST_F(E2EWriterTest, MemoryPoolBasedFlushPolicySimpleEncoding) {
  const size_t batchCount = 2000;
  const size_t batchSize = 5000;
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "byte_val:tinyint,"
      "float_val:float,"
      "double_val:double,"
      "timestamp_val:timestamp,"
      ">");

  // Final file size ~175MB
  const uint32_t seed = 1411367325;
  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = 2 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 55,
          .numStripesUpper = 55,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 4 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 23,
          .numStripesUpper = 23,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 32 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 3,
          .numStripesUpper = 3,
          .seed = seed});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    config->set<uint64_t>(dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN, 64UL);
    config->set(dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS, false);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// Many streams are not yet allocated prior to the first flush, hence first
// flush is delayed if we rely on stream usage to estimate stripe size.
TEST_F(E2EWriterTest, MemoryPoolBasedFlushPolicyDictionaryEncoding) {
  const size_t batchCount = 1000;
  const size_t batchSize = 2000;
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "string_val:string,"
      "binary_val:binary,"
      ">");

  // Final file size 43MB-46MB
  const uint32_t seed = 1630160118;
  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = 1 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 38,
          .numStripesUpper = 38,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 2 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 15,
          .numStripesUpper = 15,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 16 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 2,
          .numStripesUpper = 2,
          .seed = seed});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    config->set<uint64_t>(dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN, 64UL);
    config->set(dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS, false);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }

  // Final file size 43MB-46MB
  auto dictionaryEncodingTestCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = 1 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 63,
          .numStripesUpper = 63,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 2 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 29,
          .numStripesUpper = 29,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 16 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 3,
          .numStripesUpper = 3,
          .seed = seed});

  for (const auto& testCase : dictionaryEncodingTestCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    config->set<uint64_t>(dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN, 64UL);
    config->set(dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS, false);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }

  // Final file size 43MB-46MB
  const uint32_t dictionaryFlushSeed = 1719796763;
  auto dictionarySizeTestCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = std::numeric_limits<int64_t>::max(),
          .dictSize = 200 * kSizeKB,
          .numStripesLower = 319,
          .numStripesUpper = 319,
          .seed = dictionaryFlushSeed},
      FlushPolicyTestCase{
          .stripeSize = std::numeric_limits<int64_t>::max(),
          .dictSize = 1 * kSizeMB,
          .numStripesLower = 63,
          .numStripesUpper = 63,
          .seed = dictionaryFlushSeed});

  for (const auto& testCase : dictionarySizeTestCases) {
    // Force writing with dictionary encoding.
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    config->set<uint64_t>(dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN, 64UL);
    config->set(dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS, false);

    config->set(Config::MAX_DICTIONARY_SIZE, testCase.dictSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// stream usage seems to have a delta that is close to compression block size?
TEST_F(E2EWriterTest, MemoryPoolBasedFlushPolicyNestedTypes) {
  const size_t batchCount = 100;
  const size_t batchSize = 1000;
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "array_val:array<float>,"
      "map_val:map<int,double>,"
      "map_val1:map<bigint,double>,"
      "map_val2:map<bigint,map<string, int>>,"
      "struct_val:struct<a:float,b:double>"
      ">");

  // Final file size 14MB
  const uint32_t seed = 3850165650;
  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          .stripeSize = 2 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 5,
          .numStripesUpper = 5,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 4 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 3,
          .numStripesUpper = 3,
          .seed = seed},
      FlushPolicyTestCase{
          .stripeSize = 16 * kSizeMB,
          .dictSize = std::numeric_limits<int64_t>::max(),
          .numStripesLower = 1,
          .numStripesUpper = 1,
          .seed = seed});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    config->set<uint64_t>(dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN, 64UL);
    config->set(dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS, false);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);
    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// Flat map has 1.5 orders of magnitude inflated stream memory usage.
TEST_F(E2EWriterTest, MemoryPoolBasedFlushPolicyFlatMap) {
  const size_t batchCount = 500;
  const size_t batchSize = 500;
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  // A mixture of columns where dictionary sharing is not necessarily
  // turned on.
  auto type = parser.parse(
      "struct<"
      "map_val:map<int,int>,"
      "dense_features:map<int,float>,"
      "sparse_features:map<bigint,array<bigint>>,"
      "id_score_list_features:map<bigint,map<bigint, float>>,"
      ">");

  const uint32_t seed = 1321904009;
  auto testCases = folly::make_array<FlatMapFlushPolicyTestCase>(
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 4 * kSizeMB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 310,
           .numStripesUpper = 310,
           .seed = seed},
          /*enableDictionary=*/true,
          /*enableDictionarySharing=*/true},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 16 * kSizeMB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 3,
           .numStripesUpper = 4,
           .seed = seed},
          /*enableDictionary=*/true,
          /*enableDictionarySharing=*/true},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 128 * kSizeMB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 1,
           .numStripesUpper = 1,
           .seed = seed},
          /*enableDictionary=*/true,
          /*enableDictionarySharing=*/true},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 4 * kSizeMB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 308,
           .numStripesUpper = 308,
           .seed = seed},
          /*enableDictionary=*/false,
          /*enableDictionarySharing=*/true},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 16 * kSizeMB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 3,
           .numStripesUpper = 4,
           .seed = seed},
          /*enableDictionary=*/false,
          /*enableDictionarySharing=*/true},
      FlatMapFlushPolicyTestCase{
          {.stripeSize = 128 * kSizeMB,
           .dictSize = std::numeric_limits<int64_t>::max(),
           .numStripesLower = 1,
           .numStripesUpper = 1,
           .seed = seed},
          /*enableDictionary=*/false,
          /*enableDictionarySharing=*/true});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    config->set(Config::MAX_DICTIONARY_SIZE, testCase.dictSize);
    config->set(Config::FLATTEN_MAP, true);
    config->set(Config::MAP_FLAT_COLS, {0, 1, 2, 3});
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set<uint64_t>(dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN, 64UL);
    config->set(dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS, false);
    config->set(
        Config::MAP_FLAT_DISABLE_DICT_ENCODING, !testCase.enableDictionary);
    if (testCase.enableDictionary) {
      // Force dictionary encoding for integer columns.
      config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    } else {
      config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 0.0f);
    }
    config->set(Config::MAP_FLAT_DICT_SHARE, testCase.enableDictionarySharing);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, batchSize, testCase.seed, *pool);

    testWriterDefaultFlushPolicy(
        *pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

#endif /* FOLLY_SANITIZE_THREAD */
} // namespace facebook::velox::dwrf
