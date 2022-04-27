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
#include "folly/CPortability.h"

#include "folly/Random.h"
#include "velox/dwio/common/DataSink.h"
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/dwrf/test/utils/E2EWriterTestUtil.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"

using namespace ::testing;
using namespace facebook::velox::dwio::type::fbhive;
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
  FlushPolicyTestCase(
      uint64_t inputStripeSize,
      uint64_t inputDictSize,
      uint32_t inputNumStripesLower,
      uint32_t inputNumStripesUpper,
      uint32_t inputSeed,
      int64_t memoryBudget = std::numeric_limits<int64_t>::max())
      : stripeSize{inputStripeSize},
        dictSize{inputDictSize},
        numStripesLower{inputNumStripesLower},
        numStripesUpper{inputNumStripesUpper},
        seed{inputSeed},
        memoryBudget{memoryBudget} {}
  const uint64_t stripeSize;
  const uint64_t dictSize;
  const uint32_t numStripesLower;
  const uint32_t numStripesUpper;
  const uint32_t seed;
  const int64_t memoryBudget;
};

struct FlatMapFlushPolicyTestCase : public FlushPolicyTestCase {
 public:
  FlatMapFlushPolicyTestCase(
      uint64_t inputStripeSize,
      uint64_t inputDictSize,
      uint32_t inputNumStripesLower,
      uint32_t inputNumStripesUpper,
      bool enableDictionary,
      bool enableDictionarySharing,
      uint32_t inputSeed,
      int64_t memoryBudget = std::numeric_limits<int64_t>::max())
      : FlushPolicyTestCase{inputStripeSize, inputDictSize, inputNumStripesLower, inputNumStripesUpper, inputSeed, memoryBudget},
        enableDictionary{enableDictionary},
        enableDictionarySharing{enableDictionarySharing} {}

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
      /* flushPolicyFactory */ nullptr,
      /* layoutPlannerFactory */ nullptr,
      memoryBudget,
      false);
}

void testWriterStaticBudgetFlushPolicy(
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
      /* flushPolicyFactory */
      [stripeSizeThreshold = config->get(Config::STRIPE_SIZE),
       dictionarySizeThreshold = config->get(Config::MAX_DICTIONARY_SIZE)]() {
        return std::make_unique<StaticBudgetFlushPolicy>(
            stripeSizeThreshold, dictionarySizeThreshold);
      },
      /* layoutPlannerFactory */ nullptr,
      memoryBudget,
      false);
}

TEST(E2EWriterTests, FlushPolicySimpleEncoding) {
  const size_t batchCount = 200;
  const size_t size = 1000;
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "byte_val:tinyint,"
      "float_val:float,"
      "double_val:double,"
      "timestamp_val:timestamp,"
      ">");

  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 13,
          /*numStripesUpper*/ 13,
          /*seed*/ 1411367325},
      FlushPolicyTestCase{
          /*stripeSize*/ 512 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 7,
          /*numStripesUpper*/ 7,
          /*seed*/ 1411367325},
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 4,
          /*numStripesUpper*/ 4,
          /*seed*/ 1411367325});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);
    testWriterDefaultFlushPolicy(
        pool,
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
TEST(E2EWriterTests, FlushPolicyDictionaryEncoding) {
  const size_t batchCount = 500;
  const size_t size = 1000;
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "string_val:string,"
      "binary_val:binary,"
      ">");

  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 11,
          /*numStripesUpper*/ 11,
          /*seed*/ 1630160118},
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 11,
          /*numStripesUpper*/ 11,
          /*seed*/ 1630160118,
          11 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 2 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 6,
          /*numStripesUpper*/ 6,
          /*seed*/ 1630160118},
      FlushPolicyTestCase{
          /*stripeSize*/ 2 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 6,
          /*numStripesUpper*/ 6,
          /*seed*/ 1630160118,
          13 * kSizeMB});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);
    testWriterDefaultFlushPolicy(
        pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }

  auto dictionaryEncodingTestCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 42,
          /*numStripesUpper*/ 42,
          /*seed*/ 1630160118},
      FlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 42,
          /*numStripesUpper*/ 42,
          /*seed*/ 1630160118,
          6 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 512 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 22,
          /*numStripesUpper*/ 22,
          /*seed*/ 1630160118},
      FlushPolicyTestCase{
          /*stripeSize*/ 512 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 22,
          /*numStripesUpper*/ 22,
          /*seed*/ 1630160118,
          10 * kSizeMB});

  for (const auto& testCase : dictionaryEncodingTestCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);
    testWriterDefaultFlushPolicy(
        pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }

  auto dictionarySizeTestCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          /*stripeSize*/ std::numeric_limits<int64_t>::max(),
          /*dictSize*/ 20 * kSizeKB,
          /*numStripesLower*/ 500,
          /*numStripesUpper*/ 500,
          /*seed*/ 1719796763},
      FlushPolicyTestCase{
          /*stripeSize*/ std::numeric_limits<int64_t>::max(),
          /*dictSize*/ 40 * kSizeKB,
          /*numStripesLower*/ 250,
          /*numStripesUpper*/ 250,
          /*seed*/ 1719796763});

  for (const auto& testCase : dictionarySizeTestCases) {
    // Force writing with dictionary encoding.
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);

    config->set(Config::MAX_DICTIONARY_SIZE, testCase.dictSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);
    testWriterDefaultFlushPolicy(
        pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// stream usage seems to have a delta that is close to compression block size?
TEST(E2EWriterTests, FlushPolicyNestedTypes) {
  const size_t batchCount = 10;
  const size_t size = 1000;
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "array_val:array<float>,"
      "map_val:map<int,double>,"
      "map_val1:map<bigint,double>,"
      "map_val2:map<bigint,map<string, int>>,"
      "struct_val:struct<a:float,b:double>"
      ">");

  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 5,
          /*numStripesUpper*/ 5,
          /*seed*/ 3850165650},
      FlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 5,
          /*numStripesUpper*/ 5,
          /*seed*/ 3850165650,
          8 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 512 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 3,
          /*numStripesUpper*/ 3,
          /*seed*/ 3850165650},
      FlushPolicyTestCase{
          /*stripeSize*/ 512 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 3,
          /*numStripesUpper*/ 3,
          /*seed*/ 2969662436,
          8 * kSizeMB});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);
    testWriterDefaultFlushPolicy(
        pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// T78009859
TEST(E2EWriterTests, FlushPolicyWithStaticMemoryBudget) {
  const size_t batchCount = 2000;
  const size_t size = 1000;
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  // Dictionary encodable types have the richest feature to test.
  auto type = parser.parse(
      "struct<"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "string_val:string,"
      "binary_val:binary,"
      ">");

  auto testCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 41,
          /*numStripesUpper*/ 41,
          /*seed*/ 630992088},
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 41,
          /*numStripesUpper*/ 41,
          /*seed*/ 630992088,
          10 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 41,
          /*numStripesUpper*/ 41,
          /*seed*/ 1630160118},
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 41,
          /*numStripesUpper*/ 41,
          /*seed*/ 1630160118,
          13 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 2 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 20,
          /*numStripesUpper*/ 20,
          /*seed*/ 1630160118},
      FlushPolicyTestCase{
          /*stripeSize*/ 2 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 20,
          /*numStripesUpper*/ 20,
          /*seed*/ 1630160118,
          13 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 32 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 1,
          /*numStripesUpper*/ 1,
          /*seed*/ 1630160118},
      FlushPolicyTestCase{
          /*stripeSize*/ 32 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 2,
          /*numStripesUpper*/ 3,
          /*seed*/ 1630160118,
          40 * kSizeMB});

  for (const auto& testCase : testCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 0.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 0.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);
    testWriterStaticBudgetFlushPolicy(
        pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

TEST(E2EWriterTests, FlushPolicyDictionaryEncodingWithStaticMemoryBudget) {
  const size_t batchCount = 500;
  const size_t size = 1000;
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  // Dictionary encodable types have the richest feature to test.
  auto type = parser.parse(
      "struct<"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "string_val:string,"
      "binary_val:binary,"
      ">");

  auto dictionaryEncodingTestCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 42,
          /*numStripesUpper*/ 42,
          /*seed*/ 630992088},
      FlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 41,
          /*numStripesUpper*/ 46,
          /*seed*/ 630992088,
          4 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 11,
          /*numStripesUpper*/ 11,
          /*seed*/ 1630160118},
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 21,
          /*numStripesUpper*/ 41,
          /*seed*/ 1630160118,
          6 * kSizeMB});

  for (const auto& testCase : dictionaryEncodingTestCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);
    testWriterStaticBudgetFlushPolicy(
        pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// Test that some memory caps are not possible, or that stripe size
TEST(E2EWriterTests, StaticMemoryBudgetTrim) {
  const size_t batchCount = 2000;
  const size_t size = 1000;
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  // Dictionary encodable types have the richest feature to test.
  auto type = parser.parse(
      "struct<"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "string_val:string,"
      "binary_val:binary,"
      ">");

  auto dictionaryEncodingTestCases = folly::make_array<FlushPolicyTestCase>(
      FlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 166,
          /*numStripesUpper*/ 182,
          /*seed*/ 630992088,
          4 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 512 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 86,
          /*numStripesUpper*/ 86,
          /*seed*/ 630992088,
          6 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 44,
          /*numStripesUpper*/ 44,
          /*seed*/ 630992088,
          10 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 2 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 20,
          /*numStripesUpper*/ 20,
          /*seed*/ 630992088,
          12 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 10 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 5,
          /*numStripesUpper*/ 5,
          /*seed*/ 630992088,
          20 * kSizeMB},
      FlushPolicyTestCase{
          /*stripeSize*/ 32 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 2,
          /*numStripesUpper*/ 3,
          /*seed*/ 630992088,
          40 * kSizeMB});

  for (const auto& testCase : dictionaryEncodingTestCases) {
    auto config = std::make_shared<Config>();
    config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    // This is the one flush policy test that doesn't disable low memory mode.
    config->set(Config::STRIPE_SIZE, testCase.stripeSize);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);
    testWriterStaticBudgetFlushPolicy(
        pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

// Flat map has 1.5 orders of magnitude inflated stream memory usage.
TEST(E2EWriterTests, FlushPolicyFlatMap) {
  const size_t batchCount = 10;
  const size_t size = 1000;
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

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

  auto testCases = folly::make_array<FlatMapFlushPolicyTestCase>(
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 10,
          /*numStripesUpper*/ 10,
          /*enableDictionary*/ true,
          /*enableDictionarySharing*/ false,
          /*seed*/ 1321904009},
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 512 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 6,
          /*numStripesUpper*/ 6,
          /*enableDictionary*/ true,
          /*enableDictionarySharing*/ false,
          /*seed*/ 1321904009},
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 2 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 3,
          /*numStripesUpper*/ 3,
          /*enableDictionary*/ true,
          /*enableDictionarySharing*/ true,
          /*seed*/ 1321904009},
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 10,
          /*numStripesUpper*/ 10,
          /*enableDictionary*/ false,
          /*enableDictionarySharing*/ false,
          /*seed*/ 1321904009},
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 512 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 6,
          /*numStripesUpper*/ 6,
          /*enableDictionary*/ false,
          /*enableDictionarySharing*/ false,
          /*seed*/ 1321904009},
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 2 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 3,
          /*numStripesUpper*/ 3,
          /*enableDictionary*/ false,
          /*enableDictionarySharing*/ true,
          /*seed*/ 1321904009});

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
        type, batchCount, size, testCase.seed, pool);

    testWriterDefaultFlushPolicy(
        pool,
        type,
        batches,
        testCase.numStripesLower,
        testCase.numStripesUpper,
        config,
        testCase.memoryBudget);
  }
}

TEST(E2EWriterTests, FlushPolicyFlatMapWithStaticMemoryBudget) {
  const size_t batchCount = 10;
  const size_t size = 1000;
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

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

  auto testCases = folly::make_array<FlatMapFlushPolicyTestCase>(
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 10,
          /*numStripesUpper*/ 10,
          /*enableDictionary*/ true,
          /*enableDictionarySharing*/ false,
          /*seed*/ 1321904009,
          256 * kSizeMB},
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 4,
          /*numStripesUpper*/ 4,
          /*enableDictionary*/ true,
          /*enableDictionarySharing*/ false,
          /*seed*/ 1321904009,
          256 * kSizeMB},
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 256 * kSizeKB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 10,
          /*numStripesUpper*/ 10,
          /*enableDictionary*/ true,
          /*enableDictionarySharing*/ true,
          /*seed*/ 1321904009,
          256 * kSizeMB},
      FlatMapFlushPolicyTestCase{
          /*stripeSize*/ 1 * kSizeMB,
          /*dictSize*/ std::numeric_limits<int64_t>::max(),
          /*numStripesLower*/ 4,
          /*numStripesUpper*/ 4,
          /*enableDictionary*/ true,
          /*enableDictionarySharing*/ true,
          /*seed*/ 1321904009,
          256 * kSizeMB});

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
    }
    config->set(Config::MAP_FLAT_DICT_SHARE, testCase.enableDictionarySharing);
    auto batches = E2EWriterTestUtil::generateBatches(
        type, batchCount, size, testCase.seed, pool);

    testWriterStaticBudgetFlushPolicy(
        pool,
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
