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

#include <folly/Random.h>
#include <random>
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/encryption/TestProvider.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/dwio/dwrf/test/utils/E2EWriterTestUtil.h"
#include "velox/dwio/dwrf/test/utils/MapBuilder.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::encryption;
using namespace facebook::velox::dwio::common::encryption::test;
using namespace facebook::velox::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwrf::encryption;
using namespace facebook::velox::dwio::type::fbhive;
using namespace facebook::velox;
using facebook::velox::memory::MemoryPool;
using folly::Random;

constexpr uint64_t kSizeMB = 1024UL * 1024UL;

// This test can be run to generate test files. Run it with following command
// buck test velox/dwio/dwrf/test:velox_dwrf_e2e_writer_tests --
// DISABLED_TestFileCreation
// --run-disabled
TEST(E2EWriterTests, DISABLED_TestFileCreation) {
  const size_t batchCount = 4;
  const size_t size = 200;

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "byte_val:tinyint,"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "float_val:float,"
      "double_val:double,"
      "string_val:string,"
      "binary_val:binary,"
      "timestamp_val:timestamp,"
      "array_val:array<float>,"
      "map_val:map<int,double>,"
      "map_val:map<bigint,double>," /* this is column 12 */
      "map_val:map<bigint,map<string, int>>," /* this is column 13 */
      "struct_val:struct<a:float,b:double>"
      ">");

  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set(
      Config::MAP_FLAT_COLS, {12, 13}); /* this is the second and third map */

  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  std::vector<VectorPtr> batches;
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(BatchMaker::createBatch(type, size, pool, nullptr, i));
  }

  auto sink = std::make_unique<FileSink>("/tmp/e2e_generated_file.orc");
  E2EWriterTestUtil::writeData(
      std::move(sink),
      type,
      batches,
      config,
      E2EWriterTestUtil::simpleFlushPolicyFactory(true));
}

VectorPtr createRowVector(
    facebook::velox::memory::MemoryPool* pool,
    std::shared_ptr<const Type> type,
    size_t size,
    const VectorPtr& child) {
  return std::make_shared<RowVector>(
      pool,
      type,
      BufferPtr(nullptr),
      size,
      std::vector<VectorPtr>{child},
      0 /*nullCount*/);
}

TEST(E2EWriterTests, E2E) {
  const size_t batchCount = 4;
  // Start with a size larger than stride to cover splitting into
  // strides. Continue with smaller size for faster test.
  size_t size = 1100;
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "byte_val:tinyint,"
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "float_val:float,"
      "double_val:double,"
      "string_val:string,"
      "binary_val:binary,"
      "timestamp_val:timestamp,"
      "array_val:array<float>,"
      "map_val:map<int,double>,"
      "map_val:map<bigint,double>," /* this is column 12 */
      "map_val:map<bigint,map<string, int>>," /* this is column 13 */
      "struct_val:struct<a:float,b:double>"
      ">");

  auto config = std::make_shared<Config>();
  config->set(Config::ROW_INDEX_STRIDE, static_cast<uint32_t>(1000));
  config->set(Config::FLATTEN_MAP, true);
  config->set(
      Config::MAP_FLAT_COLS, {12, 13}); /* this is the second and third map */

  std::vector<VectorPtr> batches;
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(BatchMaker::createBatch(type, size, pool, nullptr, i));
    size = 200;
  }

  E2EWriterTestUtil::testWriter(pool, type, batches, 1, 1, config);
}

TEST(E2EWriterTests, FlatMapDictionaryEncoding) {
  const size_t batchCount = 4;
  // Start with a size larger than stride to cover splitting into
  // strides. Continue with smaller size for faster test.
  size_t size = 1100;
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "map_val:map<int,double>,"
      "map_val:map<bigint,double>,"
      "map_val:map<bigint,map<string, int>>,"
      "map_val:map<int, string>,"
      "map_val:map<bigint,map<int, string>>"
      ">");

  auto config = std::make_shared<Config>();
  config->set(Config::ROW_INDEX_STRIDE, static_cast<uint32_t>(1000));
  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {0, 1, 2, 3, 4});
  config->set(Config::MAP_FLAT_DISABLE_DICT_ENCODING, false);
  config->set(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
  config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
  config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);

  std::vector<VectorPtr> batches;
  std::mt19937 gen;
  gen.seed(983871726);
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(BatchMaker::createBatch(type, size, pool, gen));
    size = 200;
  }

  E2EWriterTestUtil::testWriter(pool, type, batches, 1, 1, config);
}

TEST(E2EWriterTests, MaxFlatMapKeys) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  const uint32_t keyLimit = 2000;
  const auto randomStart = Random::rand32(100);

  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  b::row row;
  for (int32_t i = 0; i < keyLimit; ++i) {
    row.push_back(b::pair{randomStart + i, Random::rand64()});
  }

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto batch = createRowVector(&pool, type, 1, b::create(pool, b::rows{row}));

  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {0});
  config->set(Config::MAP_FLAT_MAX_KEYS, keyLimit);

  E2EWriterTestUtil::testWriter(
      pool, type, E2EWriterTestUtil::generateBatches(batch), 1, 1, config);
}

TEST(E2EWriterTests, PresentStreamIsSuppressedOnFlatMap) {
  using keyType = int32_t;
  using valueType = int64_t;
  using b = MapBuilder<keyType, valueType>;

  const auto randomStart = Random::rand32(100);

  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  b::row row;
  row.push_back(b::pair{randomStart, Random::rand64()});

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto batch = createRowVector(&pool, type, 1, b::create(pool, b::rows{row}));

  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {0});

  auto sink = std::make_unique<MemorySink>(pool, 200 * 1024 * 1024);
  auto sinkPtr = sink.get();

  auto writer = E2EWriterTestUtil::writeData(
      std::move(sink),
      type,
      E2EWriterTestUtil::generateBatches(std::move(batch)),
      config,
      E2EWriterTestUtil::simpleFlushPolicyFactory(true));

  // read it back and verify no present streams exist.
  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());

  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto dwrfRowReader = dynamic_cast<DwrfRowReader*>(rowReader.get());
  bool preload = true;
  std::unordered_set<uint32_t> actualNodeIds;
  for (int i = 0; i < reader->getNumberOfStripes(); ++i) {
    dwrfRowReader->loadStripe(i, preload);
    auto& footer = dwrfRowReader->getStripeFooter();
    for (int j = 0; j < footer.streams_size(); ++j) {
      auto stream = footer.streams(j);
      ASSERT_NE(stream.kind(), proto::Stream_Kind::Stream_Kind_PRESENT);
    }
  }
}

TEST(E2EWriterTests, TooManyFlatMapKeys) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  const uint32_t keyLimit = 2000;
  const auto randomStart = Random::rand32(100);

  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  b::row row;
  for (int32_t i = 0; i < (keyLimit + 1); ++i) {
    row.push_back(b::pair{randomStart + i, Random::rand64()});
  }

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto batch = createRowVector(&pool, type, 1, b::create(pool, b::rows{row}));

  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {0});
  config->set(Config::MAP_FLAT_MAX_KEYS, keyLimit);

  EXPECT_THROW(
      E2EWriterTestUtil::testWriter(
          pool, type, E2EWriterTestUtil::generateBatches(batch), 1, 1, config),
      exception::LoggedException);
}

TEST(E2EWriterTests, FlatMapBackfill) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;

  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  const uint32_t strideSize = 1000;

  std::vector<VectorPtr> batches;
  b::rows rows;

  for (int32_t i = 0; i < (strideSize * 3); ++i) {
    rows.push_back(
        b::row{b::pair{1, Random::rand64()}, b::pair{2, Random::rand64()}});
  }

  for (int32_t i = 0; i < (strideSize / 2); ++i) {
    rows.push_back(b::row{b::pair{1, Random::rand64()}});
  }

  // This row introduces new key, in the middle of a stride and and existing key
  // that wasn't used in this stride. The new key will trigger backfilling based
  // on previous stride rows. But since this is part of a bigger batch, spanning
  // the entire current stride, it will not trigger the partial stride backfill.
  // This is covered in the next batch below.
  rows.push_back(
      b::row{b::pair{3, Random::rand64()}, b::pair{2, Random::rand64()}});

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto rowCount = rows.size();
  auto batch =
      createRowVector(&pool, type, rowCount, b::create(pool, std::move(rows)));
  batches.push_back(batch);

  // This extra batch is forcing another write call in the same (partial)
  // stride. This tests the backfill of partial strides.
  batch = createRowVector(
      &pool, type, 1, b::create(pool, {b::row{b::pair{4, Random::rand64()}}}));
  batches.push_back(batch);
  // TODO: Add another batch inside last stride, to test for backfill in stride.

  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {0});
  config->set(Config::ROW_INDEX_STRIDE, strideSize);

  E2EWriterTestUtil::testWriter(
      pool,
      type,
      batches,
      1,
      1,
      config,
      E2EWriterTestUtil::simpleFlushPolicyFactory(false));
}

void testFlatMapWithNulls(
    bool firstRowNotNull,
    bool enableFlatmapDictionaryEncoding = false,
    bool shareDictionary = false) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;

  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  const uint32_t strideSize = 1000;

  std::vector<VectorPtr> batches;
  b::rows rows;

  for (int32_t i = 0; i < (strideSize * 3); ++i) {
    if (firstRowNotNull && i == 0) {
      rows.push_back(
          b::row{b::pair{1, Random::rand64()}, b::pair{2, Random::rand64()}});
    } else {
      rows.push_back({});
    }
  }

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto rowCount = rows.size();
  auto batch =
      createRowVector(&pool, type, rowCount, b::create(pool, std::move(rows)));
  batches.push_back(batch);

  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {0});
  config->set(Config::ROW_INDEX_STRIDE, strideSize);
  config->set(
      Config::MAP_FLAT_DISABLE_DICT_ENCODING, !enableFlatmapDictionaryEncoding);
  config->set(Config::MAP_FLAT_DICT_SHARE, shareDictionary);

  E2EWriterTestUtil::testWriter(
      pool,
      type,
      batches,
      1,
      1,
      config,
      E2EWriterTestUtil::simpleFlushPolicyFactory(false));
}

TEST(E2EWriterTests, FlatMapWithNulls) {
  testFlatMapWithNulls(
      /* firstRowNotNull */ false, /* enableFlatmapDictionaryEncoding */ false);
  testFlatMapWithNulls(
      /* firstRowNotNull */ true, /* enableFlatmapDictionaryEncoding */ false);
  testFlatMapWithNulls(
      /* firstRowNotNull */ false, /* enableFlatmapDictionaryEncoding */ true);
  testFlatMapWithNulls(
      /* firstRowNotNull */ true, /* enableFlatmapDictionaryEncoding */ true);
}

TEST(E2EWriterTests, FlatMapWithNullsSharedDict) {
  testFlatMapWithNulls(
      /* firstRowNotNull */ false,
      /* enableFlatmapDictionaryEncoding */ true,
      /* shareDictionary */ true);
  testFlatMapWithNulls(
      /* firstRowNotNull */ true,
      /* enableFlatmapDictionaryEncoding */ true,
      /* shareDictionary */ true);
}

TEST(E2EWriterTests, FlatMapEmpty) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;

  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  const uint32_t strideSize = 1000;

  std::vector<VectorPtr> batches;
  b::rows rows;

  for (int32_t i = 0; i < strideSize; ++i) {
    if (i % 5 != 0) {
      rows.push_back(b::row{b::pair{i % 3, Random::rand64()}});
    } else {
      rows.push_back(b::row{});
    }
  }

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto rowCount = rows.size();
  auto batch =
      createRowVector(&pool, type, rowCount, b::create(pool, std::move(rows)));
  batches.push_back(batch);

  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {0});
  config->set(Config::ROW_INDEX_STRIDE, strideSize);

  E2EWriterTestUtil::testWriter(
      pool,
      type,
      batches,
      1,
      1,
      config,
      E2EWriterTestUtil::simpleFlushPolicyFactory(false));
}

void testFlatMapConfig(
    std::shared_ptr<const Type> type,
    const std::vector<uint32_t>& mapColumnIds,
    const std::unordered_set<uint32_t>& expectedNodeIds) {
  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  size_t size = 100;
  size_t stripes = 3;

  // write file to memory
  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set<const std::vector<uint32_t>>(Config::MAP_FLAT_COLS, mapColumnIds);
  config->set(Config::MAP_STATISTICS, true);

  auto sink = std::make_unique<MemorySink>(pool, 200 * 1024 * 1024);
  auto sinkPtr = sink.get();

  WriterOptions options;
  options.config = config;
  options.schema = type;
  Writer writer{options, std::move(sink), pool};

  for (size_t i = 0; i < stripes; ++i) {
    writer.write(BatchMaker::createBatch(type, size, pool, nullptr, i));
  }

  writer.close();

  // read it back and verify encoding
  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());

  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto dwrfRowReader = dynamic_cast<DwrfRowReader*>(rowReader.get());
  bool preload = true;
  std::unordered_set<uint32_t> actualNodeIds;
  for (int32_t i = 0; i < reader->getNumberOfStripes(); ++i) {
    dwrfRowReader->loadStripe(0, preload);
    auto& footer = dwrfRowReader->getStripeFooter();
    for (int32_t j = 0; j < footer.encoding_size(); ++j) {
      auto encoding = footer.encoding(j);
      if (encoding.kind() ==
          proto::ColumnEncoding_Kind::ColumnEncoding_Kind_MAP_FLAT) {
        actualNodeIds.insert(encoding.node());
      }
    }
    ASSERT_EQ(expectedNodeIds, actualNodeIds);
  }
}

TEST(E2EWriterTests, FlatMapConfigSingleColumn) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "map_val:map<bigint,double>,"
      ">");

  testFlatMapConfig(type, {0}, {1});
  testFlatMapConfig(type, {}, {});
}

TEST(E2EWriterTests, FlatMapConfigMixedTypes) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "map_val:map<bigint,double>,"
      ">");

  testFlatMapConfig(type, {1}, {2});
  testFlatMapConfig(type, {}, {});
}

TEST(E2EWriterTests, FlatMapConfigNestedMap) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "map_val:map<bigint,map<string,float>>,"
      ">");

  testFlatMapConfig(type, {1}, {2});
  testFlatMapConfig(type, {}, {});
}

TEST(E2EWriterTests, FlatMapConfigMixedMaps) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "map_val:map<bigint,map<string,float>>,"
      "map_val:map<bigint,float>,"
      "map_val:map<bigint,map<string,float>>,"
      "map_val:map<bigint,double>,"
      ">");

  testFlatMapConfig(type, {2, 3}, {9, 14});
  testFlatMapConfig(type, {}, {});
}

TEST(E2EWriterTests, FlatMapConfigNotMapColumn) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "map_val:map<bigint,double>,"
      ">");

  EXPECT_THROW(
      { testFlatMapConfig(type, {0}, {}); }, exception::LoggedException);
}

TEST(E2EWriterTests, PartialStride) {
  HiveTypeParser parser;
  auto type = parser.parse("struct<bool_val:int>");

  auto scopedPool = memory::getDefaultScopedMemoryPool();
  auto& pool = *scopedPool;
  size_t size = 1'000;

  auto config = std::make_shared<Config>();
  auto sink = std::make_unique<MemorySink>(pool, 2 * 1024 * 1024);
  auto sinkPtr = sink.get();

  WriterOptions options;
  options.config = config;
  options.schema = type;
  Writer writer{options, std::move(sink), pool};

  auto nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  size_t nullCount = 0;

  auto values = AlignedBuffer::allocate<int32_t>(size, &pool);
  auto* valuesPtr = values->asMutable<int32_t>();

  for (size_t i = 0; i < size; ++i) {
    if ((i & 1) == 0) {
      bits::clearNull(nullsPtr, i);
      valuesPtr[i] = i;
    } else {
      bits::setNull(nullsPtr, i);
      nullCount++;
    }
  }

  auto batch = createRowVector(
      &pool,
      type,
      size,
      std::make_shared<FlatVector<int32_t>>(
          &pool, nulls, size, values, std::vector<BufferPtr>()));

  writer.write(batch);
  writer.close();

  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());

  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
  ASSERT_EQ(size - nullCount, reader->columnStatistics(1)->getNumberOfValues())
      << reader->columnStatistics(1)->toString();
  ASSERT_EQ(true, reader->columnStatistics(1)->hasNull().value());
}

TEST(E2EWriterTests, OversizeRows) {
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "map_val:map<string, map<string, map<string, map<string, string>>>>,"
      "list_val:array<array<array<array<string>>>>,"
      "struct_val:struct<"
      "map_val_field_1:map<string, map<string, map<string, map<string, string>>>>,"
      "list_val_field_1:array<array<array<array<string>>>>,"
      "list_val_field_2:array<array<array<array<string>>>>,"
      "map_val_field_2:map<string, map<string, map<string, map<string, string>>>>"
      ">,"
      ">");
  auto config = std::make_shared<Config>();
  config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
  config->set(Config::STRIPE_SIZE, 10 * kSizeMB);
  config->set(
      Config::RAW_DATA_SIZE_PER_BATCH, folly::to<uint64_t>(20 * 1024UL));

  // Retained bytes in vector: 44704
  auto singleBatch = E2EWriterTestUtil::generateBatches(
      type, 1, 1, /* seed */ 1411367325, pool);

  E2EWriterTestUtil::testWriter(
      pool,
      type,
      singleBatch,
      1,
      1,
      config,
      /* flushPolicyFactory */ nullptr,
      /* layoutPlannerFactory */ nullptr,
      /* memoryBudget */ std::numeric_limits<int64_t>::max(),
      false);
}

TEST(E2EWriterTests, OversizeBatches) {
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "byte_val:tinyint,"
      "float_val:float,"
      "double_val:double,"
      ">");
  auto config = std::make_shared<Config>();
  config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
  config->set(Config::STRIPE_SIZE, 10 * kSizeMB);

  // Test splitting a gigantic batch.
  auto singleBatch = E2EWriterTestUtil::generateBatches(
      type, 1, 10000000, /* seed */ 1411367325, pool);
  // A gigantic batch is split into 10 stripes.
  E2EWriterTestUtil::testWriter(
      pool,
      type,
      singleBatch,
      10,
      10,
      config,
      /* flushPolicyFactory */ nullptr,
      /* layoutPlannerFactory */ nullptr,
      /* memoryBudget */ std::numeric_limits<int64_t>::max(),
      false);

  // Test splitting multiple huge batches.
  auto batches = E2EWriterTestUtil::generateBatches(
      type, 3, 5000000, /* seed */ 1411367325, pool);
  // 3 gigantic batches are split into 15~16 stripes.
  E2EWriterTestUtil::testWriter(
      pool,
      type,
      batches,
      15,
      16,
      config,
      /* flushPolicyFactory */ nullptr,
      /* layoutPlannerFactory */ nullptr,
      /* memoryBudget */ std::numeric_limits<int64_t>::max(),
      false);
}

TEST(E2EWriterTests, OverflowLengthIncrements) {
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "struct_val:struct<bigint_val:bigint>"
      ">");
  auto config = std::make_shared<Config>();
  config->set(Config::DISABLE_LOW_MEMORY_MODE, true);
  config->set(Config::STRIPE_SIZE, 10 * kSizeMB);
  config->set(
      Config::RAW_DATA_SIZE_PER_BATCH,
      folly::to<uint64_t>(500 * 1024UL * 1024UL));

  const size_t size = 1024;

  auto nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  for (size_t i = 0; i < size; ++i) {
    // Only the first element is non-null
    bits::setNull(nullsPtr, i, i != 0);
  }

  // Bigint column
  VectorMaker maker{&pool};
  auto child = maker.flatVector<int64_t>(std::vector<int64_t>{1UL});

  std::vector<VectorPtr> children{child};
  auto rowVec = std::make_shared<RowVector>(
      &pool, type->childAt(0), nulls, size, children, /* nullCount */ size - 1);

  // Retained bytes in vector: 192, which is much less than 1024
  auto vec = std::make_shared<RowVector>(
      &pool,
      type,
      BufferPtr{},
      size,
      std::vector<VectorPtr>{rowVec},
      /* nullCount */ 0);

  E2EWriterTestUtil::testWriter(
      pool,
      type,
      {vec},
      1,
      1,
      config,
      /*flushPolicyFactory=*/nullptr,
      /*layoutPlannerFactory=*/nullptr,
      /*memoryBudget=*/std::numeric_limits<int64_t>::max(),
      false);
}

namespace facebook::velox::dwrf {

class E2EEncryptionTest : public Test {
 protected:
  std::unique_ptr<DwrfReader> writeAndRead(
      const std::string& schema,
      const std::shared_ptr<EncryptionSpecification>& spec,
      std::shared_ptr<DecrypterFactory> decrypterFactory =
          std::make_shared<TestDecrypterFactory>()) {
    auto& pool = memory::getProcessDefaultMemoryManager().getRoot().addChild(
        "encryption_test");
    HiveTypeParser parser;
    auto type = parser.parse(schema);

    // write file to memory
    auto config = std::make_shared<Config>();
    // make sure we always write dictionary to test stride index
    config->set(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
    config->set(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);
    auto sink = std::make_unique<MemorySink>(pool, 16 * 1024 * 1024);
    sink_ = sink.get();
    WriterOptions options;
    options.config = config;
    options.schema = type;
    options.encryptionSpec = spec;
    options.encrypterFactory = std::make_shared<TestEncrypterFactory>();
    writer_ = std::make_unique<Writer>(options, std::move(sink), pool);

    for (size_t i = 0; i < batchCount_; ++i) {
      auto batch = BatchMaker::createBatch(type, batchSize_, pool, nullptr, i);
      writer_->write(batch);
      batches_.push_back(std::move(batch));
      if (i % flushInterval_ == flushInterval_ - 1) {
        writer_->flush();
      }
    }
    writer_->close();

    // read it back for compare
    auto input =
        std::make_unique<MemoryInputStream>(sink_->getData(), sink_->size());
    ReaderOptions readerOpts;
    readerOpts.setDecrypterFactory(decrypterFactory);
    return std::make_unique<DwrfReader>(readerOpts, std::move(input));
  }

  const DecryptionHandler& getDecryptionHandler(
      const DwrfReader& reader) const {
    return reader.readerBase_->getDecryptionHandler();
  }

  void validateFileContent(const DwrfReader& reader) const {
    RowReaderOptions rowReaderOpts;
    auto rowReader = reader.createRowReader(rowReaderOpts);
    // make sure size estimate works
    ASSERT_GT(rowReader->estimatedRowSize(), 0);
    VectorPtr batch;
    for (auto& expected : batches_) {
      ASSERT_TRUE(rowReader->next(batchSize_, batch));
      compareBatches(batch, expected);
    }
    ASSERT_FALSE(rowReader->next(batchSize_, batch));

    // make sure decrypter is actually used
    auto& handler = getDecryptionHandler(reader);
    for (size_t i = 0; i < handler.getEncryptionGroupCount(); ++i) {
      auto& decrypter = dynamic_cast<const TestDecrypter&>(
          handler.getEncryptionProviderByIndex(i));
      ASSERT_GT(decrypter.getCount(), 0);
    }
  }

  void compareBatches(const VectorPtr& a, const VectorPtr& b) const {
    ASSERT_EQ(a->size(), b->size());
    for (int32_t i = 0; i < a->size(); ++i) {
      ASSERT_TRUE(a->equalValueAt(b.get(), i, i)) << "Mismatch at " << i;
    }
  }

  std::unique_ptr<Writer> writer_;
  MemorySink* sink_;
  size_t batchSize_{100};
  size_t batchCount_{10};
  size_t flushInterval_{3};
  std::vector<VectorPtr> batches_;
};

} // namespace facebook::velox::dwrf

TEST_F(E2EEncryptionTest, EncryptRoot) {
  auto spec =
      std::make_shared<EncryptionSpecification>(EncryptionProvider::Unknown);
  spec->withRootEncryptionProperties(
      std::make_shared<TestEncryptionProperties>("key"));
  auto reader =
      writeAndRead("struct<a:int,b:float,c:string,d:int,e:string>", spec);

  // make sure footer has right set of properties set
  auto& handler = getDecryptionHandler(*reader);
  ASSERT_TRUE(handler.isEncrypted());
  ASSERT_EQ(handler.getEncryptionGroupCount(), 1);
  ASSERT_EQ(handler.getEncryptionRoot(0), 0);

  // make sure footer doesn't have detailed stats
  auto& footer = reader->getFooter();
  auto& unencryptedStats = footer.statistics();
  auto encryptedStats = reader->getStatistics();
  ASSERT_EQ(unencryptedStats.size(), encryptedStats->getNumberOfColumns());
  for (size_t i = 0; i < unencryptedStats.size(); ++i) {
    auto& stats = unencryptedStats.Get(i);
    ASSERT_TRUE(stats.has_hasnull());
    ASSERT_TRUE(stats.has_numberofvalues());
    ASSERT_FALSE(
        stats.has_intstatistics() || stats.has_doublestatistics() ||
        stats.has_stringstatistics());
    auto& encrypted = encryptedStats->getColumnStatistics(i);
    ASSERT_EQ(stats.hasnull(), encrypted.hasNull().value());
    ASSERT_EQ(stats.numberofvalues(), encrypted.getNumberOfValues());
    // stats got thru api should not be the basic one
    ASSERT_EQ(typeid(encrypted) == typeid(const ColumnStatistics&), i == 0);
  }

  RowReaderOptions rowReaderOpts;
  auto rowReader = reader->createRowReader(rowReaderOpts);

  // make sure stripe footer doesn't have any stream/encoding
  auto dwrfRowReader = dynamic_cast<DwrfRowReader*>(rowReader.get());
  bool preload = true;
  for (int32_t i = 0; i < reader->getNumberOfStripes(); ++i) {
    dwrfRowReader->loadStripe(0, preload);
    auto& sf = dwrfRowReader->getStripeFooter();
    ASSERT_EQ(sf.encoding_size(), 0);
    ASSERT_EQ(sf.streams_size(), 0);
    ASSERT_EQ(sf.encryptiongroups_size(), 1);
  }

  validateFileContent(*reader);
}

TEST_F(E2EEncryptionTest, EncryptSelectedFields) {
  auto spec =
      std::make_shared<EncryptionSpecification>(EncryptionProvider::Unknown);
  spec->withEncryptedField(
          FieldEncryptionSpecification{}.withIndex(1).withEncryptionProperties(
              std::make_shared<TestEncryptionProperties>("key1")))
      .withEncryptedField(
          FieldEncryptionSpecification{}.withIndex(2).withEncryptionProperties(
              std::make_shared<TestEncryptionProperties>("key2")))
      .withEncryptedField(
          FieldEncryptionSpecification{}.withIndex(4).withEncryptionProperties(
              std::make_shared<TestEncryptionProperties>("key2")));
  std::string schema =
      "struct<"
      // not encrypted
      "a:int,"
      // encrypted primitive type. encryption group 0
      "b:float,"
      // encrypted complex type. encryption group 1
      "c:map<int,string>,"
      // not encrypted
      "d:struct<a:int,b:string,c:float>,"
      // encrypted complex type. encryption group 1
      "e:array<struct<a:int,b:string>>>";
  auto reader = writeAndRead(schema, spec);

  // make sure footer has right set of properties set
  auto& handler = getDecryptionHandler(*reader);
  ASSERT_TRUE(handler.isEncrypted());
  ASSERT_EQ(handler.getEncryptionGroupCount(), 2);

  std::vector<std::vector<uint32_t>> eg{{2}, {3, 4, 5, 10, 11, 12, 13}};
  std::unordered_set<uint32_t> encryptedNodes;
  for (size_t i = 0; i < eg.size(); ++i) {
    for (auto& n : eg[i]) {
      ASSERT_EQ(handler.getEncryptionGroupIndex(n), i);
      encryptedNodes.insert(n);
    }
  }

  // make sure footer doesn't have detailed stats
  auto& footer = reader->getFooter();
  auto& unencryptedStats = footer.statistics();
  auto encryptedStats = reader->getStatistics();
  ASSERT_EQ(unencryptedStats.size(), encryptedStats->getNumberOfColumns());
  for (size_t i = 0; i < unencryptedStats.size(); ++i) {
    auto& stats = unencryptedStats.Get(i);
    ASSERT_TRUE(stats.has_hasnull());
    ASSERT_TRUE(stats.has_numberofvalues());
    // only unencrypted leaf node in the schema may have detailed stats
    ASSERT_EQ(
        stats.has_intstatistics() || stats.has_doublestatistics() ||
            stats.has_stringstatistics(),
        (i == 1 || i == 7 || i == 8 || i == 9));
    auto& encrypted = encryptedStats->getColumnStatistics(i);
    ASSERT_EQ(stats.hasnull(), encrypted.hasNull().value());
    ASSERT_EQ(stats.numberofvalues(), encrypted.getNumberOfValues());
    // stats got thru api should not be the basic one unless they are
    // intermediate nodes
    ASSERT_EQ(
        typeid(encrypted) == typeid(const ColumnStatistics&),
        (i == 0 || i == 3 || i == 6 || i == 10 || i == 11));
  }

  RowReaderOptions rowReaderOpts;
  auto rowReader = reader->createRowReader(rowReaderOpts);

  // make sure stripe footer doesn't have any stream/encoding
  auto dwrfRowReader = dynamic_cast<DwrfRowReader*>(rowReader.get());
  bool preload = true;
  for (int32_t i = 0; i < reader->getNumberOfStripes(); ++i) {
    dwrfRowReader->loadStripe(0, preload);
    auto& sf = dwrfRowReader->getStripeFooter();
    for (auto& enc : sf.encoding()) {
      ASSERT_TRUE(encryptedNodes.find(enc.node()) == encryptedNodes.end());
    }
    for (auto& stream : sf.streams()) {
      ASSERT_TRUE(encryptedNodes.find(stream.node()) == encryptedNodes.end());
    }
  }

  validateFileContent(*reader);
}

TEST_F(E2EEncryptionTest, EncryptEmptyFile) {
  auto spec =
      std::make_shared<EncryptionSpecification>(EncryptionProvider::Unknown);
  spec->withEncryptedField(
      FieldEncryptionSpecification{}.withIndex(0).withEncryptionProperties(
          std::make_shared<TestEncryptionProperties>("key1")));
  std::string schema = "struct<a:int>";
  batchCount_ = 0;
  auto reader = writeAndRead(schema, spec);

  // make sure footer has right set of properties set
  auto& handler = getDecryptionHandler(*reader);
  ASSERT_FALSE(handler.isEncrypted());
}

TEST_F(E2EEncryptionTest, ReadWithoutKey) {
  auto spec =
      std::make_shared<EncryptionSpecification>(EncryptionProvider::Unknown);
  spec->withEncryptedField(
      FieldEncryptionSpecification{}.withIndex(1).withEncryptionProperties(
          std::make_shared<TestEncryptionProperties>("key1")));
  std::string schema =
      "struct<"
      // not encrypted
      "a:int,"
      // encrypted
      "b:float>";
  HiveTypeParser parser;
  auto type = std::dynamic_pointer_cast<const RowType>(parser.parse(schema));
  auto reader = writeAndRead(schema, spec, nullptr);

  // reading unencrypted column should not fail
  {
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<ColumnSelector>(type, std::vector<uint64_t>{0}));
    auto rowReader = reader->createRowReader(rowReaderOpts);
    VectorPtr batch;
    ASSERT_TRUE(rowReader->next(1, batch));
  }

  // fail when reading encrypted column
  {
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<ColumnSelector>(type, std::vector<uint64_t>{1}));
    auto rowReader = reader->createRowReader(rowReaderOpts);
    VectorPtr batch;
    ASSERT_THROW(rowReader->next(1, batch), exception::LoggedException);
  }
}

namespace {

void testWriter(
    MemoryPool& pool,
    const std::shared_ptr<const Type>& type,
    size_t batchCount,
    std::function<VectorPtr()> generator,
    const std::shared_ptr<Config> config = std::make_shared<Config>()) {
  std::vector<VectorPtr> batches;
  for (auto i = 0; i < batchCount; ++i) {
    batches.push_back(generator());
  }
  E2EWriterTestUtil::testWriter(pool, type, batches, 1, 1, config);
};

template <TypeKind K>
VectorPtr createKeysImpl(
    MemoryPool& pool,
    std::mt19937& rng,
    size_t size,
    size_t maxVal) {
  using TCpp = typename TypeTraits<K>::NativeType;

  auto vector = std::make_shared<FlatVector<TCpp>>(
      &pool,
      nullptr,
      size,
      AlignedBuffer::allocate<TCpp>(size, &pool),
      std::vector<BufferPtr>{});

  size_t value = 0;
  for (size_t i = 0; i < vector->size(); ++i, value = ((value + 1) % maxVal)) {
    if constexpr (std::is_same_v<TCpp, StringView>) {
      auto strVal = folly::to<std::string>(value);
      StringView sv{strVal};
      DWIO_ENSURE(sv.isInline());
      vector->set(i, sv);
    } else {
      vector->set(i, value);
    }
  }

  if (folly::Random::oneIn(2, rng)) {
    return vector;
  }

  // wrap in dictionary
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, &pool);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (size_t i = 0; i < size; ++i) {
    rawIndices[i] = size - 1 - i;
  }

  return BaseVector::wrapInDictionary(nullptr, indices, size, vector);
}

VectorPtr createKeys(
    const std::shared_ptr<const Type> type,
    MemoryPool& pool,
    std::mt19937& rng,
    size_t size,
    size_t maxVal) {
  switch (type->kind()) {
    case TypeKind::INTEGER:
      return createKeysImpl<TypeKind::INTEGER>(pool, rng, size, maxVal);
    case TypeKind::VARCHAR:
      return createKeysImpl<TypeKind::VARCHAR>(pool, rng, size, maxVal);
    default:
      folly::assume_unreachable();
  }
}

} // namespace

TEST(E2EWriterTests, fuzzSimple) {
  std::unique_ptr<memory::ScopedMemoryPool> scopedPool =
      memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  auto type = ROW({
      {"bool_val", BOOLEAN()},
      {"byte_val", TINYINT()},
      {"short_val", SMALLINT()},
      {"int_val", INTEGER()},
      {"long_val", BIGINT()},
      {"float_val", REAL()},
      {"double_val", DOUBLE()},
      {"string_val", VARCHAR()},
      {"binary_val", VARBINARY()},
      {"ts_val", TIMESTAMP()},
  });
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

  // Small batches creates more edge cases.
  size_t batchSize = 10;
  VectorFuzzer noNulls(
      {
          .vectorSize = batchSize,
          .nullRatio = 0,
          .stringLength = 20,
          .stringVariableLength = true,
      },
      &pool,
      seed);

  VectorFuzzer hasNulls{
      {
          .vectorSize = batchSize,
          .nullRatio = 0.05,
          .stringLength = 10,
          .stringVariableLength = true,
      },
      &pool,
      seed};

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    testWriter(
        pool, type, batches, [&]() { return noNulls.fuzzInputRow(type); });
    testWriter(
        pool, type, batches, [&]() { return hasNulls.fuzzInputRow(type); });
  }
}

TEST(E2EWriterTests, fuzzComplex) {
  std::unique_ptr<memory::ScopedMemoryPool> scopedPool =
      memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  auto type = ROW({
      {"array", ARRAY(REAL())},
      {"map", MAP(INTEGER(), DOUBLE())},
      {"row",
       ROW({
           {"a", REAL()},
           {"b", INTEGER()},
       })},
      {"nested",
       ARRAY(ROW({
           {"a", INTEGER()},
           {"b", MAP(REAL(), REAL())},
       }))},
  });
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;

  // Small batches creates more edge cases.
  size_t batchSize = 10;
  VectorFuzzer noNulls(
      {
          .vectorSize = batchSize,
          .nullRatio = 0,
          .stringLength = 20,
          .stringVariableLength = true,
          .containerLength = 5,
          .containerVariableLength = true,
      },
      &pool,
      seed);

  VectorFuzzer hasNulls{
      {
          .vectorSize = batchSize,
          .nullRatio = 0.05,
          .stringLength = 10,
          .stringVariableLength = true,
          .containerLength = 5,
          .containerVariableLength = true,
      },
      &pool,
      seed};

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    testWriter(
        pool, type, batches, [&]() { return noNulls.fuzzInputRow(type); });
    testWriter(
        pool, type, batches, [&]() { return hasNulls.fuzzInputRow(type); });
  }
}

TEST(E2EWriterTests, fuzzFlatmap) {
  std::unique_ptr<memory::ScopedMemoryPool> scopedPool =
      memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  auto type = ROW({
      {"flatmap1", MAP(INTEGER(), REAL())},
      {"flatmap2", MAP(VARCHAR(), ARRAY(REAL()))},
      {"flatmap3", MAP(INTEGER(), MAP(INTEGER(), REAL()))},
  });
  auto config = std::make_shared<Config>();
  config->set(Config::FLATTEN_MAP, true);
  config->set(Config::MAP_FLAT_COLS, {0, 1, 2});
  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  // Small batches creates more edge cases.
  size_t batchSize = 10;
  VectorFuzzer fuzzer(
      {
          .vectorSize = batchSize,
          .nullRatio = 0,
          .stringLength = 20,
          .stringVariableLength = true,
          .containerLength = 5,
          .containerVariableLength = true,
      },
      &pool,
      seed);

  auto genMap = [&](auto type, auto size) {
    auto offsets = allocateOffsets(size, &pool);
    auto rawOffsets = offsets->template asMutable<vector_size_t>();
    auto sizes = allocateSizes(size, &pool);
    auto rawSizes = sizes->template asMutable<vector_size_t>();
    vector_size_t childSize = 0;
    // flatmap doesn't like empty map
    for (auto i = 0; i < batchSize; ++i) {
      rawOffsets[i] = childSize;
      auto length = folly::Random::rand32(1, 5, rng);
      rawSizes[i] = length;
      childSize += length;
    }

    VectorFuzzer valueFuzzer(
        {
            .vectorSize = static_cast<size_t>(childSize),
            .nullRatio = 0,
            .stringLength = 20,
            .stringVariableLength = true,
            .containerLength = 5,
            .containerVariableLength = true,
        },
        &pool,
        seed);

    auto& mapType = type->asMap();
    VectorPtr vector = std::make_shared<MapVector>(
        &pool,
        type,
        nullptr,
        size,
        offsets,
        sizes,
        createKeys(mapType.keyType(), pool, rng, childSize, 10),
        valueFuzzer.fuzz(mapType.valueType()));

    if (folly::Random::oneIn(2, rng)) {
      vector = fuzzer.fuzzDictionary(vector);
    }
    return vector;
  };

  auto gen = [&]() {
    std::vector<VectorPtr> children;
    for (auto i = 0; i < type->size(); ++i) {
      children.push_back(genMap(type->childAt(i), batchSize));
    }

    return std::make_shared<RowVector>(
        &pool, type, nullptr, batchSize, std::move(children));
  };

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    testWriter(pool, type, batches, gen, config);
  }
}
