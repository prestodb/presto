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
#include "velox/common/base/SpillConfig.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/common/encryption/TestProvider.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/common/tests/utils/MapBuilder.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/test/utils/E2EWriterTestUtil.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace ::testing;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::encryption;
using namespace facebook::velox::dwio::common::encryption::test;
using namespace facebook::velox::test;
using namespace facebook::velox::dwrf::encryption;
using namespace facebook::velox::type::fbhive;
using namespace facebook::velox;
using facebook::velox::memory::MemoryPool;
using folly::Random;

constexpr uint64_t kSizeMB = 1024UL * 1024UL;

namespace {
class E2EWriterTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    TestValue::enable();
    memory::MemoryManager::testingSetInstance({});
  }

  E2EWriterTest() {
    rootPool_ = memory::memoryManager()->addRootPool("E2EWriterTest");
    leafPool_ = rootPool_->addLeafChild("leaf");
  }

  std::unique_ptr<dwrf::DwrfReader> createReader(
      const MemorySink& sink,
      const dwio::common::ReaderOptions& opts) {
    std::string_view data(sink.data(), sink.size());
    return std::make_unique<dwrf::DwrfReader>(
        opts,
        std::make_unique<BufferedInput>(
            std::make_shared<InMemoryReadFile>(data), opts.memoryPool()));
  }

  void testFlatMapConfig(
      std::shared_ptr<const Type> type,
      const std::vector<uint32_t>& mapColumnIds,
      const std::unordered_set<uint32_t>& expectedNodeIds) {
    size_t batchSize = 100;
    size_t stripes = 3;

    // write file to memory
    auto config = std::make_shared<dwrf::Config>();
    config->set(dwrf::Config::FLATTEN_MAP, true);
    config->set<const std::vector<uint32_t>>(
        dwrf::Config::MAP_FLAT_COLS, mapColumnIds);
    config->set(dwrf::Config::MAP_STATISTICS, true);

    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024,
        dwio::common::FileSink::Options{.pool = leafPool_.get()});
    auto sinkPtr = sink.get();

    dwrf::WriterOptions options;
    options.config = config;
    options.schema = type;
    options.memoryPool = rootPool_.get();
    dwrf::Writer writer{std::move(sink), options};

    for (size_t i = 0; i < stripes; ++i) {
      writer.write(
          BatchMaker::createBatch(type, batchSize, *leafPool_, nullptr, i));
    }

    writer.close();

    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    RowReaderOptions rowReaderOpts;
    auto reader = createReader(*sinkPtr, readerOpts);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto dwrfRowReader = dynamic_cast<dwrf::DwrfRowReader*>(rowReader.get());
    bool preload = true;
    std::unordered_set<uint32_t> actualNodeIds;
    for (int32_t i = 0; i < reader->getNumberOfStripes(); ++i) {
      auto stripeMetadata = dwrfRowReader->fetchStripe(i, preload);
      auto& footer = *stripeMetadata->footer;
      for (int32_t j = 0; j < footer.encoding_size(); ++j) {
        auto encoding = footer.encoding(j);
        if (encoding.kind() ==
            dwrf::proto::ColumnEncoding_Kind::ColumnEncoding_Kind_MAP_FLAT) {
          actualNodeIds.insert(encoding.node());
        }
      }
      ASSERT_EQ(expectedNodeIds, actualNodeIds);
    }
  }

  void testFlatMapFileStats(
      std::shared_ptr<const Type> type,
      const std::vector<uint32_t>& mapColumnIds,
      const uint32_t strideSize = 10000,
      const uint32_t rowCount = 2000) {
    size_t stripes = 3;

    // write file to memory
    auto config = std::make_shared<dwrf::Config>();
    // Ensure we cross stride boundary
    config->set(dwrf::Config::ROW_INDEX_STRIDE, strideSize);
    config->set(dwrf::Config::FLATTEN_MAP, true);
    config->set<const std::vector<uint32_t>>(
        dwrf::Config::MAP_FLAT_COLS, mapColumnIds);
    config->set(dwrf::Config::MAP_STATISTICS, true);

    auto sink = std::make_unique<MemorySink>(
        400 * 1024 * 1024,
        dwio::common::FileSink::Options{.pool = leafPool_.get()});
    auto sinkPtr = sink.get();

    dwrf::WriterOptions options;
    options.config = config;
    options.schema = type;
    options.memoryPool = rootPool_.get();
    dwrf::Writer writer{std::move(sink), options};

    const size_t seed = std::time(nullptr);
    LOG(INFO) << "seed: " << seed;
    std::mt19937 gen{};
    gen.seed(seed);
    for (size_t i = 0; i < stripes; ++i) {
      // The logic really does not depend on data shape. Hence, we can
      // ignore the nulls.
      writer.write(
          BatchMaker::createBatch(type, rowCount, *leafPool_, gen, nullptr));
      writer.flush();
    }

    writer.close();

    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    RowReaderOptions rowReaderOpts;
    auto reader = createReader(*sinkPtr, readerOpts);
    auto rowReader = reader->createRowReader(rowReaderOpts);

    auto dwrfRowReader = dynamic_cast<dwrf::DwrfRowReader*>(rowReader.get());
    bool preload = true;

    auto typeWithId = TypeWithId::create(type);
    for (auto mapColumn : mapColumnIds) {
      folly::F14FastMap<KeyInfo, uint64_t, folly::transparent<KeyInfoHash>>
          featureStreamSizes;
      auto mapTypeId = typeWithId->childAt(mapColumn)->id();
      auto valueTypeId = mapTypeId + 2;
      for (int32_t i = 0; i < reader->getNumberOfStripes(); ++i) {
        auto stripeMetadata = dwrfRowReader->fetchStripe(i, preload);
        auto& currentStripeInfo = stripeMetadata->stripeInfo;
        dwrf::StripeStreamsImpl stripeStreams(
            std::make_shared<dwrf::StripeReadState>(
                dwrfRowReader->readerBaseShared(), std::move(stripeMetadata)),
            &dwrfRowReader->getColumnSelector(),
            nullptr,
            rowReaderOpts,
            currentStripeInfo.offset(),
            currentStripeInfo.numberOfRows(),
            *dwrfRowReader,
            i);

        folly::F14FastMap<int64_t, dwio::common::KeyInfo> sequenceToKey;

        stripeStreams.visitStreamsOfNode(
            valueTypeId, [&](const dwrf::StreamInformation& stream) {
              auto sequence = stream.getSequence();
              // No need to load shared dictionary stream here.
              if (sequence == 0) {
                return;
              }

              dwrf::EncodingKey seqEk(valueTypeId, sequence);
              const auto& keyInfo = stripeStreams.getEncoding(seqEk).key();
              auto key = dwrf::constructKey(keyInfo);
              sequenceToKey.emplace(sequence, key);
            });

        auto allStreams = stripeStreams.getStreamIdentifiers();
        for (const auto& streamIdPerNode : allStreams) {
          for (const auto& streamId : streamIdPerNode.second) {
            if (streamId.encodingKey().sequence() != 0 &&
                streamId.column() == mapColumn) {
              // Update the aggregate.
              const auto& keyInfo =
                  sequenceToKey.at(streamId.encodingKey().sequence());
              auto streamLength = stripeStreams.getStreamLength(streamId);
              auto it = featureStreamSizes.find(keyInfo);
              if (it == featureStreamSizes.end()) {
                featureStreamSizes.emplace(keyInfo, streamLength);
              } else {
                it->second += streamLength;
              }
            }
          }
        }
      }
      auto stats = reader->getFooter().statistics(mapTypeId);
      ASSERT_TRUE(stats.hasMapStatistics());
      ASSERT_EQ(featureStreamSizes.size(), stats.mapStatistics().stats_size());
      for (size_t i = 0; i != stats.mapStatistics().stats_size(); ++i) {
        const auto& entry = stats.mapStatistics().stats(i);
        ASSERT_TRUE(entry.stats().has_size());
        EXPECT_EQ(
            featureStreamSizes.at(dwrf::constructKey(entry.key())),
            entry.stats().size());
      }
    }
  }

  static common::SpillConfig getSpillConfig(
      int32_t minSpillableReservationPct,
      int32_t spillableReservationGrowthPct,
      uint64_t writerFlushThresholdSize = 0) {
    static const std::string emptySpillFolder = "";
    return common::SpillConfig(
        [&]() -> const std::string& { return emptySpillFolder; },
        [&](uint64_t) {},
        "fakeSpillConfig",
        0,
        0,
        0,
        nullptr,
        minSpillableReservationPct,
        spillableReservationGrowthPct,
        0,
        0,
        0,
        0,
        writerFlushThresholdSize,
        "none");
  }

  std::shared_ptr<MemoryPool> rootPool_;
  std::shared_ptr<MemoryPool> leafPool_;
};

// This test can be run to generate test files. Run it with following command
// buck test velox/dwio/dwrf/test:velox_dwrf_e2e_writer_tests --
// DISABLED_TestFileCreation
// --run-disabled
TEST_F(E2EWriterTest, DISABLED_TestFileCreation) {
  const size_t batchCount = 4;
  const size_t batchSize = 200;

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

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(
      dwrf::Config::MAP_FLAT_COLS,
      {12, 13}); /* this is the second and third map */

  std::vector<VectorPtr> batches;
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(
        BatchMaker::createBatch(type, batchSize, *leafPool_, nullptr, i));
  }

  auto path = "/tmp/e2e_generated_file.orc";
  auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink = std::make_unique<WriteFileSink>(std::move(localWriteFile), path);
  dwrf::E2EWriterTestUtil::writeData(
      std::move(sink),
      type,
      batches,
      config,
      dwrf::E2EWriterTestUtil::simpleFlushPolicyFactory(true));
}

VectorPtr createRowVector(
    facebook::velox::memory::MemoryPool* pool,
    std::shared_ptr<const Type> type,
    size_t batchSize,
    const VectorPtr& child) {
  return std::make_shared<RowVector>(
      pool,
      type,
      BufferPtr(nullptr),
      batchSize,
      std::vector<VectorPtr>{child},
      /*nullCount=*/0);
}

TEST_F(E2EWriterTest, E2E) {
  const size_t batchCount = 4;
  // Start with a size larger than stride to cover splitting into
  // strides. Continue with smaller size for faster test.
  size_t batchSize = 1100;

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

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::ROW_INDEX_STRIDE, static_cast<uint32_t>(1000));
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(
      dwrf::Config::MAP_FLAT_COLS,
      {12, 13}); /* this is the second and third map */

  std::vector<VectorPtr> batches;
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(
        BatchMaker::createBatch(type, batchSize, *leafPool_, nullptr, i));
    batchSize = 200;
  }

  dwrf::E2EWriterTestUtil::testWriter(*leafPool_, type, batches, 1, 1, config);
}

TEST_F(E2EWriterTest, DisableLinearHeuristics) {
  const size_t batchCount = 100;
  size_t batchSize = 3000;

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

  auto batches = dwrf::E2EWriterTestUtil::generateBatches(
      type, batchCount, batchSize, /*seed=*/1411367325, *leafPool_);

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::ROW_INDEX_STRIDE, static_cast<uint32_t>(1000));
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(
      dwrf::Config::MAP_FLAT_COLS,
      {12, 13}); /* this is the second and third map */
  config->set<uint64_t>(dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN, 64UL);
  config->set<uint64_t>(dwrf::Config::STRIPE_SIZE, 25UL * 1024 * 1024);

  // default mode writer
  dwrf::E2EWriterTestUtil::testWriter(*leafPool_, type, batches, 3, 3, config);

  // disable linear heuristics
  config->set(dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS, false);
  dwrf::E2EWriterTestUtil::testWriter(*leafPool_, type, batches, 2, 3, config);
}

// Beside writing larger files, this test also uses regular maps only.
TEST_F(E2EWriterTest, DisableLinearHeuristicsLargeAnalytics) {
  const size_t batchCount = 500;
  size_t batchSize = 3000;

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

  auto batches = dwrf::E2EWriterTestUtil::generateBatches(
      type, batchCount, batchSize, /*seed=*/1411367325, *leafPool_);

  auto config = std::make_shared<dwrf::Config>();
  config->set<uint64_t>(dwrf::Config::COMPRESSION_BLOCK_SIZE_MIN, 64UL);
  config->set<uint64_t>(dwrf::Config::STRIPE_SIZE, 25UL * 1024 * 1024);

  // default mode writer
  dwrf::E2EWriterTestUtil::testWriter(
      *leafPool_, type, batches, 10, 10, config);

  // disable linear heuristics
  config->set(dwrf::Config::LINEAR_STRIPE_SIZE_HEURISTICS, false);
  // When disabling linear heuristics, avg stripe size goes up to ~33MB from
  // ~25MB.
  dwrf::E2EWriterTestUtil::testWriter(*leafPool_, type, batches, 8, 8, config);
}

TEST_F(E2EWriterTest, FlatMapDictionaryEncoding) {
  const size_t batchCount = 4;
  // Start with a size larger than stride to cover splitting into
  // strides. Continue with smaller size for faster test.
  size_t batchSize = 1100;
  auto pool = memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "map_val:map<int,double>,"
      "map_val:map<bigint,double>,"
      "map_val:map<bigint,map<string, int>>,"
      "map_val:map<int, string>,"
      "map_val:map<bigint,map<int, string>>"
      ">");

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::ROW_INDEX_STRIDE, static_cast<uint32_t>(1000));
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0, 1, 2, 3, 4});
  config->set(dwrf::Config::MAP_FLAT_DISABLE_DICT_ENCODING, false);
  config->set(dwrf::Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
  config->set(dwrf::Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
  config->set(dwrf::Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);

  std::vector<VectorPtr> batches;
  std::mt19937 gen;
  gen.seed(983871726);
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(BatchMaker::createBatch(type, batchSize, *pool, gen));
    batchSize = 200;
  }

  dwrf::E2EWriterTestUtil::testWriter(*pool, type, batches, 1, 1, config);
}

TEST_F(E2EWriterTest, MaxFlatMapKeys) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  const uint32_t keyLimit = 2000;
  const auto randomStart = Random::rand32(100);

  auto pool = memory::memoryManager()->addLeafPool();
  b::row row;
  for (int32_t i = 0; i < keyLimit; ++i) {
    row.push_back(b::pair{randomStart + i, Random::rand64()});
  }

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto batch =
      createRowVector(pool.get(), type, 1, b::create(*pool, b::rows{row}));

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});
  config->set(dwrf::Config::MAP_FLAT_MAX_KEYS, keyLimit);

  dwrf::E2EWriterTestUtil::testWriter(
      *pool,
      type,
      dwrf::E2EWriterTestUtil::generateBatches(batch),
      1,
      1,
      config);
}

TEST_F(E2EWriterTest, PresentStreamIsSuppressedOnFlatMap) {
  using keyType = int32_t;
  using valueType = int64_t;
  using b = MapBuilder<keyType, valueType>;

  const auto randomStart = Random::rand32(100);

  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();
  b::row row;
  row.push_back(b::pair{randomStart, Random::rand64()});

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto batch =
      createRowVector(pool.get(), type, 1, b::create(*pool, b::rows{row}));

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});

  auto sink = std::make_unique<MemorySink>(
      200 * 1024 * 1024,
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto sinkPtr = sink.get();

  auto writer = dwrf::E2EWriterTestUtil::writeData(
      std::move(sink),
      type,
      dwrf::E2EWriterTestUtil::generateBatches(std::move(batch)),
      config,
      dwrf::E2EWriterTestUtil::simpleFlushPolicyFactory(true));

  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  RowReaderOptions rowReaderOpts;
  auto reader = createReader(*sinkPtr, readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto dwrfRowReader = dynamic_cast<dwrf::DwrfRowReader*>(rowReader.get());
  bool preload = true;
  std::unordered_set<uint32_t> actualNodeIds;
  for (int i = 0; i < reader->getNumberOfStripes(); ++i) {
    auto stripeMetadata = dwrfRowReader->fetchStripe(i, preload);
    auto& footer = *stripeMetadata->footer;
    for (int j = 0; j < footer.streams_size(); ++j) {
      auto stream = footer.streams(j);
      ASSERT_NE(stream.kind(), dwrf::proto::Stream_Kind::Stream_Kind_PRESENT);
    }
  }
}

TEST_F(E2EWriterTest, TooManyFlatMapKeys) {
  using keyType = int32_t;
  using valueType = int32_t;
  using b = MapBuilder<keyType, valueType>;

  const uint32_t keyLimit = 2000;
  const auto randomStart = Random::rand32(100);

  auto pool = memory::memoryManager()->addLeafPool();
  b::row row;
  for (int32_t i = 0; i < (keyLimit + 1); ++i) {
    row.push_back(b::pair{randomStart + i, Random::rand64()});
  }

  const auto type = CppToType<Row<Map<keyType, valueType>>>::create();
  auto batch =
      createRowVector(pool.get(), type, 1, b::create(*pool, b::rows{row}));

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});
  config->set(dwrf::Config::MAP_FLAT_MAX_KEYS, keyLimit);

  EXPECT_THROW(
      dwrf::E2EWriterTestUtil::testWriter(
          *pool,
          type,
          dwrf::E2EWriterTestUtil::generateBatches(batch),
          1,
          1,
          config),
      exception::LoggedException);
}

TEST_F(E2EWriterTest, FlatMapBackfill) {
  auto pool = memory::memoryManager()->addLeafPool();

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
  auto batch = createRowVector(
      pool.get(), type, rowCount, b::create(*pool, std::move(rows)));
  batches.push_back(batch);

  // This extra batch is forcing another write call in the same (partial)
  // stride. This tests the backfill of partial strides.
  batch = createRowVector(
      pool.get(),
      type,
      1,
      b::create(*pool, {b::row{b::pair{4, Random::rand64()}}}));
  batches.push_back(batch);
  // TODO: Add another batch inside last stride, to test for backfill in stride.

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});
  config->set(dwrf::Config::ROW_INDEX_STRIDE, strideSize);

  dwrf::E2EWriterTestUtil::testWriter(
      *pool,
      type,
      batches,
      1,
      1,
      config,
      dwrf::E2EWriterTestUtil::simpleFlushPolicyFactory(false));
}

void testFlatMapWithNulls(
    bool firstRowNotNull,
    bool enableFlatmapDictionaryEncoding = false,
    bool shareDictionary = false) {
  auto pool = memory::memoryManager()->addLeafPool();

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
  auto batch = createRowVector(
      pool.get(), type, rowCount, b::create(*pool, std::move(rows)));
  batches.push_back(batch);

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});
  config->set(dwrf::Config::ROW_INDEX_STRIDE, strideSize);
  config->set(
      dwrf::Config::MAP_FLAT_DISABLE_DICT_ENCODING,
      !enableFlatmapDictionaryEncoding);
  config->set(dwrf::Config::MAP_FLAT_DICT_SHARE, shareDictionary);

  dwrf::E2EWriterTestUtil::testWriter(
      *pool,
      type,
      batches,
      1,
      1,
      config,
      dwrf::E2EWriterTestUtil::simpleFlushPolicyFactory(false));
}

TEST_F(E2EWriterTest, FlatMapWithNulls) {
  testFlatMapWithNulls(
      /*firstRowNotNull=*/false, /*enableFlatmapDictionaryEncoding=*/false);
  testFlatMapWithNulls(
      /*firstRowNotNull=*/true, /*enableFlatmapDictionaryEncoding=*/false);
  testFlatMapWithNulls(
      /*firstRowNotNull=*/false, /*enableFlatmapDictionaryEncoding=*/true);
  testFlatMapWithNulls(
      /*firstRowNotNull=*/true, /*enableFlatmapDictionaryEncoding=*/true);
}

TEST_F(E2EWriterTest, FlatMapWithNullsSharedDict) {
  testFlatMapWithNulls(
      /*firstRowNotNull=*/false,
      /*enableFlatmapDictionaryEncoding=*/true,
      /*shareDictionary=*/true);
  testFlatMapWithNulls(
      /*firstRowNotNull=*/true,
      /*enableFlatmapDictionaryEncoding=*/true,
      /*shareDictionary=*/true);
}

TEST_F(E2EWriterTest, FlatMapEmpty) {
  auto pool = memory::memoryManager()->addLeafPool();

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
  auto batch = createRowVector(
      pool.get(), type, rowCount, b::create(*pool, std::move(rows)));
  batches.push_back(batch);

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});
  config->set(dwrf::Config::ROW_INDEX_STRIDE, strideSize);

  dwrf::E2EWriterTestUtil::testWriter(
      *pool,
      type,
      batches,
      1,
      1,
      config,
      dwrf::E2EWriterTestUtil::simpleFlushPolicyFactory(false));
}

TEST_F(E2EWriterTest, FlatMapConfigSingleColumn) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "map_val:map<bigint,double>,"
      ">");

  testFlatMapConfig(type, {0}, {1});
  testFlatMapConfig(type, {}, {});
}

TEST_F(E2EWriterTest, FlatMapConfigMixedTypes) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "map_val:map<bigint,double>,"
      ">");

  testFlatMapConfig(type, {1}, {2});
  testFlatMapConfig(type, {}, {});
}

TEST_F(E2EWriterTest, FlatMapConfigNestedMap) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "map_val:map<bigint,map<string,float>>,"
      ">");

  testFlatMapConfig(type, {1}, {2});
  testFlatMapConfig(type, {}, {});
}

TEST_F(E2EWriterTest, FlatMapConfigMixedMaps) {
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

TEST_F(E2EWriterTest, FlatMapConfigNotMapColumn) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "map_val:map<bigint,double>,"
      ">");

  EXPECT_THROW(
      { testFlatMapConfig(type, {0}, {}); }, exception::LoggedException);
}

TEST_F(E2EWriterTest, mapStatsSingleStride) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "map_val:map<bigint,int>,"
      "map_val:map<bigint,double>,"
      "map_val:map<bigint,map<bigint,bigint>>,"
      "map_val:map<bigint,map<bigint,double>>,"
      "map_val:map<bigint,array<bigint>>,"
      "map_val:map<bigint,map<string,float>>,"
      ">");

  // Single column
  testFlatMapFileStats(type, {0});
  // All non-nested columns
  testFlatMapFileStats(type, {0, 1});
  // All columns
  testFlatMapFileStats(type, {0, 1, 2, 3, 4, 5});
}

TEST_F(E2EWriterTest, mapStatsMultiStrides) {
  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "map_val:map<bigint,int>,"
      "map_val:map<bigint,double>,"
      "map_val:map<bigint,map<bigint,bigint>>,"
      "map_val:map<bigint,map<bigint,double>>,"
      "map_val:map<bigint,array<bigint>>,"
      "map_val:map<bigint,map<string,float>>,"
      ">");

  // Single column
  testFlatMapFileStats(type, {0}, /*strideSize=*/1000);
  // All non-nested columns
  testFlatMapFileStats(type, {0, 1}, /*strideSize=*/1000);
  // All columns
  testFlatMapFileStats(type, {0, 1, 2, 3, 4, 5}, /*strideSize=*/1000);
}

TEST_F(E2EWriterTest, PartialStride) {
  auto type = ROW({"bool_val"}, {INTEGER()});

  size_t batchSize = 1'000;

  auto config = std::make_shared<dwrf::Config>();
  auto sink = std::make_unique<MemorySink>(
      2 * 1024 * 1024,
      dwio::common::FileSink::Options{.pool = leafPool_.get()});
  auto sinkPtr = sink.get();

  dwrf::WriterOptions options;
  options.config = config;
  options.schema = type;
  options.memoryPool = rootPool_.get();
  dwrf::Writer writer{std::move(sink), options};

  auto nulls = allocateNulls(batchSize, leafPool_.get());
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  size_t nullCount = 0;

  auto values = AlignedBuffer::allocate<int32_t>(batchSize, leafPool_.get());
  auto* valuesPtr = values->asMutable<int32_t>();

  for (size_t i = 0; i < batchSize; ++i) {
    if ((i & 1) == 0) {
      bits::clearNull(nullsPtr, i);
      valuesPtr[i] = i;
    } else {
      bits::setNull(nullsPtr, i);
      nullCount++;
    }
  }

  auto batch = createRowVector(
      leafPool_.get(),
      type,
      batchSize,
      std::make_shared<FlatVector<int32_t>>(
          leafPool_.get(),
          type->childAt(0),
          nulls,
          batchSize,
          values,
          std::vector<BufferPtr>()));

  writer.write(batch);
  writer.close();

  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  RowReaderOptions rowReaderOpts;
  auto reader = createReader(*sinkPtr, readerOpts);
  ASSERT_EQ(
      batchSize - nullCount, reader->columnStatistics(1)->getNumberOfValues())
      << reader->columnStatistics(1)->toString();
  ASSERT_EQ(true, reader->columnStatistics(1)->hasNull().value());
}

TEST_F(E2EWriterTest, OversizeRows) {
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

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
  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::DISABLE_LOW_MEMORY_MODE, true);
  config->set(dwrf::Config::STRIPE_SIZE, 10 * kSizeMB);
  config->set(
      dwrf::Config::RAW_DATA_SIZE_PER_BATCH, folly::to<uint64_t>(20 * 1024UL));

  // Retained bytes in vector: 44704
  auto singleBatch = dwrf::E2EWriterTestUtil::generateBatches(
      type, 1, 1, /*seed=*/1411367325, *pool);

  dwrf::E2EWriterTestUtil::testWriter(
      *pool,
      type,
      singleBatch,
      1,
      1,
      config,
      /*flushPolicyFactory=*/nullptr,
      /*layoutPlannerFactory=*/nullptr,
      /*memoryBudget=*/std::numeric_limits<int64_t>::max(),
      false);
}

TEST_F(E2EWriterTest, OversizeBatches) {
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "bool_val:boolean,"
      "byte_val:tinyint,"
      "float_val:float,"
      "double_val:double,"
      ">");
  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::DISABLE_LOW_MEMORY_MODE, true);
  config->set(dwrf::Config::STRIPE_SIZE, 10 * kSizeMB);

  // Test splitting a gigantic batch.
  auto singleBatch = dwrf::E2EWriterTestUtil::generateBatches(
      type, 1, 10000000, /*seed=*/1411367325, *pool);
  // A gigantic batch is split into 10 stripes.
  dwrf::E2EWriterTestUtil::testWriter(
      *pool,
      type,
      singleBatch,
      10,
      10,
      config,
      /*flushPolicyFactory=*/nullptr,
      /*layoutPlannerFactory=*/nullptr,
      /*memoryBudget=*/std::numeric_limits<int64_t>::max(),
      false);

  // Test splitting multiple huge batches.
  auto batches = dwrf::E2EWriterTestUtil::generateBatches(
      type, 3, 5000000, /*seed=*/1411367325, *pool);
  // 3 gigantic batches are split into 15~16 stripes.
  dwrf::E2EWriterTestUtil::testWriter(
      *pool,
      type,
      batches,
      15,
      16,
      config,
      /*flushPolicyFactory=*/nullptr,
      /*layoutPlannerFactory=*/nullptr,
      /*memoryBudget=*/std::numeric_limits<int64_t>::max(),
      false);
}

TEST_F(E2EWriterTest, OverflowLengthIncrements) {
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  HiveTypeParser parser;
  auto type = parser.parse(
      "struct<"
      "struct_val:struct<bigint_val:bigint>"
      ">");
  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::DISABLE_LOW_MEMORY_MODE, true);
  config->set(dwrf::Config::STRIPE_SIZE, 10 * kSizeMB);
  config->set(
      dwrf::Config::RAW_DATA_SIZE_PER_BATCH,
      folly::to<uint64_t>(500 * 1024UL * 1024UL));

  const size_t batchSize = 1024;

  auto nulls =
      AlignedBuffer::allocate<char>(bits::nbytes(batchSize), pool.get());
  auto* nullsPtr = nulls->asMutable<uint64_t>();
  for (size_t i = 0; i < batchSize; ++i) {
    // Only the first element is non-null
    bits::setNull(nullsPtr, i, i != 0);
  }

  // Bigint column
  VectorMaker maker{pool.get()};
  auto child = maker.flatVector<int64_t>(std::vector<int64_t>{1UL});

  std::vector<VectorPtr> children{child};
  auto rowVec = std::make_shared<RowVector>(
      pool.get(),
      type->childAt(0),
      nulls,
      batchSize,
      children,
      /*nullCount=*/batchSize - 1);

  // Retained bytes in vector: 192, which is much less than 1024
  auto vec = std::make_shared<RowVector>(
      pool.get(),
      type,
      BufferPtr{},
      batchSize,
      std::vector<VectorPtr>{rowVec},
      /*nullCount=*/0);

  dwrf::E2EWriterTestUtil::testWriter(
      *pool,
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

class E2EEncryptionTest : public E2EWriterTest {
 protected:
  E2EEncryptionTest() : E2EWriterTest() {}

  std::unique_ptr<::facebook::velox::dwrf::DwrfReader> writeAndRead(
      const std::string& schema,
      const std::shared_ptr<EncryptionSpecification>& spec,
      std::shared_ptr<DecrypterFactory> decrypterFactory =
          std::make_shared<TestDecrypterFactory>()) {
    HiveTypeParser parser;
    auto type = parser.parse(schema);

    // write file to memory
    auto config = std::make_shared<::facebook::velox::dwrf::Config>();
    // make sure we always write dictionary to test stride index
    config->set(
        ::facebook::velox::dwrf::Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD,
        1.0f);
    config->set(
        ::facebook::velox::dwrf::Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD,
        0.0f);
    auto sink = std::make_unique<MemorySink>(
        16 * 1024 * 1024,
        dwio::common::FileSink::Options{.pool = leafPool_.get()});
    sink_ = sink.get();
    ::facebook::velox::dwrf::WriterOptions options;
    options.config = config;
    options.schema = type;
    options.encryptionSpec = spec;
    options.encrypterFactory = std::make_shared<TestEncrypterFactory>();
    writer_ =
        std::make_unique<dwrf::Writer>(std::move(sink), options, rootPool_);

    for (size_t i = 0; i < batchCount_; ++i) {
      auto batch =
          BatchMaker::createBatch(type, batchSize_, *leafPool_, nullptr, i);
      writer_->write(batch);
      batches_.push_back(std::move(batch));
      if (i % flushInterval_ == flushInterval_ - 1) {
        writer_->flush();
      }
    }
    writer_->close();

    // read it back for compare
    dwio::common::ReaderOptions readerOpts{leafPool_.get()};
    readerOpts.setDecrypterFactory(decrypterFactory);
    return createReader(*sink_, readerOpts);
  }

  const DecryptionHandler& getDecryptionHandler(
      const ::facebook::velox::dwrf::DwrfReader& reader) const {
    return reader.testingReaderBase()->getDecryptionHandler();
  }

  void validateFileContent(
      const ::facebook::velox::dwrf::DwrfReader& reader) const {
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
  auto dwrfRowReader =
      dynamic_cast<::facebook::velox::dwrf::DwrfRowReader*>(rowReader.get());
  bool preload = true;
  for (int32_t i = 0; i < reader->getNumberOfStripes(); ++i) {
    auto stripeMetadata = dwrfRowReader->fetchStripe(i, preload);
    auto& sf = *stripeMetadata->footer;
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
  auto dwrfRowReader = dynamic_cast<dwrf::DwrfRowReader*>(rowReader.get());
  bool preload = true;
  for (int32_t i = 0; i < reader->getNumberOfStripes(); ++i) {
    auto stripeMetadata = dwrfRowReader->fetchStripe(i, preload);
    auto& sf = *stripeMetadata->footer;
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
    ASSERT_THROW(
        reader->createRowReader(rowReaderOpts), exception::LoggedException);
  }
}

void testWriter(
    MemoryPool& pool,
    const std::shared_ptr<const Type>& type,
    size_t batchCount,
    std::function<VectorPtr()> generator,
    const std::shared_ptr<dwrf::Config> config =
        std::make_shared<dwrf::Config>()) {
  std::vector<VectorPtr> batches;
  for (auto i = 0; i < batchCount; ++i) {
    batches.push_back(generator());
  }
  dwrf::E2EWriterTestUtil::testWriter(pool, type, batches, 1, 1, config);
};

template <TypeKind K>
VectorPtr createKeysImpl(
    MemoryPool& pool,
    std::mt19937& rng,
    size_t batchSize,
    size_t maxVal) {
  using TCpp = typename TypeTraits<K>::NativeType;

  auto vector = std::make_shared<FlatVector<TCpp>>(
      &pool,
      CppToType<TCpp>::create(),
      nullptr,
      batchSize,
      AlignedBuffer::allocate<TCpp>(batchSize, &pool),
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
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(batchSize, &pool);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (size_t i = 0; i < batchSize; ++i) {
    rawIndices[i] = batchSize - 1 - i;
  }

  return BaseVector::wrapInDictionary(nullptr, indices, batchSize, vector);
}

VectorPtr createKeys(
    const std::shared_ptr<const Type> type,
    MemoryPool& pool,
    std::mt19937& rng,
    size_t batchSize,
    size_t maxVal) {
  switch (type->kind()) {
    case TypeKind::INTEGER:
      return createKeysImpl<TypeKind::INTEGER>(pool, rng, batchSize, maxVal);
    case TypeKind::VARCHAR:
      return createKeysImpl<TypeKind::VARCHAR>(pool, rng, batchSize, maxVal);
    default:
      folly::assume_unreachable();
  }
}

TEST_F(E2EWriterTest, fuzzSimple) {
  auto pool = memory::memoryManager()->addLeafPool();
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
      pool.get(),
      seed);

  VectorFuzzer hasNulls{
      {
          .vectorSize = batchSize,
          .nullRatio = 0.05,
          .stringLength = 10,
          .stringVariableLength = true,
      },
      pool.get(),
      seed};

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    testWriter(
        *pool, type, batches, [&]() { return noNulls.fuzzInputRow(type); });
    testWriter(
        *pool, type, batches, [&]() { return hasNulls.fuzzInputRow(type); });
  }
}

TEST_F(E2EWriterTest, fuzzComplex) {
  auto pool = memory::memoryManager()->addLeafPool();
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
      pool.get(),
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
      pool.get(),
      seed};

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    testWriter(
        *pool, type, batches, [&]() { return noNulls.fuzzInputRow(type); });
    testWriter(
        *pool, type, batches, [&]() { return hasNulls.fuzzInputRow(type); });
  }
}

TEST_F(E2EWriterTest, fuzzFlatmap) {
  auto pool = memory::memoryManager()->addLeafPool();
  auto type = ROW({
      {"flatmap1", MAP(INTEGER(), REAL())},
      {"flatmap2", MAP(VARCHAR(), ARRAY(REAL()))},
      {"flatmap3", MAP(INTEGER(), MAP(INTEGER(), REAL()))},
  });
  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0, 1, 2});
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
      pool.get(),
      seed);

  auto genMap = [&](auto type, auto size) {
    auto offsets = allocateOffsets(batchSize, pool.get());
    auto rawOffsets = offsets->template asMutable<vector_size_t>();
    auto sizes = allocateSizes(batchSize, pool.get());
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
        pool.get(),
        seed);

    auto& mapType = type->asMap();
    VectorPtr vector = std::make_shared<MapVector>(
        pool.get(),
        type,
        nullptr,
        batchSize,
        offsets,
        sizes,
        createKeys(mapType.keyType(), *pool, rng, childSize, 10),
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
        pool.get(), type, nullptr, batchSize, std::move(children));
  };

  auto iterations = 20;
  auto batches = 20;
  for (auto i = 0; i < iterations; ++i) {
    testWriter(*pool, type, batches, gen, config);
  }
}

TEST_F(E2EWriterTest, memoryConfigError) {
  const auto type = ROW(
      {{"int_val", INTEGER()},
       {"string_val", VARCHAR()},
       {"binary_val", VARBINARY()}});

  dwrf::WriterOptions options;
  options.schema = type;
  const common::SpillConfig spillConfig = getSpillConfig(10, 20);
  options.spillConfig = &spillConfig;
  auto writerPool = memory::memoryManager()->addRootPool(
      "memoryReclaim", 1L << 30, exec::MemoryReclaimer::create());
  auto dwrfPool = writerPool->addAggregateChild("writer");
  auto sinkPool =
      writerPool->addLeafChild("sink", true, exec::MemoryReclaimer::create());
  auto sink = std::make_unique<MemorySink>(
      200 * 1024 * 1024, FileSink::Options{.pool = sinkPool.get()});
  VELOX_ASSERT_THROW(
      std::make_unique<dwrf::Writer>(std::move(sink), options, dwrfPool),
      "nonReclaimableSection_ must be set if writer memory reclaim is enabled");
}

DEBUG_ONLY_TEST_F(E2EWriterTest, memoryReclaimOnWrite) {
  const auto type = ROW(
      {{"int_val", INTEGER()},
       {"string_val", VARCHAR()},
       {"binary_val", VARBINARY()}});

  VectorFuzzer fuzzer(
      {
          .vectorSize = 1000,
          .stringLength = 1'000,
          .stringVariableLength = false,
      },
      leafPool_.get());

  std::vector<VectorPtr> vectors;
  for (int i = 0; i < 10; ++i) {
    vectors.push_back(fuzzer.fuzzInputRow(type));
  }
  const common::SpillConfig spillConfig = getSpillConfig(10, 20);
  for (bool enableReclaim : {false, true}) {
    SCOPED_TRACE(fmt::format("enableReclaim {}", enableReclaim));

    auto config = std::make_shared<dwrf::Config>();
    config->set<uint64_t>(dwrf::Config::STRIPE_SIZE, 1L << 30);
    config->set<uint64_t>(dwrf::Config::MAX_DICTIONARY_SIZE, 1L << 30);

    tsan_atomic<bool> nonReclaimableSection{false};
    dwrf::WriterOptions options;
    options.schema = type;
    options.config = std::move(config);
    options.nonReclaimableSection = &nonReclaimableSection;
    if (enableReclaim) {
      options.spillConfig = &spillConfig;
    }
    auto writerPool = memory::memoryManager()->addRootPool(
        "memoryReclaim", 1L << 30, exec::MemoryReclaimer::create());
    auto dwrfPool = writerPool->addAggregateChild("writer");
    auto sinkPool =
        writerPool->addLeafChild("sink", true, exec::MemoryReclaimer::create());
    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024, FileSink::Options{.pool = sinkPool.get()});

    SCOPED_TESTVALUE_SET(
        "facebook::velox::dwrf::Writer::write",
        std::function<void(dwrf::Writer*)>([&](dwrf::Writer* writer) {
          VELOX_CHECK(writer->testingNonReclaimableSection());
          ASSERT_EQ(writer->canReclaim(), enableReclaim);
          if (!writer->canReclaim()) {
            return;
          }
          auto& context = writer->getContext();
          const auto memoryUsage = context.getTotalMemoryUsage();
          const auto availableMemoryUsage =
              context.availableMemoryReservation();
          ASSERT_GE(
              availableMemoryUsage,
              memoryUsage * spillConfig.minSpillableReservationPct / 100);
        }));

    auto writer =
        std::make_unique<dwrf::Writer>(std::move(sink), options, dwrfPool);

    std::atomic<bool> reservationCalled{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
        std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
          ASSERT_TRUE(enableReclaim);
          reservationCalled = true;
          // Verify the writer is reclaimable under memory reservation.
          ASSERT_FALSE(writer->testingNonReclaimableSection());
        }));

    memory::MemoryReclaimer::Stats stats;
    const auto oldCapacity = writerPool->capacity();
    const auto oldReservedBytes = writerPool->reservedBytes();
    const auto oldUsedBytes = writerPool->usedBytes();
    writerPool->reclaim(1L << 30, 0, stats);
    ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
    // We don't expect the capacity change by memory reclaim but only the used
    // or reserved memory change.
    ASSERT_EQ(writerPool->capacity(), oldCapacity);
    if (enableReclaim) {
      // The writer is empty so nothing to free.
      ASSERT_EQ(stats.reclaimedBytes, 0);
      ASSERT_GE(stats.reclaimExecTimeUs, 0);
      ASSERT_EQ(
          oldReservedBytes - writerPool->reservedBytes(), stats.reclaimedBytes);
      ASSERT_EQ(oldUsedBytes, writerPool->usedBytes());
    } else {
      ASSERT_EQ(stats, memory::MemoryReclaimer::Stats{});
    }

    // Expect a throw if we don't set the non-reclaimable section.
    VELOX_ASSERT_THROW(writer->write(vectors[0]), "");
    {
      memory::NonReclaimableSectionGuard nonReclaimableGuard(
          &nonReclaimableSection);
      for (size_t i = 0; i < vectors.size(); ++i) {
        writer->write(vectors[i]);
      }
    }
    if (!enableReclaim) {
      ASSERT_FALSE(reservationCalled);
      ASSERT_EQ(writerPool->reclaim(1L << 30, 0, stats), 0);
      ASSERT_EQ(stats, memory::MemoryReclaimer::Stats{});
    } else {
      ASSERT_TRUE(reservationCalled);
      writer->testingNonReclaimableSection() = true;
      ASSERT_EQ(writerPool->reclaim(1L << 30, 0, stats), 0);
      ASSERT_EQ(stats.numNonReclaimableAttempts, 1);
      writer->testingNonReclaimableSection() = false;
      stats.numNonReclaimableAttempts = 0;
      const auto reclaimedBytes = writerPool->reclaim(1L << 30, 0, stats);
      ASSERT_GT(reclaimedBytes, 0);
      ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
      ASSERT_GT(stats.reclaimedBytes, 0);
      ASSERT_GT(stats.reclaimExecTimeUs, 0);
    }
    writer->close();
  }
}

DEBUG_ONLY_TEST_F(E2EWriterTest, memoryReclaimOnFlush) {
  const auto type = ROW(
      {{"int_val", INTEGER()},
       {"string_val", VARCHAR()},
       {"binary_val", VARBINARY()}});

  VectorFuzzer fuzzer(
      {
          .vectorSize = 1000,
          .stringLength = 1'000,
          .stringVariableLength = false,
      },
      leafPool_.get());
  std::vector<VectorPtr> vectors;
  for (int i = 0; i < 10; ++i) {
    vectors.push_back(fuzzer.fuzzInputRow(type));
  }
  const common::SpillConfig spillConfig = getSpillConfig(10, 20);
  for (bool enableReclaim : {false, true}) {
    SCOPED_TRACE(fmt::format("enableReclaim {}", enableReclaim));

    auto config = std::make_shared<dwrf::Config>();
    config->set<uint64_t>(dwrf::Config::STRIPE_SIZE, 1L << 30);
    config->set<uint64_t>(dwrf::Config::MAX_DICTIONARY_SIZE, 1L << 30);

    dwrf::WriterOptions options;
    options.schema = type;
    options.config = std::move(config);
    tsan_atomic<bool> nonReclaimableSection{false};
    options.nonReclaimableSection = &nonReclaimableSection;
    if (enableReclaim) {
      options.spillConfig = &spillConfig;
    }
    auto writerPool = memory::memoryManager()->addRootPool(
        "memoryReclaim", 1L << 30, exec::MemoryReclaimer::create());
    auto dwrfPool = writerPool->addAggregateChild("writer");
    auto sinkPool =
        writerPool->addLeafChild("sink", true, exec::MemoryReclaimer::create());
    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024, FileSink::Options{.pool = sinkPool.get()});

    SCOPED_TESTVALUE_SET(
        "facebook::velox::dwrf::Writer::flushStripe",
        std::function<void(dwrf::Writer*)>([&](dwrf::Writer* writer) {
          VELOX_CHECK(writer->testingNonReclaimableSection());
          ASSERT_EQ(writer->canReclaim(), enableReclaim);
          if (!writer->canReclaim()) {
            return;
          }
          auto& context = writer->getContext();
          auto& outputPool =
              context.getMemoryPool(dwrf::MemoryUsageCategory::OUTPUT_STREAM);
          const auto memoryUsage = outputPool.usedBytes();
          const auto availableMemoryUsage = outputPool.availableReservation();
          ASSERT_GE(
              availableMemoryUsage,
              memoryUsage * spillConfig.minSpillableReservationPct / 100);
        }));

    auto writer =
        std::make_unique<dwrf::Writer>(std::move(sink), options, dwrfPool);

    std::atomic<bool> reservationCalled{false};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
        std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
          ASSERT_TRUE(enableReclaim);
          reservationCalled = true;
          // Verify the writer is reclaimable under memory reservation.
          ASSERT_FALSE(writer->testingNonReclaimableSection());
        }));

    {
      memory::NonReclaimableSectionGuard nonReclaimableGuard(
          &nonReclaimableSection);
      for (size_t i = 0; i < vectors.size(); ++i) {
        writer->write(vectors[i]);
      }
      writer->flush();
    }
    ASSERT_EQ(reservationCalled, enableReclaim);
    writer->close();
  }
}

TEST_F(E2EWriterTest, memoryReclaimAfterClose) {
  const auto type = ROW(
      {{"int_val", INTEGER()},
       {"string_val", VARCHAR()},
       {"binary_val", VARBINARY()}});

  VectorFuzzer fuzzer(
      {
          .vectorSize = 1000,
          .stringLength = 1'000,
          .stringVariableLength = false,
      },
      leafPool_.get());
  std::vector<VectorPtr> vectors;
  for (int i = 0; i < 10; ++i) {
    vectors.push_back(fuzzer.fuzzInputRow(type));
  }

  const common::SpillConfig spillConfig = getSpillConfig(10, 20);
  struct {
    bool canReclaim;
    bool abort;
    bool expectedNonReclaimableAttempt;

    std::string debugString() const {
      return fmt::format(
          "canReclaim: {}, abort: {}, expectedNonReclaimableAttempt: {}",
          canReclaim,
          abort,
          expectedNonReclaimableAttempt);
    }
  } testSettings[] = {
      {true, true, true},
      {true, false, true},
      {false, true, false},
      {false, false, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto config = std::make_shared<dwrf::Config>();
    config->set<uint64_t>(dwrf::Config::STRIPE_SIZE, 1L << 30);
    config->set<uint64_t>(dwrf::Config::MAX_DICTIONARY_SIZE, 1L << 30);

    dwrf::WriterOptions options;
    options.schema = type;
    options.config = std::move(config);
    tsan_atomic<bool> nonReclaimableSection{false};
    options.nonReclaimableSection = &nonReclaimableSection;
    if (testData.canReclaim) {
      options.spillConfig = &spillConfig;
    }
    auto writerPool = memory::memoryManager()->addRootPool(
        "memoryReclaim", 1L << 30, exec::MemoryReclaimer::create());
    auto dwrfPool = writerPool->addAggregateChild("writer");
    auto sinkPool =
        writerPool->addLeafChild("sink", true, exec::MemoryReclaimer::create());
    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024, FileSink::Options{.pool = sinkPool.get()});

    auto writer =
        std::make_unique<dwrf::Writer>(std::move(sink), options, dwrfPool);
    ASSERT_EQ(writer->state(), dwio::common::Writer::State::kRunning);
    ASSERT_EQ(writer->canReclaim(), testData.canReclaim);

    writer->flush();

    {
      memory::NonReclaimableSectionGuard nonReclaimableGuard(
          &nonReclaimableSection);
      for (size_t i = 0; i < vectors.size(); ++i) {
        writer->write(vectors[i]);
      }
    }

    if (testData.abort) {
      writer->abort();
      VELOX_ASSERT_THROW(writer->abort(), "Writer is not running: ABORTED");
      VELOX_ASSERT_THROW(writer->close(), "Writer is not running: ABORTED");
    } else {
      writer->close();
      VELOX_ASSERT_THROW(writer->abort(), "Writer is not running: CLOSED");
      VELOX_ASSERT_THROW(writer->close(), "Writer is not running: CLOSED");
    }
    // Verify append or write after close or abort will fail.
    VELOX_ASSERT_THROW(writer->write(vectors[0]), "Writer is not running");
    VELOX_ASSERT_THROW(writer->flush(), "Writer is not running");

    memory::MemoryReclaimer::Stats stats;
    const auto oldCapacity = writerPool->capacity();
    writerPool->reclaim(1L << 30, 0, stats);
    if (testData.abort || !testData.canReclaim) {
      ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
    } else {
      ASSERT_EQ(stats.numNonReclaimableAttempts, 1);
    }
    // Reclaim does not happen as the writer is either aborted or closed.
    ASSERT_EQ(stats.reclaimExecTimeUs, 0);
    ASSERT_EQ(stats.reclaimedBytes, 0);
  }
}

DEBUG_ONLY_TEST_F(E2EWriterTest, memoryReclaimDuringInit) {
  const auto type = ROW(
      {{"int_val", INTEGER()},
       {"string_val", VARCHAR()},
       {"binary_val", VARBINARY()}});

  VectorFuzzer fuzzer(
      {
          .vectorSize = 1000,
          .stringLength = 1'000,
          .stringVariableLength = false,
      },
      leafPool_.get());

  const common::SpillConfig spillConfig = getSpillConfig(10, 20);
  for (const auto& reclaimable : {false, true}) {
    SCOPED_TRACE(fmt::format("reclaimable {}", reclaimable));

    auto config = std::make_shared<dwrf::Config>();
    config->set<uint64_t>(dwrf::Config::STRIPE_SIZE, 1L << 30);
    config->set<uint64_t>(dwrf::Config::MAX_DICTIONARY_SIZE, 1L << 30);

    dwrf::WriterOptions options;
    options.schema = type;
    options.config = std::move(config);
    tsan_atomic<bool> nonReclaimableSection{false};
    options.nonReclaimableSection = &nonReclaimableSection;
    if (reclaimable) {
      options.spillConfig = &spillConfig;
    }
    auto writerPool = memory::memoryManager()->addRootPool(
        "memoryReclaimDuringInit", 1L << 30, exec::MemoryReclaimer::create());
    auto dwrfPool = writerPool->addAggregateChild("writer");
    auto sinkPool =
        writerPool->addLeafChild("sink", true, exec::MemoryReclaimer::create());
    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024, FileSink::Options{.pool = sinkPool.get()});

    SCOPED_TESTVALUE_SET(
        "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
        std::function<void(MemoryPool*)>([&](MemoryPool* /*unused*/) {
          auto reclaimableBytesOpt = writerPool->reclaimableBytes();
          ASSERT_EQ(reclaimableBytesOpt.has_value(), reclaimable);

          memory::MemoryReclaimer::Stats stats;
          writerPool->reclaim(1L << 30, 0, stats);
          if (reclaimable) {
            ASSERT_GE(reclaimableBytesOpt.value(), 0);
            // We can't reclaim during writer init.
            ASSERT_LE(stats.numNonReclaimableAttempts, 1);
            ASSERT_EQ(stats.reclaimedBytes, 0);
            ASSERT_EQ(stats.reclaimExecTimeUs, 0);
          } else {
            ASSERT_EQ(stats, memory::MemoryReclaimer::Stats{});
          }
        }));

    std::unique_ptr<dwrf::Writer> writer;
    {
      memory::NonReclaimableSectionGuard nonReclaimableGuard(
          &nonReclaimableSection);
      std::thread writerThread([&]() {
        writer =
            std::make_unique<dwrf::Writer>(std::move(sink), options, dwrfPool);
      });

      writerThread.join();
    }
    ASSERT_TRUE(writer != nullptr);
    ASSERT_EQ(writer->canReclaim(), reclaimable);
    writer->close();
  }
}

TEST_F(E2EWriterTest, memoryReclaimThreshold) {
  const auto type = ROW(
      {{"int_val", INTEGER()},
       {"string_val", VARCHAR()},
       {"binary_val", VARBINARY()}});

  VectorFuzzer fuzzer(
      {
          .vectorSize = 1000,
          .stringLength = 1'000,
          .stringVariableLength = false,
      },
      leafPool_.get());
  std::vector<VectorPtr> vectors;
  for (int i = 0; i < 10; ++i) {
    vectors.push_back(fuzzer.fuzzInputRow(type));
  }
  const std::vector<uint64_t> writerFlushThresholdSizes = {0, 1L << 30};
  for (uint64_t writerFlushThresholdSize : writerFlushThresholdSizes) {
    SCOPED_TRACE(fmt::format(
        "writerFlushThresholdSize {}",
        succinctBytes(writerFlushThresholdSize)));

    const common::SpillConfig spillConfig =
        getSpillConfig(10, 20, writerFlushThresholdSize);
    auto config = std::make_shared<dwrf::Config>();
    config->set<uint64_t>(dwrf::Config::STRIPE_SIZE, 1L << 30);
    config->set<uint64_t>(dwrf::Config::MAX_DICTIONARY_SIZE, 1L << 30);

    dwrf::WriterOptions options;
    options.schema = type;
    options.config = std::move(config);
    tsan_atomic<bool> nonReclaimableSection{false};
    options.nonReclaimableSection = &nonReclaimableSection;
    options.spillConfig = &spillConfig;

    auto writerPool = memory::memoryManager()->addRootPool(
        "memoryReclaimThreshold", 1L << 30, exec::MemoryReclaimer::create());
    auto dwrfPool = writerPool->addAggregateChild("writer");
    auto sinkPool =
        writerPool->addLeafChild("sink", true, exec::MemoryReclaimer::create());
    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024, FileSink::Options{.pool = sinkPool.get()});

    auto writer =
        std::make_unique<dwrf::Writer>(std::move(sink), options, dwrfPool);

    {
      memory::NonReclaimableSectionGuard nonReclaimableGuard(
          &nonReclaimableSection);
      for (size_t i = 0; i < vectors.size(); ++i) {
        writer->write(vectors[i]);
      }
    }

    uint64_t reclaimableBytes{0};
    memory::MemoryReclaimer::Stats stats;
    if (writerFlushThresholdSize == 0) {
      ASSERT_TRUE(writerPool->reclaimer()->reclaimableBytes(
          *writerPool, reclaimableBytes));
      ASSERT_GT(reclaimableBytes, 0);
      ASSERT_GT(writerPool->reclaim(1L << 30, 0, stats), 0);
      ASSERT_GT(stats.reclaimExecTimeUs, 0);
      ASSERT_GT(stats.reclaimedBytes, 0);
    } else {
      ASSERT_FALSE(writerPool->reclaimer()->reclaimableBytes(
          *writerPool, reclaimableBytes));
      ASSERT_EQ(reclaimableBytes, 0);
      ASSERT_EQ(writerPool->reclaim(1L << 30, 0, stats), 0);
      ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
      ASSERT_EQ(stats.reclaimExecTimeUs, 0);
      ASSERT_EQ(stats.reclaimedBytes, 0);
    }
    writer->flush();
    writer->close();
  }
}
} // namespace
