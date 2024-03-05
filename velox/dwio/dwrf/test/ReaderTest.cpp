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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <velox/buffer/Buffer.h>
#include "folly/Random.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/lang/Assume.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/ExecutorBarrier.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/dwrf/test/utils/E2EWriterTestUtil.h"
#include "velox/type/Type.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <fmt/core.h>
#include <array>
#include <future>
#include <memory>
#include <numeric>

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::type::fbhive;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::test;

namespace {
const std::string& getStructFile() {
  static const std::string structFile_ = getExampleFilePath("struct.orc");
  return structFile_;
}

const std::string& getFMSmallFile() {
  static const std::string fmSmallFile_ = getExampleFilePath("fm_small.orc");
  return fmSmallFile_;
}

const std::string& getFMLargeFile() {
  static const std::string fmLargeFile_ = getExampleFilePath("fm_large.orc");
  return fmLargeFile_;
}

// RowType for fmSmallFile and fmLargeFile
const std::shared_ptr<const RowType>& getFlatmapSchema() {
  static const std::shared_ptr<const RowType> schema_ =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
         id:int,\
     map1:map<int, array<float>>,\
     map2:map<string, map<smallint,bigint>>,\
     map3:map<int,int>,\
     map4:map<int,struct<field1:int,field2:float,field3:string>>,\
     memo:string>"));
  return schema_;
}

class TestReaderP
    : public testing::TestWithParam</* parallel decoding = */ bool>,
      public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  folly::Executor* executor() {
    if (GetParam() && !executor_) {
      std::make_shared<folly::CPUThreadPoolExecutor>(
          getDecodingParallelismFactor());
    }
    return executor_.get();
  }

  size_t getDecodingParallelismFactor() {
    return GetParam() ? 2 : 0;
  }

 private:
  std::unique_ptr<folly::Executor> executor_;
};

class TestReader : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::vector<VectorPtr> createBatches(
      const std::vector<std::vector<int32_t>>& values) {
    std::vector<VectorPtr> batches;
    for (const auto& value : values) {
      auto vector = makeFlatVector<int32_t>(value);
      auto rowVector = makeRowVector({vector});
      batches.push_back(rowVector);
    }
    return batches;
  }
};

class TestRowReaderPrefetch : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

class TestRowReaderPfetch : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

} // namespace

TEST_F(TestReader, testWriterVersions) {
  EXPECT_EQ("original", writerVersionToString(ORIGINAL));
  EXPECT_EQ("dwrf-4.9", writerVersionToString(DWRF_4_9));
  EXPECT_EQ("dwrf-5.0", writerVersionToString(DWRF_5_0));
  EXPECT_EQ("dwrf-6.0", writerVersionToString(DWRF_6_0));
  EXPECT_EQ(
      "future - 99", writerVersionToString(static_cast<WriterVersion>(99)));
}

std::unique_ptr<BufferedInput> createFileBufferedInput(
    const std::string& path,
    memory::MemoryPool& pool) {
  return std::make_unique<BufferedInput>(
      std::make_shared<LocalReadFile>(path), pool);
}

// Prefetches the entire range of the reader and verifies correctness in
// PrefetchUnits() API. Does not do any actual reading of the file.
void verifyPrefetch(
    DwrfRowReader* rowReader,
    const std::vector<uint32_t>& expectedPrefetchRowSizes = {},
    const std::vector<bool>& shouldTryPrefetch = {}) {
  auto prefetchUnitsOpt = rowReader->prefetchUnits();
  ASSERT_TRUE(prefetchUnitsOpt.has_value());
  auto prefetchUnits = std::move(prefetchUnitsOpt.value());
  auto numFetches = prefetchUnits.size();
  auto expectedResultsSize = shouldTryPrefetch.size();
  auto expectedRowsSize = expectedPrefetchRowSizes.size();
  bool shouldCheckResults = expectedResultsSize != 0;
  bool shouldCheckRowCount = expectedRowsSize != 0;

  // Empty vector will skip the check, but they should never been different than
  // actual expected prefetchUnits vector
  DWIO_ENSURE(expectedResultsSize == numFetches || !shouldCheckResults);
  DWIO_ENSURE(expectedRowsSize == numFetches || !shouldCheckRowCount);

  for (int i = 0; i < numFetches; i++) {
    if (shouldCheckRowCount) {
      EXPECT_EQ(prefetchUnits[i].rowCount, expectedPrefetchRowSizes[i]);
    }
    if (shouldCheckResults && shouldTryPrefetch[i]) {
      RowReader::FetchResult result = prefetchUnits[i].prefetch();
      EXPECT_EQ(
          result,
          // A prefetch request for the first stripe should be already fetched,
          // because createDwrfRowReader calls startNextStripe() synchronously.
          i == 0 ? RowReader::FetchResult::kAlreadyFetched
                 : RowReader::FetchResult::kFetched);
    }
  }
}

// This relies on schema and data inside of our fm_small and fm_large orc files,
// and is not composeable with other schema/datas
void verifyFlatMapReading(
    DwrfRowReader* rowReader,
    const int32_t seeks[],
    const int32_t expectedBatchSize[],
    const int32_t numBatches) {
  VectorPtr batch;
  int32_t batchId = 0;
  do {
    // for every read, it seek to the specified row
    if (seeks[batchId] > 0) {
      rowReader->seekToRow(seeks[batchId]);
    }

    bool result = rowReader->next(1000, batch);
    if (!result) {
      break;
    }

    // verify current batch
    auto root = batch->as<RowVector>();
    EXPECT_EQ(root->childrenSize(), 6);
    // 4 stripes -> 4 batches
    EXPECT_EQ(root->size(), expectedBatchSize[batchId++]);

    // try to read first map as map<int, list<float>>
    auto map1 = root->childAt(1)->as<MapVector>();
    auto map1KeyInt = map1->mapKeys()->as<SimpleVector<int32_t>>();
    auto map1ValueList = map1->mapValues();

    // print all the map vector based on offsets
    EXPECT_EQ(map1KeyInt->size(), map1ValueList->size());
    EXPECT_EQ(0, map1KeyInt->getNullCount().value());

    // try to verify map2 as map<string, map<smallint, bigint>>
    auto map2 = root->childAt(2)->as<MapVector>();
    auto map2Key = map2->mapKeys();
    FlatVectorPtr<StringView> map2KeyString =
        std::dynamic_pointer_cast<FlatVector<StringView>>(map2Key);
    auto map2ValueMap = map2->mapValues();
    EXPECT_EQ(map2KeyString->size(), map2ValueMap->size());
    EXPECT_EQ(0, map2KeyString->getNullCount().value());

    // data - map2 always has string keys "key-1" and "key-nullable"
    // key-1 always has value map {1:1}
    // value of "key-nullable" is either null or map {1:1}
    for (int32_t i = 0; i < map2->size(); ++i) {
      int64_t start = map2->offsetAt(i);
      int64_t end = start + map2->sizeAt(i);

      // map2 has at least key-1 and key-nullable
      EXPECT_GE(end - start, 2);

      // go through all the keys
      int32_t found = 0;
      while (start < end) {
        std::string keyStr = map2KeyString->valueAt(start).str();
        start++;

        if (keyStr == "key-1" || keyStr == "key-nullable") {
          found++;
        }
      }

      // these two keys should always present
      EXPECT_EQ(found, 2);
    }

    // try to verify map3 as map<int, int>
    auto map3 = root->childAt(3)->as<MapVector>();
    auto map3KeyInt = map3->mapKeys()->as<SimpleVector<int32_t>>();
    auto map3ValueInt = map3->mapValues()->as<SimpleVector<int32_t>>();

    EXPECT_EQ(map3KeyInt->size(), map3ValueInt->size());
    EXPECT_EQ(0, map3KeyInt->getNullCount().value());

    // try to verify map4 as
    // map<int,struct<field1:int,field2:float,field3:string>>
    auto map4 = root->childAt(4)->as<MapVector>();
    auto map4KeyInt = map4->mapKeys()->as<SimpleVector<int32_t>>();
    auto map4ValueStruct = map4->mapValues();

    EXPECT_EQ(map4KeyInt->size(), map4ValueStruct->size());
    EXPECT_EQ(0, map4KeyInt->getNullCount().value());

    // data - map4 always has 9 keys [0-8]
    // each key maps the a internal struct with all fields the same value as key
    EXPECT_EQ(map4->size() * 9, map4KeyInt->size());
  } while (true);

  // number of batches should match
  EXPECT_EQ(batchId, numBatches);
}

// schema of flat map sample file
// struct {
//   id int,
//   map1 map<int, array<float>>,
//   map2 map<varchar, map<smallint, bigint>>,
//   map3 map<int, int>,
//   map4 map<int, struct<field1 int, field2 float, field3 varchar>>,
//   memo varchar
// }
void verifyFlatMapReading(
    memory::MemoryPool* pool,
    const std::string& file,
    const int32_t seeks[],
    const int32_t expectedBatchSize[],
    const int32_t numBatches,
    bool returnFlatVector,
    const std::vector<uint32_t>& expectedPrefetchRowSizes = {},
    const std::vector<bool>& shouldTryPrefetch = {}) {
  dwio::common::ReaderOptions readerOpts{pool};

  /* If an extra sanity check is desired you can uncomment the 2 below lines and
   * re-run */
  // readerOpts.setFooterEstimatedSize(257);
  // readerOpts.setFilePreloadThreshold(0);

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setReturnFlatVector(returnFlatVector);
  rowReaderOpts.select(std::make_shared<ColumnSelector>(getFlatmapSchema()));
  auto reader = DwrfReader::create(
      createFileBufferedInput(file, readerOpts.getMemoryPool()), readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  // Prefetch the requested # of times
  verifyPrefetch(rowReader, expectedPrefetchRowSizes, shouldTryPrefetch);
  verifyFlatMapReading(rowReader, seeks, expectedBatchSize, numBatches);
}

/*
Verifies contents of dict_encoded_strings.orc
schema:
struct {
 int_column int,
 string_column string
 string_column_2 string
}
*/
void verifyCachedIndexStreamReads(
    DwrfRowReader* rowReader,
    uint32_t firstStripe,
    uint32_t pastLastStripe) {
  VectorPtr batch;

  if (firstStripe == 0 && pastLastStripe > 0) {
    // Stripe 1
    ASSERT_TRUE(rowReader->next(100, batch));
    auto root = batch->as<RowVector>();
    EXPECT_EQ(root->childrenSize(), 4);
    auto stringCol1 = root->childAt(1)->as<SimpleVector<StringView>>();
    auto stringCol2 = root->childAt(2)->as<SimpleVector<StringView>>();

    for (int i = 0; i < 50; i++) {
      ASSERT_EQ(stringCol1->valueAt(i), "baz");
      ASSERT_EQ(stringCol2->valueAt(i), "abcdefghijklmnop");
    }

    ASSERT_EQ(stringCol1->valueAt(50), "zax");
    ASSERT_EQ(stringCol2->valueAt(50), "unique");

    ASSERT_EQ(stringCol1->valueAt(51), "zax");
    ASSERT_EQ(stringCol2->valueAt(51), "different");

    ASSERT_EQ(stringCol1->valueAt(52), "zax");
    ASSERT_EQ(stringCol2->valueAt(52), "special");

    for (int i = 53; i < 100; i++) {
      ASSERT_EQ(stringCol1->valueAt(i), "baz");
      ASSERT_EQ(stringCol2->valueAt(i), "abcdefghijklmnop");
    }
  }

  if (firstStripe <= 1 && pastLastStripe > 1) {
    // // Stripe 2
    ASSERT_TRUE(rowReader->next(100, batch));
    auto root = batch->as<RowVector>();
    EXPECT_EQ(root->childrenSize(), 4);
    auto stringCol1 = root->childAt(1)->as<SimpleVector<StringView>>();
    auto stringCol2 = root->childAt(2)->as<SimpleVector<StringView>>();

    for (int i = 0; i < 50; i++) {
      ASSERT_EQ(stringCol1->valueAt(i), "ee");
      ASSERT_EQ(stringCol2->valueAt(i), "pomelo");
    }

    ASSERT_EQ(stringCol1->valueAt(50), "craz");
    ASSERT_EQ(stringCol2->valueAt(50), "unique");

    ASSERT_EQ(stringCol1->valueAt(51), "doop");
    ASSERT_EQ(stringCol2->valueAt(51), "different");

    ASSERT_EQ(stringCol1->valueAt(52), "hello");
    ASSERT_EQ(stringCol2->valueAt(52), "special");

    for (int i = 53; i < 100; i++) {
      ASSERT_EQ(stringCol1->valueAt(i), "baz");
      ASSERT_EQ(stringCol2->valueAt(i), "pomelo");
    }
  }

  if (firstStripe <= 2 && pastLastStripe > 2) {
    // Stripe 3
    ASSERT_TRUE(rowReader->next(100, batch));
    auto root = batch->as<RowVector>();
    ASSERT_EQ(root->size(), 3);
    EXPECT_EQ(root->childrenSize(), 4);
    auto stringCol1 = root->childAt(1)->as<SimpleVector<StringView>>();
    auto stringCol2 = root->childAt(2)->as<SimpleVector<StringView>>();

    ASSERT_EQ(stringCol1->valueAt(0), "craz");
    ASSERT_EQ(stringCol2->valueAt(0), "dog");

    ASSERT_EQ(stringCol1->valueAt(1), "doop");
    ASSERT_EQ(stringCol2->valueAt(1), "cat");

    ASSERT_EQ(stringCol1->valueAt(2), "hello");
    ASSERT_EQ(stringCol2->valueAt(2), "chicken");
  }
}

class TestFlatMapReader : public TestWithParam<bool>, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_P(TestFlatMapReader, testReadFlatMapEmptyMap) {
  const std::string emptyFile(getExampleFilePath("empty_flatmap.orc"));
  auto returnFlatVector = GetParam();

  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setReturnFlatVector(returnFlatVector);
  std::shared_ptr<const RowType> emptyFileType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
         id:int,\
     mapCol:map<int,int>,\
     ds:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(emptyFileType));
  auto reader = DwrfReader::create(
      createFileBufferedInput(emptyFile, readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());
  VectorPtr batch;
  rowReader->next(1, batch);
  auto root = batch->as<RowVector>();

  auto map = root->childAt(1)->as<MapVector>();
  auto mapKeyInt = map->mapKeys()->as<SimpleVector<int32_t>>();
  auto mapValueInt = map->mapValues()->as<SimpleVector<int32_t>>();

  EXPECT_EQ(0, mapKeyInt->size());
  EXPECT_EQ(0, mapValueInt->size());
  EXPECT_EQ(mapKeyInt->getNullCount().has_value(), false);
}

TEST_P(TestFlatMapReader, testStringKeyLifeCycle) {
  auto returnFlatVector = GetParam();

  VectorPtr batch;
  dwio::common::ReaderOptions readerOptions{pool()};

  {
    RowReaderOptions rowReaderOptions;
    rowReaderOptions.setReturnFlatVector(returnFlatVector);

    auto reader = DwrfReader::create(
        createFileBufferedInput(
            getFMSmallFile(), readerOptions.getMemoryPool()),
        readerOptions);
    auto rowReader = reader->createRowReader(rowReaderOptions);
    rowReader->next(100, batch);
  }

  // try to verify map2 as map<string, map<smallint, bigint>>
  auto map2 = batch->as<RowVector>()->childAt(2)->as<MapVector>();
  auto map2Key = map2->mapKeys();
  FlatVectorPtr<StringView> map2KeyString =
      std::dynamic_pointer_cast<FlatVector<StringView>>(map2Key);

  // data - map2 always has string keys "key-1" and "key-nullable"
  // key-1 always has value map {1:1}
  // value of "key-nullable" is either null or map {1:1}
  for (int32_t i = 0; i < map2->size(); ++i) {
    int64_t start = map2->offsetAt(i);
    int64_t end = start + map2->sizeAt(i);

    // map2 has at least key-1 and key-nullable
    EXPECT_GE(end - start, 2);

    // go through all the keys
    int32_t found = 0;
    while (start < end) {
      auto keyStr = map2KeyString->valueAt(start++).str();
      if (keyStr == "key-1" || keyStr == "key-nullable") {
        found++;
      }
    }

    // these two keys should always be present
    EXPECT_EQ(found, 2);
  }

  // try to verify map4 as
  // map<int,struct<field1:int,field2:float,field3:string>>
  auto map4 = batch->as<RowVector>()->childAt(4)->as<MapVector>();
  auto rowField =
      map4->mapValues()->wrappedVector()->as<RowVector>()->childAt(2);
  FlatVectorPtr<StringView> rowFieldString =
      std::dynamic_pointer_cast<FlatVector<StringView>>(rowField);
  ASSERT_GT(rowFieldString->size(), 0);
  ASSERT_GE(rowFieldString->valueAt(0).str().size(), 0);
}

TEST_P(TestFlatMapReader, testReadFlatMapSampleSmallSkips) {
  // batch size is set as 1000 in reading
  const std::array<int32_t, 4> seeks{100, 700, 0, 0};
  const std::array<int32_t, 3> expectedBatchSize{200, 200, 100};
  auto returnFlatVector = GetParam();
  verifyFlatMapReading(
      pool(),
      getFMSmallFile(),
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      returnFlatVector);
}

TEST_P(TestFlatMapReader, testReadFlatMapSampleSmall) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);
  const std::array<int32_t, 4> expectedBatchSize{300, 300, 300, 100};
  auto returnFlatVector = GetParam();
  verifyFlatMapReading(
      pool(),
      getFMSmallFile(),
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      returnFlatVector);
}

TEST_P(TestFlatMapReader, testReadFlatMapSampleLarge) {
  // batch size is set as 1000 in reading
  // 3000 per stripe
  std::array<int32_t, 11> seeks;
  seeks.fill(0);
  // batch size is set as 1000 in reading
  const std::array<int32_t, 10> expectedBatchSize{
      1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000};
  auto returnFlatVector = GetParam();
  verifyFlatMapReading(
      pool(),
      getFMLargeFile(),
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      returnFlatVector);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    FlatMapReaderTests,
    TestFlatMapReader,
    Values(true, false));

TEST_F(TestRowReaderPrefetch, testPartialPrefetch) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);
  const std::array<int32_t, 4> expectedBatchSize{300, 300, 300, 100};
  verifyFlatMapReading(
      pool(),
      getFMSmallFile(),
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      false,
      {300, 300, 300, 100},
      /* file has 4 stripes, prefetch only some and verify whole read */
      {true, false, true, false});
}

TEST_F(TestRowReaderPrefetch, testPrefetchWholeFile) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);
  const std::array<int32_t, 4> expectedBatchSize{300, 300, 300, 100};
  verifyFlatMapReading(
      pool(),
      getFMSmallFile(),
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      false,
      {300, 300, 300, 100},
      /* file has 4 stripes, issue prefetch for each one */
      {true, true, true, true});
}

TEST_F(TestRowReaderPfetch, testSeekBeforePrefetch) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);

  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(std::make_shared<ColumnSelector>(getFlatmapSchema()));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  rowReader->seekToRow(100);
  // First stripe has 300 rows, but expect 200 due to seeking past first 100
  const std::array<int32_t, 4> expectedBatchSize{200, 300, 300, 100};
  auto prefetches = rowReader->prefetchUnits().value();
  for (auto& prefetch : prefetches) {
    prefetch.prefetch();
  }

  verifyFlatMapReading(
      rowReader,
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size());
}

// Synchronous interleaving
TEST_F(TestRowReaderPrefetch, testPrefetchAndStartNextStripeInterleaved) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);
  const std::array<int32_t, 4> expectedBatchSize{300, 300, 300, 100};
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFilePreloadThreshold(0);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(std::make_shared<ColumnSelector>(getFlatmapSchema()));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  // startNextStripe just loads state for current row- it shouldn't prefetch
  // ahead of its place
  rowReader->startNextStripe();

  // std::optional<std::vector<DwrfRowReader::PrefetchUnit>> units =
  // rowReader->prefetchUnits();
  auto units = rowReader->prefetchUnits().value();
  EXPECT_EQ(units.size(), 4);

  // startNextStripe should not interfere with prefetch- it should just be
  // continuously re-loading the stripe its row index is on (currently 0).
  EXPECT_EQ(units[0].prefetch(), DwrfRowReader::FetchResult::kAlreadyFetched);
  EXPECT_EQ(units[1].prefetch(), DwrfRowReader::FetchResult::kFetched);
  EXPECT_EQ(units[1].prefetch(), DwrfRowReader::FetchResult::kAlreadyFetched);
  rowReader->startNextStripe();
  rowReader->startNextStripe();
  EXPECT_EQ(units[1].prefetch(), DwrfRowReader::FetchResult::kAlreadyFetched);

  // Prefetch rest of stripe and call again (expecting no-op)
  EXPECT_EQ(units[2].prefetch(), DwrfRowReader::FetchResult::kFetched);
  EXPECT_EQ(units[3].prefetch(), DwrfRowReader::FetchResult::kFetched);

  // DwrfRowReader still should register having no prefetching to do
  rowReader->startNextStripe();

  // Verify reads are correct
  verifyFlatMapReading(
      rowReader,
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size());
}

TEST_F(TestRowReaderPrefetch, testReadLargePrefetch) {
  // batch size is set as 1000 in reading
  // 3000 per stripe
  std::array<int32_t, 11> seeks;
  seeks.fill(0);
  const std::array<int32_t, 10> expectedBatchSize{
      1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000};
  verifyFlatMapReading(
      pool(),
      getFMLargeFile(),
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      false,
      {3000, 3000, 3000, 1000},
      {true, true, false, false});
}

TEST_F(TestRowReaderPrefetch, testParallelPrefetch) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);
  const std::array<int32_t, 4> expectedBatchSize{300, 300, 300, 100};
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(std::make_shared<ColumnSelector>(getFlatmapSchema()));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  auto units = rowReader->prefetchUnits().value();
  std::vector<std::future<DwrfRowReader::FetchResult>> prefetches;
  prefetches.reserve(4);
  for (int i = 0; i < 4; i++) {
    prefetches.push_back(std::async(units[i].prefetch));
  }

  // Verify reads are correct
  verifyFlatMapReading(
      rowReader,
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size());
}

// Use large file and disable preload to test
TEST_F(TestRowReaderPrefetch, testParallelPrefetchNoPreload) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 11> seeks;
  seeks.fill(0);
  const std::array<int32_t, 10> expectedBatchSize{
      1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000};
  dwio::common::ReaderOptions readerOpts{pool()};
  // Explicitly disable so IO takes some time
  readerOpts.setFilePreloadThreshold(0);
  readerOpts.setFooterEstimatedSize(4);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(std::make_shared<ColumnSelector>(getFlatmapSchema()));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMLargeFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  auto units = rowReader->prefetchUnits().value();
  std::vector<std::future<DwrfRowReader::FetchResult>> prefetches;
  prefetches.reserve(4);
  for (int i = 0; i < 4; i++) {
    prefetches.push_back(std::async(units[i].prefetch));
  }

  // Verify reads are correct
  verifyFlatMapReading(
      rowReader,
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size());
}

TEST_F(TestRowReaderPrefetch, prefetchWithCachedIndexStream) {
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFilePreloadThreshold(0);
  readerOpts.setFooterEstimatedSize(4);
  RowReaderOptions rowReaderOpts;

  std::shared_ptr<const RowType> requestedType = std::dynamic_pointer_cast<
      const RowType>(HiveTypeParser().parse(
      "struct<int_column:int,string_column:string,string_column_2:string,ds:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(requestedType));
  rowReaderOpts.setEagerFirstStripeLoad(false);

  auto reader = DwrfReader::create(
      createFileBufferedInput(
          getExampleFilePath("dict_encoded_strings.orc"),
          readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  auto units = rowReader->prefetchUnits().value();
  std::vector<DwrfRowReader::FetchResult> prefetches;

  prefetches.reserve(1);
  for (int i = 0; i < 3; i++) {
    prefetches.emplace_back(units[i].prefetch());
  }

  for (auto& fetchResult : prefetches) {
    ASSERT_EQ(DwrfRowReader::FetchResult::kFetched, fetchResult);
  }
  verifyCachedIndexStreamReads(rowReader, 0, 3);
}

struct ByStripeInfo {
  uint64_t offset;
  uint64_t length;
  uint32_t firstStripe;
  uint32_t pastLastStripe;

  ByStripeInfo(
      uint64_t offset,
      uint64_t length,
      uint32_t firstStripe,
      uint32_t pastLastStripe)
      : offset(offset),
        length(length),
        firstStripe(firstStripe),
        pastLastStripe(pastLastStripe) {}
};

class TestRowReaderPrefetchByStripe : public TestWithParam<ByStripeInfo>,
                                      public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

// This test ensures that we only return the prefetch units for the stripes that
// we'll actually use, according to the range passed to the row reader. We
// don't need to be able to prefetch stripes that we won't use. That would
// confuse us, since we'd have to calculate, outside of the reader, which
// stripes we need to prefetch.
TEST_P(TestRowReaderPrefetchByStripe, prefetchWithCachedIndexStream) {
  auto opt = GetParam();
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFilePreloadThreshold(0);
  readerOpts.setFooterEstimatedSize(4);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.range(opt.offset, opt.length);

  std::shared_ptr<const RowType> requestedType = std::dynamic_pointer_cast<
      const RowType>(HiveTypeParser().parse(
      "struct<int_column:int,string_column:string,string_column_2:string,ds:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(requestedType));
  rowReaderOpts.setEagerFirstStripeLoad(false);

  auto reader = DwrfReader::create(
      createFileBufferedInput(
          getExampleFilePath("dict_encoded_strings.orc"),
          readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  auto units = rowReader->prefetchUnits().value();
  EXPECT_EQ(units.size(), (opt.pastLastStripe - opt.firstStripe));
  std::vector<DwrfRowReader::FetchResult> prefetches;
  prefetches.reserve(opt.pastLastStripe - opt.firstStripe);

  for (auto& unit : units) {
    prefetches.emplace_back(unit.prefetch());
  }

  ASSERT_EQ(prefetches.size(), opt.pastLastStripe - opt.firstStripe);

  for (auto& fetchResult : prefetches) {
    ASSERT_EQ(DwrfRowReader::FetchResult::kFetched, fetchResult);
  }
  verifyCachedIndexStreamReads(rowReader, opt.firstStripe, opt.pastLastStripe);
}

// Stripe | offset | length | rows
//      0 |      3 |    583 |    3
//      1 |    586 |    508 |  100
//      2 |   1094 | ? (1+) |  100
VELOX_INSTANTIATE_TEST_SUITE_P(
    TestRowReaderPrefetchByStripeSuite,
    TestRowReaderPrefetchByStripe,
    ValuesIn({
        ByStripeInfo(3, 1, 0, 1), // Stripes: 0
        ByStripeInfo(586, 1, 1, 2), // Stripes: 1
        ByStripeInfo(1094, 1, 2, 3), // Stripes: 2
        ByStripeInfo(3, 584, 0, 2), // Stripes: 0, 1
        ByStripeInfo(586, 509, 1, 3), // Stripes: 1, 2
        ByStripeInfo(3, 1092, 0, 3) // Stripes: 0, 1, 2
    }));

// This test just verifies read correctness with the eager first stripe load
// config off for regression purposes. It does not ensure the first stripe is
// not loaded before we explicitly prefetch or start reading.
TEST_F(TestRowReaderPrefetch, testNoEagerFirstStripeLoad) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);
  const std::array<int32_t, 4> expectedBatchSize{300, 300, 300, 100};
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;

  // If we ever change default to false, let us fail this test so we can
  // change tests in this file accordingly.
  ASSERT_TRUE(rowReaderOpts.getEagerFirstStripeLoad());
  rowReaderOpts.setEagerFirstStripeLoad(false);
  rowReaderOpts.select(std::make_shared<ColumnSelector>(getFlatmapSchema()));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  // Verify reads are correct
  verifyFlatMapReading(
      rowReader,
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size());
}

// Other tests use default of eager loading, and test first stripe is
// preloaded after DwrfRowReader::create. This tests the case where eager
// loading is set to false.
TEST_F(TestRowReaderPrefetch, testFirstStripeNotLoadedWithEagerLoadingOff) {
  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setEagerFirstStripeLoad(false);
  rowReaderOpts.select(std::make_shared<ColumnSelector>(getFlatmapSchema()));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());

  auto units = rowReader->prefetchUnits().value();
  ASSERT_EQ(units[0].prefetch(), DwrfRowReader::FetchResult::kFetched);
}

// PrefetchUnits should return empty
TEST_F(TestRowReaderPrefetch, testEmptyRowRange) {
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  // Set empty range in rowreader options
  rowReaderOpts.range(0, 0);
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  auto rowReader = dynamic_cast<DwrfRowReader*>(rowReaderOwner.get());
  auto units = rowReader->prefetchUnits().value();
  ASSERT_EQ(0, units.size());
}

class TestFlatMapReaderFlatLayout
    : public TestWithParam<std::tuple<bool, size_t>>,
      public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_P(TestFlatMapReaderFlatLayout, testCompare) {
  dwio::common::ReaderOptions readerOptions{pool()};
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOptions.getMemoryPool()),
      readerOptions);
  RowReaderOptions rowReaderOptions;
  auto param = GetParam();
  rowReaderOptions.setReturnFlatVector(false);
  auto rowReader = reader->createRowReader(rowReaderOptions);
  rowReaderOptions.setReturnFlatVector(true);
  auto rowReader2 = reader->createRowReader(rowReaderOptions);

  VectorPtr vector1;
  auto size = std::get<1>(param);
  while (rowReader->next(size, vector1) > 0) {
    VectorPtr vector2;
    rowReader2->next(size, vector2);
    ASSERT_EQ(vector1->size(), vector2->size());
    VectorPtr comp1 = vector1;
    VectorPtr comp2 = vector2;
    for (auto i = 0; i < vector1->size(); ++i) {
      ASSERT_TRUE(comp1->equalValueAt(comp2.get(), i, i)) << i;
    }
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    FlatMapReaderFlatLayoutTests,
    TestFlatMapReaderFlatLayout,
    Combine(Bool(), Values(1, 100)));

TEST_F(TestReader, testReadFlatMapWithKeyFilters) {
  // batch size is set as 1000 in reading
  // file has schema: a int, b struct<a:int, b:float, c:string>, c float
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  // set map key filter for map1 we only need key=1, and map2 only key-1
  auto cs = std::make_shared<ColumnSelector>(
      getFlatmapSchema(),
      std::vector<std::string>{"map1#[1]", "map2#[\"key-1\"]"});
  rowReaderOpts.select(cs);
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;

  do {
    bool result = rowReader->next(1000, batch);
    if (!result) {
      break;
    }

    // verify current batch
    auto root = batch->as<RowVector>();

    // verify map1
    {
      auto map1 = root->childAt(1)->as<MapVector>();
      auto map1KeyInt = map1->mapKeys()->as<SimpleVector<int32_t>>();

      // every key value should be 1
      EXPECT_GT(map1KeyInt->size(), 0);
      for (int32_t i = 0; i < map1KeyInt->size(); ++i) {
        // every key should be just 1
        EXPECT_EQ(map1KeyInt->valueAt(i), 1);
      }
    }

    // verify map2
    {
      auto map2 = root->childAt(2)->as<MapVector>();
      auto map2KeyString = map2->mapKeys()->as<SimpleVector<StringView>>();

      // every key value should be key-1
      EXPECT_GT(map2KeyString->size(), 0);
      for (int32_t i = 0; i < map2KeyString->size(); ++i) {
        // every key should be just 1
        EXPECT_EQ(map2KeyString->valueAt(i).str(), "key-1");
      }
    }
  } while (true);
}

TEST_F(TestReader, testReadFlatMapWithKeyRejectList) {
  // batch size is set as 1000 in reading
  // file has schema: a int, b struct<a:int, b:float, c:string>, c float
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  auto cs = std::make_shared<ColumnSelector>(
      getFlatmapSchema(), std::vector<std::string>{"map1#[\"!2\",\"!3\"]"});
  rowReaderOpts.select(cs);
  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;

  const std::unordered_set<int32_t> map1RejectList{2, 3};

  do {
    bool result = rowReader->next(1000, batch);
    if (!result) {
      break;
    }

    // verify current batch
    auto root = batch->as<RowVector>();

    // verify map1
    {
      auto map1 = root->childAt(1)->as<MapVector>();
      auto map1KeyInt = map1->mapKeys()->as<SimpleVector<int32_t>>();

      // every key value should be 1
      EXPECT_GT(map1KeyInt->size(), 0);
      for (int32_t i = 0; i < map1KeyInt->size(); ++i) {
        // These keys should not exist
        EXPECT_TRUE(map1RejectList.count(map1KeyInt->valueAt(i)) == 0);
      }
    }
  } while (true);
}

TEST_F(TestReader, testStatsCallbackFiredWithFiltering) {
  RowReaderOptions rowReaderOpts;
  // Apply feature projection
  auto cs = std::make_shared<ColumnSelector>(
      getFlatmapSchema(), std::vector<std::string>{"map2#[\"key-1\"]"});
  rowReaderOpts.select(cs);

  uint64_t totalKeyStreamsAggregate = 0;
  uint64_t selectedKeyStreamsAggregate = 0;

  rowReaderOpts.setKeySelectionCallback(
      [&totalKeyStreamsAggregate, &selectedKeyStreamsAggregate](
          facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats
              keySelectionStats) {
        totalKeyStreamsAggregate += keySelectionStats.totalKeys;
        selectedKeyStreamsAggregate += keySelectionStats.selectedKeys;
      });

  dwio::common::ReaderOptions readerOpts{pool()};

  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;

  do {
    bool result = rowReader->next(1000, batch);
    if (!result) {
      break;
    }
  } while (true);

  // Features were projected, so we expect selected keys > total keys
  EXPECT_EQ(totalKeyStreamsAggregate, 16);
  EXPECT_EQ(selectedKeyStreamsAggregate, 4);
}

TEST_F(TestReader, testBlockedIoCallbackFiredBlocking) {
  RowReaderOptions rowReaderOpts;
  std::optional<uint64_t> metricToIncrement;

  rowReaderOpts.setBlockedOnIoCallback(
      [&metricToIncrement](uint64_t blockedTimeMs) {
        if (metricToIncrement) {
          *metricToIncrement += blockedTimeMs;
        } else {
          metricToIncrement = blockedTimeMs;
        }
      });
  rowReaderOpts.setEagerFirstStripeLoad(false);

  dwio::common::ReaderOptions readerOpts{pool()};

  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMLargeFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  // We didn't preload first stripe, so we expect metric to not be populated yet
  EXPECT_EQ(metricToIncrement, std::nullopt);
  VectorPtr batch;

  auto lastMetric = metricToIncrement;
  do {
    bool result = rowReader->next(1000, batch);
    // Stripes in fm_large are 1000 rows, so we expect reading 1000 rows to load
    // a new stripe and increment the metric
    EXPECT_GE(metricToIncrement, lastMetric);
    lastMetric = metricToIncrement;
    if (!result) {
      break;
    }
  } while (true);

  // Reading stripes that were prefetched should not affect metric
  EXPECT_GE(metricToIncrement, 0);
}

TEST_F(TestReader, testBlockedIoCallbackFiredNonBlocking) {
  RowReaderOptions rowReaderOpts;
  std::optional<uint64_t> metricToIncrement;

  rowReaderOpts.setBlockedOnIoCallback(
      [&metricToIncrement](uint64_t blockedTimeMs) {
        if (metricToIncrement) {
          *metricToIncrement += blockedTimeMs;
        } else {
          metricToIncrement = blockedTimeMs;
        }
      });
  rowReaderOpts.setEagerFirstStripeLoad(false);

  dwio::common::ReaderOptions readerOpts{pool()};

  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMLargeFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  EXPECT_EQ(metricToIncrement, std::nullopt);
  VectorPtr batch;

  auto units = rowReader->prefetchUnits().value();

  // Blocking prefetch all stripes
  for (auto& unit : units) {
    unit.prefetch();
    // Since these are not blocking a read, metric should not be incremented.
    EXPECT_EQ(metricToIncrement, std::nullopt);
  }

  do {
    bool result = rowReader->next(1000, batch);
    if (!result) {
      break;
    }
  } while (true);

  // Reading prefetched stripes should not increment metric, but it should set
  // the metric from nullopt to 0, indicating we've hit the read path
  EXPECT_EQ(metricToIncrement, 0);
}

TEST_F(TestReader, testBlockedIoCallbackFiredWithFirstStripeLoad) {
  RowReaderOptions rowReaderOpts;
  std::optional<uint64_t> metricToIncrement;

  rowReaderOpts.setBlockedOnIoCallback(
      [&metricToIncrement](uint64_t blockedTimeMs) {
        if (metricToIncrement) {
          *metricToIncrement += blockedTimeMs;
        } else {
          metricToIncrement = blockedTimeMs;
        }
      });

  rowReaderOpts.setEagerFirstStripeLoad(true);

  dwio::common::ReaderOptions readerOpts{pool()};

  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMLargeFile(), readerOpts.getMemoryPool()),
      readerOpts);
  EXPECT_EQ(metricToIncrement, std::nullopt);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  // Expect metric has now been populated, due to the initial blocking IO of
  // startNextStripe()
  EXPECT_GE(metricToIncrement, 0);
  auto metricAfterFirstStripe = metricToIncrement;
  VectorPtr batch;

  auto units = rowReader->prefetchUnits().value();
  EXPECT_EQ(metricToIncrement, metricAfterFirstStripe);

  // Blocking prefetch all stripes
  for (auto& unit : units) {
    unit.prefetch();
    // Since these are not blocking a read, metric should not be incremented
    EXPECT_EQ(metricToIncrement, metricAfterFirstStripe);
  }

  do {
    bool result = rowReader->next(1000, batch);
    if (!result) {
      break;
    }
  } while (true);

  // Reading prefetched stripes should not affect metric
  EXPECT_EQ(metricToIncrement, metricAfterFirstStripe);
}

TEST_F(TestReader, testEstimatedSize) {
  dwio::common::ReaderOptions readerOpts{pool()};
  {
    auto reader = DwrfReader::create(
        createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
        readerOpts);
    auto cs = std::make_shared<ColumnSelector>(
        getFlatmapSchema(), std::vector<std::string>{"map2"});
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(cs);

    auto rowReader = reader->createRowReader(rowReaderOpts);
    ASSERT_EQ(rowReader->estimatedRowSize(), 79);
  }

  {
    auto reader = DwrfReader::create(
        createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
        readerOpts);
    auto cs = std::make_shared<ColumnSelector>(
        getFlatmapSchema(), std::vector<std::string>{"id"});
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(cs);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    ASSERT_EQ(rowReader->estimatedRowSize(), 13);
  }
}

TEST_F(TestReader, testStatsCallbackFiredWithoutFiltering) {
  RowReaderOptions rowReaderOpts;
  // Don't apply feature projection here
  auto cs = std::make_shared<ColumnSelector>(
      getFlatmapSchema(), std::vector<std::string>{"map2"});
  rowReaderOpts.select(cs);

  uint64_t totalKeyStreamsAggregate = 0;
  uint64_t selectedKeyStreamsAggregate = 0;

  rowReaderOpts.setKeySelectionCallback(
      [&totalKeyStreamsAggregate, &selectedKeyStreamsAggregate](
          facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats
              keySelectionStats) {
        totalKeyStreamsAggregate += keySelectionStats.totalKeys;
        selectedKeyStreamsAggregate += keySelectionStats.selectedKeys;
      });

  dwio::common::ReaderOptions readerOpts{pool()};

  auto reader = DwrfReader::create(
      createFileBufferedInput(getFMSmallFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;

  do {
    bool result = rowReader->next(1000, batch);
    if (!result) {
      break;
    }
  } while (true);

  // No features were projected, so we expect selected keys == total keys
  EXPECT_EQ(totalKeyStreamsAggregate, 16);
  EXPECT_EQ(selectedKeyStreamsAggregate, 16);
}

namespace {

std::vector<std::string> stringify(const std::vector<int32_t>& values) {
  std::vector<std::string> converted(values.size());
  std::transform(
      values.begin(), values.end(), converted.begin(), [](int32_t value) {
        return folly::to<std::string>(value);
      });
  return converted;
}

std::unordered_map<uint32_t, std::vector<std::string>> makeStructEncodingOption(
    const ColumnSelector& cs,
    const std::string& columnName,
    const std::vector<int32_t>& keys) {
  const auto schema = cs.getSchemaWithId();
  const auto names = schema->type()->as<TypeKind::ROW>().names();

  for (uint32_t i = 0; i < names.size(); ++i) {
    if (columnName == names[i]) {
      std::unordered_map<uint32_t, std::vector<std::string>> config;
      config[schema->childAt(i)->id()] = stringify(keys);
      return config;
    }
  }

  folly::assume_unreachable();
}

void verifyMapColumnEqual(
    MapVector* mapVector,
    RowVector* rowVector,
    int32_t key,
    vector_size_t childOffset) {
  const auto& key1ValueVector = rowVector->childAt(childOffset);
  const auto& keyVector = mapVector->mapKeys()->as<SimpleVector<int32_t>>();
  const auto& valueVector = mapVector->mapValues();
  for (uint64_t i = 0; i < mapVector->size(); ++i) {
    if (mapVector->isNullAt(i)) {
      EXPECT_TRUE(key1ValueVector->isNullAt(i));
    } else {
      bool found = false;
      for (uint64_t j = mapVector->offsetAt(i);
           j < mapVector->offsetAt(i) + mapVector->sizeAt(i);
           ++j) {
        if (keyVector->valueAt(j) == key) {
          EXPECT_EQ(valueVector->compare(key1ValueVector.get(), j, i), 0);
          found = true;
          break;
        }
      }

      if (!found) {
        EXPECT_TRUE(key1ValueVector->isNullAt(i));
      }
    }
  }
}

void verifyFlatmapStructEncoding(
    memory::MemoryPool* pool,
    const std::string& filename,
    const std::vector<int32_t>& keysAsFields,
    const std::vector<int32_t>& keysToSelect,
    size_t batchSize = 1000) {
  dwio::common::ReaderOptions readerOpts{pool};
  auto reader = DwrfReader::create(
      createFileBufferedInput(filename, readerOpts.getMemoryPool()),
      readerOpts);

  const std::string projectedColumn = "map1";
  const vector_size_t projectedColumnIndex = 1;
  const std::vector<std::string> columnSelections = keysToSelect.empty()
      ? std::vector<std::string>{projectedColumn}
      : std::vector<std::string>{
            projectedColumn + "#[" + folly::join(", ", keysToSelect) + "]"};

  auto cs = std::make_shared<ColumnSelector>(
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
         id:int,\
     map1:map<int, array<float>>,\
     map2:map<string, map<smallint,bigint>>,\
     map3:map<int,int>,\
     map4:map<int,struct<field1:int,field2:float,field3:string>>,\
     memo:string>")),
      columnSelections);

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(cs);

  auto mapEncodingReader = reader->createRowReader(rowReaderOpts);

  rowReaderOpts.setFlatmapNodeIdsAsStruct(
      makeStructEncodingOption(*cs, "map1", keysAsFields));
  auto structEncodingReader = reader->createRowReader(rowReaderOpts);

  const auto compare = [&]() {
    VectorPtr batchMap;
    VectorPtr batchStruct;

    do {
      bool resultMap = mapEncodingReader->next(batchSize, batchMap);
      bool resultStruct = structEncodingReader->next(batchSize, batchStruct);

      EXPECT_EQ(resultMap, resultStruct);
      if (!resultMap) {
        break;
      }

      // verify current batch
      auto rowMapEncoding = batchMap->as<RowVector>();
      auto rowStructEncoding = batchStruct->as<RowVector>();

      EXPECT_EQ(rowMapEncoding->size(), rowStructEncoding->size());

      for (size_t i = 0; i < keysAsFields.size(); ++i) {
        verifyMapColumnEqual(
            rowMapEncoding->childAt(projectedColumnIndex)->as<MapVector>(),
            rowStructEncoding->childAt(projectedColumnIndex)->as<RowVector>(),
            keysAsFields[i],
            i);
      }
    } while (true);
  };
  compare();
}
} // namespace

TEST_F(TestReader, testFlatmapAsStructSmall) {
  verifyFlatmapStructEncoding(
      pool(),
      getFMSmallFile(),
      {1, 2, 3, 4, 5, -99999999 /* does not exist */},
      {} /* no key filtering */);
}

TEST_F(TestReader, testFlatmapAsStructSmallEmptyInmap) {
  verifyFlatmapStructEncoding(
      pool(),
      getFMSmallFile(),
      {1, 2, 3, 4, 5, -99999999 /* does not exist */},
      {} /* no key filtering */,
      2);
}

TEST_F(TestReader, testFlatmapAsStructLarge) {
  verifyFlatmapStructEncoding(
      pool(),
      getFMSmallFile(),
      {1, 2, 3, 4, 5, -99999999 /* does not exist */},
      {} /* no key filtering */);
}

TEST_F(TestReader, testFlatmapAsStructWithKeyProjection) {
  verifyFlatmapStructEncoding(
      pool(),
      getFMSmallFile(),
      {1, 2, 3, 4, 5, -99999999 /* does not exist */},
      {3, 5} /* select only these to read */);
}

TEST_F(TestReader, testFlatmapAsStructRequiringKeyList) {
  const std::unordered_map<uint32_t, std::vector<std::string>> emptyKeys = {
      {0, {}}};
  RowReaderOptions rowReaderOpts;
  EXPECT_THROW(
      rowReaderOpts.setFlatmapNodeIdsAsStruct(emptyKeys), VeloxException);
}

// TODO: replace with mock
TEST_F(TestReader, testMismatchSchemaMoreFields) {
  // file has schema: a int, b struct<a:int, b:float, c:string>, c float
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:int,b:struct<a:int,b:float,c:string>,c:float,d:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<uint64_t>{1, 2, 3}));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    EXPECT_EQ(4, root->childrenSize());
    EXPECT_EQ(1, root->size());
    // Column 3 should be filled with NULLs
    EXPECT_LT(0, root->childAt(3)->getNullCount().value());
    EXPECT_TRUE(root->childAt(3)->isNullAt(0));
    // Column 0 should be null since it's not selected
    EXPECT_FALSE(root->childAt(0));
  }

  rowReaderOpts.setProjectSelectedType(true);
  reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  rowReader = reader->createRowReader(rowReaderOpts);
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    // We should have 3 columns since projection is pushed
    EXPECT_EQ(4, root->childrenSize());
    EXPECT_EQ(1, root->size());
    // Column 2 should be filled with NULLs
    EXPECT_LT(0, root->childAt(2)->getNullCount().value());
    EXPECT_TRUE(root->childAt(2)->isNullAt(0));
  }
}

TEST_F(TestReader, testMismatchSchemaFewerFields) {
  // file has schema: a int, b struct<a:int, b:float, c:string>, c float
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:int,b:struct<a:int,b:float,c:string>>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<uint64_t>{1}));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    EXPECT_EQ(2, root->childrenSize());
    EXPECT_EQ(1, root->size());

    // Column 0 should be null since it's not selected
    EXPECT_FALSE(root->childAt(0));
  }

  batch.reset();
  rowReaderOpts.setProjectSelectedType(true);
  reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  rowReader = reader->createRowReader(rowReaderOpts);
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    // We should have 1 column since projection is pushed
    EXPECT_EQ(1, root->childrenSize());
    EXPECT_EQ(1, root->size());
  }
}

TEST_F(TestReader, testMismatchSchemaNestedMoreFields) {
  // file has schema: a int, b struct<a:int, b:float>, c float
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:int,b:struct<a:int,b:float,c:string,d:binary>,c:float>"));
  LOG(INFO) << requestedType->toString();
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<std::string>{"b.b", "b.c", "b.d", "c"}));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    EXPECT_EQ(3, root->childrenSize());

    auto nested = std::dynamic_pointer_cast<RowVector>(root->childAt(1));
    EXPECT_EQ(4, nested->childrenSize());
    EXPECT_EQ(1, nested->size());

    // Column 3 should be filled with NULLs
    EXPECT_EQ(1, nested->childAt(3)->getNullCount().value());
    EXPECT_TRUE(nested->childAt(3)->isNullAt(0));

    // Column 0 should be null since it's not selected
    EXPECT_FALSE(nested->childAt(0));

    // float column should be selected and not null
    auto fv = std::dynamic_pointer_cast<FlatVector<float>>(root->childAt(2));
    EXPECT_EQ(1, fv->size());
    EXPECT_EQ(0, fv->getNullCount().value());
  }

  batch.reset();
  rowReaderOpts.setProjectSelectedType(true);
  reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  rowReader = reader->createRowReader(rowReaderOpts);
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    EXPECT_EQ(2, root->childrenSize());

    auto nested = std::dynamic_pointer_cast<RowVector>(root->childAt(0));
    // We should have 3 columns since projection is pushed
    EXPECT_EQ(3, nested->childrenSize());
    EXPECT_EQ(1, nested->size());

    // Column 1 should be filled with NULLs
    EXPECT_EQ(1, nested->childAt(2)->getNullCount().value());
    EXPECT_TRUE(nested->childAt(2)->isNullAt(0));

    // float column should be selected and not null
    auto fv = std::dynamic_pointer_cast<FlatVector<float>>(root->childAt(1));
    EXPECT_EQ(1, fv->size());
    EXPECT_EQ(0, fv->getNullCount().value());
  }
}

TEST_F(TestReader, testMismatchSchemaNestedFewerFields) {
  // file has schema: a int, b struct<a:int, b:float>, c float
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:int,b:struct<a:int,b:float>,c:float>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<std::string>{"b.b", "c"}));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    EXPECT_EQ(3, root->childrenSize());

    auto nested = std::dynamic_pointer_cast<RowVector>(root->childAt(1));
    EXPECT_EQ(2, nested->childrenSize());
    EXPECT_EQ(1, nested->size());

    // Column 0 should have size 0 since it's not selected
    EXPECT_FALSE(nested->childAt(0));

    // float column should be selected and not null
    auto fv = std::dynamic_pointer_cast<FlatVector<float>>(root->childAt(2));
    EXPECT_EQ(1, fv->size());
    EXPECT_EQ(0, fv->getNullCount().value());
  }

  batch.reset();
  rowReaderOpts.setProjectSelectedType(true);
  reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  rowReader = reader->createRowReader(rowReaderOpts);
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    EXPECT_EQ(2, root->childrenSize());

    auto nested = std::dynamic_pointer_cast<RowVector>(root->childAt(0));
    // We should have 1 column since projection is pushed
    EXPECT_EQ(1, nested->childrenSize());
    EXPECT_EQ(1, nested->size());

    // float column should be selected and not null
    auto fv = std::dynamic_pointer_cast<FlatVector<float>>(root->childAt(1));
    EXPECT_EQ(1, fv->size());
    EXPECT_EQ(0, fv->getNullCount().value());
  }
}

TEST_F(TestReader, testMismatchSchemaIncompatibleNotSelected) {
  // file has schema: a int, b struct<a:int, b:float>, c float
  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:float,b:struct<a:string,b:float>,c:int>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<std::string>{"b.b"}));
  auto reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch;
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    EXPECT_EQ(3, root->childrenSize());

    auto nested = std::dynamic_pointer_cast<RowVector>(root->childAt(1));
    EXPECT_EQ(2, nested->childrenSize());
    EXPECT_EQ(1, nested->size());

    // Column 0 should have size 0 since it's not selected
    EXPECT_FALSE(nested->childAt(0));
    // Column 1 should be selected and not null
    EXPECT_EQ(nested->childAt(1)->size(), 1);
    EXPECT_EQ(0, nested->childAt(1)->getNullCount().value());

    // Columns not selected should have nullptr
    EXPECT_FALSE(root->childAt(0));
    EXPECT_FALSE(root->childAt(2));
  }

  batch.reset();
  rowReaderOpts.setProjectSelectedType(true);
  reader = DwrfReader::create(
      createFileBufferedInput(getStructFile(), readerOpts.getMemoryPool()),
      readerOpts);
  rowReader = reader->createRowReader(rowReaderOpts);
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    EXPECT_EQ(1, root->childrenSize());

    auto nested = std::dynamic_pointer_cast<RowVector>(root->childAt(0));
    // We should have 1 column since projection is pushed
    EXPECT_EQ(1, nested->childrenSize());
    EXPECT_EQ(1, nested->size());

    EXPECT_EQ(1, nested->childAt(0)->size());
    EXPECT_EQ(0, nested->childAt(0)->getNullCount().value());
  }
}

TEST_F(TestReader, testMismatchSchemaIncompatible) {
  MockStripeStreams streams;

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  std::shared_ptr<const RowType> rowType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:int>"));

  auto types = folly::make_array<std::string>("float", "smallint");
  EncodingKey root(0, 0);
  for (auto& t : types) {
    std::shared_ptr<const RowType> reqType =
        std::dynamic_pointer_cast<const RowType>(
            HiveTypeParser().parse(fmt::format("struct<col0:{}>", t)));
    EXPECT_THROW(
        ColumnSelector cs(reqType, rowType), facebook::velox::VeloxUserError);
  }
}

TEST_F(TestReader, fileColumnNamesReadAsLowerCase) {
  // upper.orc holds one columns (Bool_Val: BOOLEAN, b: BIGINT)
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFileColumnNamesReadAsLowerCase(true);
  auto reader = DwrfReader::create(
      createFileBufferedInput(
          getExampleFilePath("upper.orc"), readerOpts.getMemoryPool()),
      readerOpts);
  auto type = reader->typeWithId();
  auto col0 = type->childAt(0);
  EXPECT_EQ(type->childByName("bool_val"), col0);
}

TEST_F(TestReader, fileColumnNamesReadAsLowerCaseComplexStruct) {
  // upper_complex.orc holds type
  // Cc:struct<CcLong0:bigint,CcMap1:map<string,struct<CcArray2:array<struct<CcInt3:int>>>>>
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFileColumnNamesReadAsLowerCase(true);
  auto reader = DwrfReader::create(
      createFileBufferedInput(
          getExampleFilePath("upper_complex.orc"), readerOpts.getMemoryPool()),
      readerOpts);
  auto type = reader->typeWithId();

  auto col0 = type->childAt(0);
  EXPECT_EQ(col0->type()->kind(), TypeKind::ROW);
  EXPECT_EQ(type->childByName("cc"), col0);

  auto col0_0 = col0->childAt(0);
  EXPECT_EQ(col0_0->type()->kind(), TypeKind::BIGINT);
  EXPECT_EQ(col0->childByName("cclong0"), col0_0);

  auto col0_1 = col0->childAt(1);
  EXPECT_EQ(col0_1->type()->kind(), TypeKind::MAP);
  EXPECT_EQ(col0->childByName("ccmap1"), col0_1);

  auto col0_1_0 = col0_1->childAt(0);
  EXPECT_EQ(col0_1_0->type()->kind(), TypeKind::VARCHAR);

  auto col0_1_1 = col0_1->childAt(1);
  EXPECT_EQ(col0_1_1->type()->kind(), TypeKind::ROW);

  auto col0_1_1_0 = col0_1_1->childAt(0);
  EXPECT_EQ(col0_1_1_0->type()->kind(), TypeKind::ARRAY);
  EXPECT_EQ(col0_1_1->childByName("ccarray2"), col0_1_1_0);

  auto col0_1_1_0_0 = col0_1_1_0->childAt(0);
  EXPECT_EQ(col0_1_1_0_0->type()->kind(), TypeKind::ROW);
  auto col0_1_1_0_0_0 = col0_1_1_0_0->childAt(0);
  EXPECT_EQ(col0_1_1_0_0_0->type()->kind(), TypeKind::INTEGER);
  EXPECT_EQ(col0_1_1_0_0->childByName("ccint3"), col0_1_1_0_0_0);
}

TEST_F(TestReader, TestStripeSizeCallback) {
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFilePreloadThreshold(0);
  readerOpts.setFooterEstimatedSize(4);
  RowReaderOptions rowReaderOpts;

  std::shared_ptr<const RowType> requestedType = std::dynamic_pointer_cast<
      const RowType>(HiveTypeParser().parse(
      "struct<int_column:int,string_column:string,string_column_2:string,ds:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(requestedType));
  rowReaderOpts.setEagerFirstStripeLoad(false);
  uint16_t stripeCount = 0;
  int numCalls = 0;
  rowReaderOpts.setStripeCountCallback([&](uint16_t count) {
    stripeCount += count;
    ++numCalls;
  });

  auto reader = DwrfReader::create(
      createFileBufferedInput(
          getExampleFilePath("dict_encoded_strings.orc"),
          readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  EXPECT_EQ(stripeCount, 3);
  EXPECT_EQ(numCalls, 1);
}

TEST_F(TestReader, TestStripeSizeCallbackLimitsOneStripe) {
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFilePreloadThreshold(0);
  readerOpts.setFooterEstimatedSize(4);
  RowReaderOptions rowReaderOpts;

  std::shared_ptr<const RowType> requestedType = std::dynamic_pointer_cast<
      const RowType>(HiveTypeParser().parse(
      "struct<int_column:int,string_column:string,string_column_2:string,ds:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(requestedType));
  rowReaderOpts.setEagerFirstStripeLoad(false);
  rowReaderOpts.range(600, 600);
  uint16_t stripeCount = 0;
  int numCalls = 0;
  rowReaderOpts.setStripeCountCallback([&](uint16_t count) {
    stripeCount += count;
    ++numCalls;
  });

  auto reader = DwrfReader::create(
      createFileBufferedInput(
          getExampleFilePath("dict_encoded_strings.orc"),
          readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  EXPECT_EQ(stripeCount, 1);
  EXPECT_EQ(numCalls, 1);
}

TEST_F(TestReader, TestStripeSizeCallbackLimitsTwoStripe) {
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFilePreloadThreshold(0);
  readerOpts.setFooterEstimatedSize(4);
  RowReaderOptions rowReaderOpts;

  std::shared_ptr<const RowType> requestedType = std::dynamic_pointer_cast<
      const RowType>(HiveTypeParser().parse(
      "struct<int_column:int,string_column:string,string_column_2:string,ds:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(requestedType));
  rowReaderOpts.setEagerFirstStripeLoad(false);
  rowReaderOpts.range(0, 600);
  uint16_t stripeCount = 0;
  int numCalls = 0;
  rowReaderOpts.setStripeCountCallback([&](uint16_t count) {
    stripeCount += count;
    ++numCalls;
  });

  auto reader = DwrfReader::create(
      createFileBufferedInput(
          getExampleFilePath("dict_encoded_strings.orc"),
          readerOpts.getMemoryPool()),
      readerOpts);
  auto rowReaderOwner = reader->createRowReader(rowReaderOpts);
  EXPECT_EQ(stripeCount, 2);
  EXPECT_EQ(numCalls, 1);
}

TEST_P(TestReaderP, testUpcastBoolean) {
  MockStripeStreams streams;

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0, 1] * 52 = 104 booleans/bits or 13 bytes
  // 0,1 encoded in a byte is 0101 0101 ->0x55
  // ByteRLE - Repeat->10 (13-MINIMUM_REPEAT), Value - 0x55
  auto data = folly::make_array<char>(10, 0x55);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));

  // create the row type
  std::shared_ptr<const RowType> rowType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:boolean>"));
  std::shared_ptr<const RowType> reqType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:int>"));
  ColumnSelector cs(reqType, rowType);
  EXPECT_CALL(streams, getColumnSelectorProxy()).WillRepeatedly(Return(&cs));
  memory::AllocationPool allocPool(pool());
  StreamLabels labels(allocPool);
  std::unique_ptr<ColumnReader> reader = ColumnReader::build(
      TypeWithId::create(reqType),
      TypeWithId::create(rowType),
      streams,
      labels,
      executor(),
      getDecodingParallelismFactor());

  VectorPtr batch;
  reader->next(104, batch);

  auto lv = std::dynamic_pointer_cast<FlatVector<int32_t>>(
      std::dynamic_pointer_cast<RowVector>(batch)->childAt(0));

  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(lv->valueAt(i), i % 2);
  }
}

TEST_P(TestReaderP, testUpcastIntDirect) {
  MockStripeStreams streams;

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0..99]
  std::array<char, 100> data;
  std::iota(data.begin(), data.end(), 0);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));

  // create the row type
  std::shared_ptr<const RowType> rowType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:int>"));
  std::shared_ptr<const RowType> reqType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:bigint>"));

  ColumnSelector cs(reqType, rowType);
  EXPECT_CALL(streams, getColumnSelectorProxy()).WillRepeatedly(Return(&cs));
  memory::AllocationPool allocPool(pool());
  StreamLabels labels(allocPool);
  std::unique_ptr<ColumnReader> reader = ColumnReader::build(
      TypeWithId::create(reqType),
      TypeWithId::create(rowType),
      streams,
      labels,
      executor(),
      getDecodingParallelismFactor());

  VectorPtr batch;
  reader->next(100, batch);

  auto lv = std::dynamic_pointer_cast<FlatVector<int64_t>>(
      std::dynamic_pointer_cast<RowVector>(batch)->childAt(0));
  for (size_t i = 0; i < batch->size(); ++i) {
    // bytes in the stream are zig-zag decoded on read
    // so zigzag::decode i to match the value.
    EXPECT_EQ(lv->valueAt(i), zigZagDecode(i));
  }
}

TEST_P(TestReaderP, testUpcastIntDict) {
  MockStripeStreams streams;

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  const size_t DICT_SIZE = 100;
  proto::ColumnEncoding dictEncoding;
  dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictEncoding.set_dictionarysize(DICT_SIZE);
  EXPECT_CALL(streams, getEncodingProxy(1))
      .WillRepeatedly(Return(&dictEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(nullptr));

  // [0..99] RLE encoded, is length = 100 (subtract -3 minimum repeat, 97 =
  // 0x61), delta - 1, start - 0
  auto data = folly::make_array<char>(0x61, 0x01, 0x00);
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));

  EXPECT_CALL(streams, genMockDictDataSetter(1, 0))
      .WillRepeatedly(Return([](BufferPtr& buffer, MemoryPool* pool) {
        buffer = AlignedBuffer::allocate<int64_t>(1024, pool);
        setSequence<int64_t>(buffer, 0, 100);
      }));

  // create the row type
  std::shared_ptr<const RowType> rowType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:int>"));
  std::shared_ptr<const RowType> reqType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:bigint>"));
  ColumnSelector cs(reqType, rowType);
  EXPECT_CALL(streams, getColumnSelectorProxy()).WillRepeatedly(Return(&cs));
  memory::AllocationPool allocPool(pool());
  StreamLabels labels(allocPool);
  std::unique_ptr<ColumnReader> reader = ColumnReader::build(
      TypeWithId::create(reqType),
      TypeWithId::create(rowType),
      streams,
      labels,
      executor(),
      getDecodingParallelismFactor());

  VectorPtr batch;
  reader->next(100, batch);

  auto lv = std::dynamic_pointer_cast<FlatVector<int64_t>>(
      std::dynamic_pointer_cast<RowVector>(batch)->childAt(0));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(lv->valueAt(i), i);
  }
}

TEST_P(TestReaderP, testUpcastFloat) {
  MockStripeStreams streams;

  // set getEncoding
  proto::ColumnEncoding directEncoding;
  directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&directEncoding));

  // set getStream
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));

  // [0..99]
  std::array<char, 100 * 4> data;
  size_t pos = 0;
  for (size_t i = 0; i < 100; ++i) {
    auto val = static_cast<float>(i);
    auto intPtr = reinterpret_cast<int32_t*>(&val);
    for (size_t j = 0; j < sizeof(int32_t); ++j) {
      data.data()[pos++] = static_cast<char>((*intPtr >> (8 * j)) & 0xff);
    }
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(data.data(), data.size())));

  // create the row type
  std::shared_ptr<const RowType> rowType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:float>"));
  std::shared_ptr<const RowType> reqType =
      std::dynamic_pointer_cast<const RowType>(
          HiveTypeParser().parse("struct<col0:double>"));
  ColumnSelector cs(reqType, rowType);
  EXPECT_CALL(streams, getColumnSelectorProxy()).WillRepeatedly(Return(&cs));
  memory::AllocationPool allocPool(pool());
  StreamLabels labels(allocPool);
  std::unique_ptr<ColumnReader> reader = ColumnReader::build(
      TypeWithId::create(reqType),
      TypeWithId::create(rowType),
      streams,
      labels,
      executor(),
      getDecodingParallelismFactor());

  VectorPtr batch;
  reader->next(100, batch);

  auto lv = std::dynamic_pointer_cast<FlatVector<double>>(
      std::dynamic_pointer_cast<RowVector>(batch)->childAt(0));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(lv->valueAt(i), static_cast<double>(i));
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    TestReaderSerialDecoding,
    TestReaderP,
    Values(false));

VELOX_INSTANTIATE_TEST_SUITE_P(
    TestReaderParallelDecoding,
    TestReaderP,
    Values(true));

TEST_F(TestReader, testEmptyFile) {
  MemorySink sink{1024, {.pool = pool()}};
  DataBufferHolder holder{*pool(), 1024, 0, DEFAULT_PAGE_GROW_RATIO, &sink};
  BufferedOutputStream output{holder};

  proto::Footer footer;
  footer.set_numberofrows(0);
  auto type = footer.add_types();
  type->set_kind(proto::Type_Kind::Type_Kind_STRUCT);

  footer.SerializeToZeroCopyStream(&output);
  output.flush();
  auto footerLen = sink.size();

  proto::PostScript ps;
  ps.set_footerlength(footerLen);
  ps.set_compression(proto::CompressionKind::NONE);

  ps.SerializeToZeroCopyStream(&output);
  output.flush();
  auto psLen = static_cast<uint8_t>(sink.size() - footerLen);

  DataBuffer<char> buf{*pool(), 1};
  buf.data()[0] = psLen;
  sink.write(std::move(buf));
  std::string_view data(sink.data(), sink.size());
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(data), *pool());

  dwio::common::ReaderOptions readerOpts{pool()};
  RowReaderOptions rowReaderOpts;

  auto rowReader = DwrfReader::create(std::move(input), readerOpts)
                       ->createRowReader(rowReaderOpts);
  VectorPtr batch;
  EXPECT_FALSE(rowReader->next(1, batch));
  EXPECT_FALSE(batch);
}

namespace {

using IteraterCallback =
    std::function<void(const std::vector<BufferPtr>&, size_t)>;

template <typename T>
std::vector<BufferPtr> getBuffers(const VectorPtr& vector) {
  auto flat = vector->asFlatVector<T>();
  return {flat->nulls(), flat->values()};
}

size_t
iterateVector(const VectorPtr& vector, IteraterCallback cb, size_t index = 0) {
  switch (vector->typeKind()) {
    case TypeKind::ROW:
      cb({vector->nulls()}, index++);
      for (auto& child : vector->as<RowVector>()->children()) {
        index = iterateVector(child, cb, index);
      }
      break;
    case TypeKind::ARRAY: {
      auto array = vector->as<ArrayVector>();
      cb({array->nulls(), array->offsets(), array->sizes()}, index++);
      index = iterateVector(array->elements(), cb, index);
      break;
    }
    case TypeKind::MAP: {
      auto map = vector->as<MapVector>();
      cb({map->nulls(), map->offsets(), map->sizes()}, index++);
      index = iterateVector(map->mapKeys(), cb, index);
      index = iterateVector(map->mapValues(), cb, index);
      break;
    }
    case TypeKind::BOOLEAN:
      cb(getBuffers<bool>(vector), index++);
      break;
    case TypeKind::TINYINT:
      cb(getBuffers<int8_t>(vector), index++);
      break;
    case TypeKind::SMALLINT:
      cb(getBuffers<int16_t>(vector), index++);
      break;
    case TypeKind::INTEGER:
      cb(getBuffers<int32_t>(vector), index++);
      break;
    case TypeKind::BIGINT:
      cb(getBuffers<int64_t>(vector), index++);
      break;
    case TypeKind::REAL:
      cb(getBuffers<float>(vector), index++);
      break;
    case TypeKind::DOUBLE:
      cb(getBuffers<double>(vector), index++);
      break;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      cb(getBuffers<StringView>(vector), index++);
      break;
    case TypeKind::TIMESTAMP:
      cb(getBuffers<Timestamp>(vector), index++);
      break;
    default:
      folly::assume_unreachable();
  }
  return index;
}

void testBufferLifeCycle(
    memory::MemoryPool* pool,
    const std::shared_ptr<const RowType>& schema,
    const std::shared_ptr<dwrf::Config>& config,
    std::mt19937& rng,
    size_t batchSize,
    bool hasNull) {
  std::vector<VectorPtr> batches;
  std::function<bool(vector_size_t)> isNullAt = nullptr;
  if (hasNull) {
    isNullAt = [](vector_size_t i) { return i % 2 == 0; };
  }
  auto vector =
      BatchMaker::createBatch(schema, batchSize * 2, *pool, rng, isNullAt);
  batches.push_back(vector);

  auto sink = std::make_unique<MemorySink>(
      1024 * 1024, FileSink::Options{.pool = pool});
  auto sinkPtr = sink.get();
  auto writer =
      E2EWriterTestUtil::writeData(std::move(sink), schema, batches, config);

  std::string_view data(sinkPtr->data(), sinkPtr->size());
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(data), *pool);

  dwio::common::ReaderOptions readerOpts{pool};
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setReturnFlatVector(true);
  auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  std::vector<BufferPtr> buffers;
  std::vector<size_t> bufferIndices;
  VectorPtr result;
  rowReader->next(batchSize, result);
  // Iterate through the vector hierarchy to introduce additional buffer
  // reference randomly.
  iterateVector(
      result, [&](const std::vector<BufferPtr>& vectorBuffers, size_t index) {
        EXPECT_EQ(buffers.size(), index);
        auto bufferIndex = folly::Random::rand32(vectorBuffers.size(), rng);
        buffers.push_back(vectorBuffers.at(bufferIndex));
        bufferIndices.push_back(bufferIndex);
      });

  rowReader->next(batchSize, result);
  // Verify buffers are recreated instead of being reused.
  iterateVector(
      result, [&](const std::vector<BufferPtr>& vectorBuffers, size_t index) {
        auto bufferIndex = bufferIndices.at(index);
        if (buffers.at(index)) {
          EXPECT_NE(
              vectorBuffers.at(bufferIndex).get(), buffers.at(index).get());
        }
      });
}

void testFlatmapAsMapFieldLifeCycle(
    memory::MemoryPool* pool,
    const std::shared_ptr<const RowType>& schema,
    const std::shared_ptr<dwrf::Config>& config,
    std::mt19937& rng,
    size_t batchSize,
    bool hasNull) {
  std::vector<VectorPtr> batches;
  std::function<bool(vector_size_t)> isNullAt = nullptr;
  if (hasNull) {
    isNullAt = [](vector_size_t i) { return i % 2 == 0; };
  }
  auto vector =
      BatchMaker::createBatch(schema, batchSize * 5, *pool, rng, isNullAt);
  batches.push_back(vector);

  auto sink = std::make_unique<MemorySink>(
      1024 * 1024, FileSink::Options{.pool = pool});
  auto sinkPtr = sink.get();
  auto writer =
      E2EWriterTestUtil::writeData(std::move(sink), schema, batches, config);

  std::string_view data(sinkPtr->data(), sinkPtr->size());
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(data), *pool);

  dwio::common::ReaderOptions readerOpts{pool};
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setReturnFlatVector(true);
  auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
  auto rowReader = reader->createRowReader(rowReaderOpts);

  std::vector<BufferPtr> buffers;
  std::vector<size_t> bufferIndices;
  VectorPtr result;

  EXPECT_TRUE(rowReader->next(batchSize, result));
  auto child =
      std::dynamic_pointer_cast<MapVector>(result->as<RowVector>()->childAt(0));
  BaseVector* rowPtr = result.get();
  MapVector* childPtr = child.get();
  Buffer* rawNulls = child->nulls().get();
  BufferPtr sizes = child->sizes();
  Buffer* rawOffsets = child->offsets().get();
  BaseVector* keysPtr = child->mapKeys().get();
  child.reset();

  EXPECT_TRUE(rowReader->next(batchSize, result));
  child =
      std::dynamic_pointer_cast<MapVector>(result->as<RowVector>()->childAt(0));
  EXPECT_EQ(rawNulls, child->nulls().get());
  EXPECT_NE(sizes, child->sizes());
  EXPECT_EQ(rawOffsets, child->offsets().get());
  EXPECT_EQ(keysPtr, child->mapKeys().get());
  // there is a TODO in FlatMapColumnReader next() (result is not reused)
  // should be EQ; fix: https://fburl.com/code/wtrq8r5q
  // EXPECT_EQ(childPtr, child.get());
  EXPECT_EQ(rowPtr, result.get());

  auto mapKeys = child->mapKeys();
  auto rawSizes = child->sizes().get();
  childPtr = child.get();
  child.reset();

  EXPECT_TRUE(rowReader->next(batchSize, result));
  child =
      std::dynamic_pointer_cast<MapVector>(result->as<RowVector>()->childAt(0));
  EXPECT_EQ(rawNulls, child->nulls().get());
  EXPECT_EQ(rawSizes, child->sizes().get());
  EXPECT_EQ(rawOffsets, child->offsets().get());
  EXPECT_NE(mapKeys, child->mapKeys());
  // there is a TODO in FlatMapColumnReader next() (result is not reused)
  // should be EQ; fix: https://fburl.com/code/wtrq8r5q
  // EXPECT_EQ(childPtr, child.get());
  EXPECT_EQ(rowPtr, result.get());

  EXPECT_TRUE(rowReader->next(batchSize, result));
  auto childCurr =
      std::dynamic_pointer_cast<MapVector>(result->as<RowVector>()->childAt(0));
  EXPECT_TRUE(
      (rawNulls != childCurr->nulls().get()) ||
      (!rawNulls && !childCurr->nulls()));
  EXPECT_NE(rawSizes, childCurr->sizes().get());
  EXPECT_NE(rawOffsets, childCurr->offsets().get());
  EXPECT_NE(keysPtr, childCurr->mapKeys().get());
  // EXPECT_EQ(childPtr, childCurr.get());
  EXPECT_EQ(rowPtr, result.get());
}

} // namespace

TEST_F(TestReader, testBufferLifeCycle) {
  const size_t batchSize = 10;
  auto schema = ROW({
      MAP(VARCHAR(), INTEGER()),
      MAP(BIGINT(), ARRAY(VARCHAR())),
      MAP(INTEGER(), MAP(TINYINT(), VARCHAR())),
      MAP(SMALLINT(),
          ROW({
              VARCHAR(),
              REAL(),
              BOOLEAN(),
          })),
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      ARRAY(INTEGER()),
      MAP(SMALLINT(), REAL()),
      ROW({DOUBLE(), BIGINT()}),
  });

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0, 1, 2, 3});

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  for (auto i = 0; i < 10; ++i) {
    testBufferLifeCycle(pool(), schema, config, rng, batchSize, false);
    testBufferLifeCycle(pool(), schema, config, rng, batchSize, true);
  }
}

TEST_F(TestReader, testFlatmapAsMapFieldLifeCycle) {
  const size_t batchSize = 10;
  auto schema = ROW({
      MAP(VARCHAR(), INTEGER()),
  });

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});

  auto seed = folly::Random::rand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 rng{seed};

  testFlatmapAsMapFieldLifeCycle(pool(), schema, config, rng, batchSize, false);
  testFlatmapAsMapFieldLifeCycle(pool(), schema, config, rng, batchSize, true);
}

TEST_F(TestReader, testOrcReaderSimple) {
  const std::string simpleTest(
      getExampleFilePath("TestStringDictionary.testRowIndex.orc"));
  dwio::common::ReaderOptions readerOpts{pool()};
  // To make DwrfReader reads ORC file, setFileFormat to FileFormat::ORC
  readerOpts.setFileFormat(dwio::common::FileFormat::ORC);
  auto reader = DwrfReader::create(
      createFileBufferedInput(simpleTest, readerOpts.getMemoryPool()),
      readerOpts);

  RowReaderOptions rowReaderOptions;
  auto rowReader = reader->createRowReader(rowReaderOptions);

  VectorPtr batch;
  const std::string stringPrefix{"row "};
  size_t rowNumber = 0;
  while (rowReader->next(500, batch)) {
    auto rowVector = batch->as<RowVector>();
    auto strings = rowVector->childAt(0)->as<SimpleVector<StringView>>();
    for (size_t i = 0; i < rowVector->size(); ++i) {
      std::stringstream stream;
      stream << std::setfill('0') << std::setw(6) << rowNumber;
      EXPECT_EQ(stringPrefix + stream.str(), strings->valueAt(i).str());
      rowNumber++;
    }
  }
  EXPECT_EQ(rowNumber, 32768);
}

TEST_F(TestReader, testFooterWrapper) {
  proto::Footer impl;
  FooterWrapper wrapper(&impl);
  EXPECT_FALSE(wrapper.hasNumberOfRows());
  impl.set_numberofrows(0);
  ASSERT_TRUE(wrapper.hasNumberOfRows());
  EXPECT_EQ(wrapper.numberOfRows(), 0);
}

TEST_F(TestReader, testOrcAndDwrfRowIndexStride) {
  // orc footer
  proto::orc::Footer orcFooter;
  FooterWrapper orcFooterWrapper(&orcFooter);
  EXPECT_FALSE(orcFooterWrapper.hasRowIndexStride());
  orcFooter.set_rowindexstride(100);
  ASSERT_TRUE(orcFooterWrapper.hasRowIndexStride());
  EXPECT_EQ(orcFooterWrapper.rowIndexStride(), 100);

  // dwrf footer
  proto::Footer dwrfFooter;
  FooterWrapper dwrfFooterWrapper(&dwrfFooter);
  EXPECT_FALSE(dwrfFooterWrapper.hasRowIndexStride());
  dwrfFooter.set_rowindexstride(100);
  ASSERT_TRUE(dwrfFooterWrapper.hasRowIndexStride());
  EXPECT_EQ(dwrfFooterWrapper.rowIndexStride(), 100);
}

TEST_F(TestReader, testOrcReaderComplexTypes) {
  const std::string icebergOrc(getExampleFilePath("complextypes_iceberg.orc"));
  const std::shared_ptr<const RowType> expectedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
     id:bigint,int_array:array<int>,int_array_array:array<array<int>>,\
     int_map:map<string,int>,int_map_array:array<map<string,int>>,\
     nested_struct:struct<\
       a:int,b:array<int>,c:struct<\
         d:array<array<struct<\
           e:int,f:string>>>>,\
         g:map<string,struct<\
           h:struct<\
             i:array<double>>>>>>"));
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFileFormat(dwio::common::FileFormat::ORC);
  auto reader = DwrfReader::create(
      createFileBufferedInput(icebergOrc, readerOpts.getMemoryPool()),
      readerOpts);
  auto rowType = reader->rowType();
  EXPECT_TRUE(rowType->equivalent(*expectedType));
}

TEST_F(TestReader, testOrcReaderVarchar) {
  const std::string varcharOrc(getExampleFilePath("orc_index_int_string.orc"));
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFileFormat(dwio::common::FileFormat::ORC);
  auto reader = DwrfReader::create(
      createFileBufferedInput(varcharOrc, readerOpts.getMemoryPool()),
      readerOpts);

  RowReaderOptions rowReaderOptions;
  auto rowReader = reader->createRowReader(rowReaderOptions);

  VectorPtr batch;
  int counter = 0;
  while (rowReader->next(500, batch)) {
    auto rowVector = batch->as<RowVector>();
    auto ints = rowVector->childAt(0)->as<SimpleVector<int32_t>>();
    auto strings = rowVector->childAt(1)->as<SimpleVector<StringView>>();
    for (size_t i = 0; i < rowVector->size(); ++i) {
      counter++;
      EXPECT_EQ(counter, ints->valueAt(i));
      std::stringstream stream;
      stream << counter;
      if (counter < 1000) {
        stream << "a";
      }
      EXPECT_EQ(stream.str(), strings->valueAt(i).str());
    }
  }
  EXPECT_EQ(counter, 6000);
}

TEST_F(TestReader, testOrcReaderDate) {
  const std::string dateOrc(getExampleFilePath("TestOrcFile.testDate1900.orc"));
  dwio::common::ReaderOptions readerOpts{pool()};
  readerOpts.setFileFormat(dwio::common::FileFormat::ORC);
  auto reader = DwrfReader::create(
      createFileBufferedInput(dateOrc, readerOpts.getMemoryPool()), readerOpts);

  RowReaderOptions rowReaderOptions;
  auto rowReader = reader->createRowReader(rowReaderOptions);

  VectorPtr batch;
  int year = 1900;
  while (rowReader->next(1000, batch)) {
    auto rowVector = batch->as<RowVector>();
    auto dates = rowVector->childAt(1)->as<SimpleVector<int32_t>>();

    std::stringstream stream;
    stream << year << "-12-25";
    EXPECT_EQ(stream.str(), DATE()->toString(dates->valueAt(0)));

    for (size_t i = 1; i < rowVector->size(); ++i) {
      EXPECT_EQ(dates->valueAt(0), dates->valueAt(i));
    }

    year++;
  }
}

namespace {

/*
 * Verifies that row numbers are equal to values in first column
 */
void verifyRowNumbers(
    RowReader& rowReader,
    memory::MemoryPool* pool,
    int expectedNumRows) {
  auto result = BaseVector::create(ROW({{"c0", INTEGER()}}), 0, pool);
  int numRows = 0;
  while (rowReader.next(10, result) > 0) {
    auto* rowVector = result->asUnchecked<RowVector>();
    ASSERT_EQ(2, rowVector->childrenSize());
    ASSERT_EQ(rowVector->type()->asRow().nameOf(1), "");
    DecodedVector values(*rowVector->childAt(0));
    DecodedVector rowNumbers(*rowVector->childAt(1));
    for (size_t i = 0; i < rowVector->size(); ++i) {
      ASSERT_EQ(values.valueAt<int32_t>(i), rowNumbers.valueAt<int64_t>(i));
    }
    numRows += result->size();
  }
  ASSERT_EQ(numRows, expectedNumRows);
}

std::pair<std::unique_ptr<dwrf::Writer>, std::unique_ptr<DwrfReader>>
createWriterReader(
    const std::vector<VectorPtr>& batches,
    memory::MemoryPool* pool,
    const std::shared_ptr<dwrf::Config>& config =
        std::make_shared<dwrf::Config>(),
    std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicy =
        E2EWriterTestUtil::simpleFlushPolicyFactory(true)) {
  auto sink =
      std::make_unique<MemorySink>(1 << 20, FileSink::Options{.pool = pool});
  auto* sinkPtr = sink.get();
  auto writer = E2EWriterTestUtil::writeData(
      std::move(sink),
      asRowType(batches[0]->type()),
      batches,
      config,
      std::move(flushPolicy));
  std::string_view data(sinkPtr->data(), sinkPtr->size());
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(data), *pool);
  dwio::common::ReaderOptions readerOpts(pool);
  readerOpts.setFileFormat(FileFormat::DWRF);
  auto reader = DwrfReader::create(std::move(input), readerOpts);
  return std::make_pair(std::move(writer), std::move(reader));
}

} // namespace

TEST_F(TestReader, appendRowNumberColumn) {
  std::vector<std::vector<int32_t>> integerValues{
      {0, 1, 2, 3, 4},
      {5, 6, 7},
      {8},
      {},
      {9, 10, 11, 12, 13, 14, 15},
  };
  auto batches = createBatches(integerValues);
  auto schema = asRowType(batches[0]->type());
  auto [writer, reader] = createWriterReader(batches, pool());

  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);
  rowReaderOpts.setAppendRowNumberColumn(true);
  {
    SCOPED_TRACE("Selective no filter");
    auto rowReader = reader->createRowReader(rowReaderOpts);
    verifyRowNumbers(*rowReader, pool(), 16);
  }
  spec->childByName("c0")->setFilter(
      common::createBigintValues({1, 4, 5, 7, 11, 14}, false));
  spec->resetCachedValues(true);
  {
    SCOPED_TRACE("Selective with filter");
    auto rowReader = reader->createRowReader(rowReaderOpts);
    verifyRowNumbers(*rowReader, pool(), 6);
  }
}

TEST_F(TestReader, reuseRowNumberColumn) {
  std::vector<std::vector<int32_t>> integerValues{{0, 1, 2, 3, 4}};
  auto batches = createBatches(integerValues);
  auto schema = asRowType(batches[0]->type());
  auto [writer, reader] = createWriterReader(batches, pool());

  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);
  rowReaderOpts.setAppendRowNumberColumn(true);
  {
    SCOPED_TRACE("Reuse passed in");
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto result =
        BaseVector::create(ROW({{"c0", INTEGER()}, {"", BIGINT()}}), 0, pool());
    auto* rowNum = result->asUnchecked<RowVector>()->childAt(1).get();
    ASSERT_EQ(rowReader->next(3, result), 3);
    ASSERT_EQ(rowNum, result->asUnchecked<RowVector>()->childAt(1).get());
  }
  {
    SCOPED_TRACE("Reuse generated");
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto result = BaseVector::create(ROW({{"c0", INTEGER()}}), 0, pool());
    ASSERT_EQ(rowReader->next(3, result), 3);
    auto* rowNum = result->asUnchecked<RowVector>()->childAt(1).get();
    ASSERT_EQ(rowReader->next(3, result), 2);
    ASSERT_EQ(rowNum, result->asUnchecked<RowVector>()->childAt(1).get());
  }
  {
    SCOPED_TRACE("No reuse passed in");
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto result =
        BaseVector::create(ROW({{"c0", INTEGER()}, {"", BIGINT()}}), 0, pool());
    auto rowNum = result->asUnchecked<RowVector>()->childAt(1);
    ASSERT_EQ(rowReader->next(3, result), 3);
    ASSERT_NE(rowNum.get(), result->asUnchecked<RowVector>()->childAt(1).get());
  }
  {
    SCOPED_TRACE("No reuse generated");
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto result = BaseVector::create(ROW({{"c0", INTEGER()}}), 0, pool());
    ASSERT_EQ(rowReader->next(3, result), 3);
    auto rowNum = result->asUnchecked<RowVector>()->childAt(1);
    ASSERT_EQ(rowReader->next(3, result), 2);
    ASSERT_NE(rowNum.get(), result->asUnchecked<RowVector>()->childAt(1).get());
  }
  {
    SCOPED_TRACE("No reuse type mismatch");
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto result = BaseVector::create(
        ROW({{"c0", INTEGER()}, {"", INTEGER()}}), 0, pool());
    auto rowNum = result->asUnchecked<RowVector>()->childAt(1);
    ASSERT_EQ(rowReader->next(3, result), 3);
    ASSERT_NE(rowNum.get(), result->asUnchecked<RowVector>()->childAt(1).get());
  }
}

TEST_F(TestReader, failToReuseReaderNulls) {
  auto c0 = makeRowVector(
      {"a", "b"},
      {
          makeFlatVector<int64_t>(11, folly::identity),
          makeFlatVector<int64_t>(
              11, folly::identity, [](auto i) { return i % 3 == 0; }),
      });
  // Set a null so that the children will not be loaded lazily.
  bits::setNull(c0->mutableRawNulls(), 10);
  auto data = makeRowVector({
      c0,
      makeRowVector({"c"}, {makeFlatVector<int64_t>(11, folly::identity)}),
  });
  auto schema = asRowType(data->type());
  auto [writer, reader] = createWriterReader({data}, pool());
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  spec->childByName("c0")->childByName("a")->setFilter(
      std::make_unique<common::BigintRange>(
          0, std::numeric_limits<int64_t>::max(), false));
  spec->childByName("c1")->childByName("c")->setFilter(
      std::make_unique<common::BigintRange>(0, 4, false));
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(schema, 0, pool());
  ASSERT_EQ(rowReader->next(10, result), 10);
  ASSERT_EQ(result->size(), 5);
  for (int i = 0; i < result->size(); ++i) {
    ASSERT_TRUE(result->equalValueAt(data.get(), i, i)) << result->toString(i);
  }
}

TEST_F(TestReader, readFlatMapsSomeEmpty) {
  // Test reading a flat map where the key filter means that some maps are
  // empty.
  auto keys = makeFlatVector(std::vector<int64_t>{
      1,
      2,
      3,
      4,
      5,
      6, // map 1 has more than just the selected keys.
      1,
      2,
      3, // map 2 has only selected keys.
      4,
      5,
      6, // map 3 has no selected keys.
      1,
      2,
      5,
      6 // map 4 has some selected keys.
  });
  auto values = makeFlatVector<int64_t>(16, folly::identity);
  auto maps =
      makeMapVector(std::vector<vector_size_t>{0, 6, 9, 12, 16}, keys, values);
  auto row = makeRowVector({"a"}, {maps});

  // Set up the config so that the maps are flattened.
  std::shared_ptr<dwrf::Config> config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});

  auto [writer, reader] = createWriterReader({row}, pool(), config);

  auto schema = asRowType(row->type());
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  spec->childByName("a")
      ->childByName(common::ScanSpec::kMapKeysFieldName)
      ->setFilter(common::createBigintValues({1, 2, 3}, false));
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);

  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch = BaseVector::create(schema, 0, pool());

  ASSERT_TRUE(rowReader->next(4, batch));
  auto rowVector = batch->as<RowVector>();
  auto resultMaps = rowVector->childAt(0)->loadedVector()->as<MapVector>();
  ASSERT_EQ(resultMaps->size(), 4);
  auto resultKeys = resultMaps->mapKeys()->as<SimpleVector<int64_t>>();
  auto resultValues = resultMaps->mapValues()->as<SimpleVector<int64_t>>();

  auto validate = [&](vector_size_t index,
                      vector_size_t expectedSize,
                      const std::unordered_set<int64_t>& expectedKeys,
                      const std::unordered_set<int64_t>& expectedValues) {
    ASSERT_FALSE(resultMaps->isNullAt(index));

    vector_size_t offset = resultMaps->offsetAt(index);
    vector_size_t size = resultMaps->sizeAt(index);
    ASSERT_EQ(size, expectedSize);

    std::unordered_set<int64_t> keySet;
    std::unordered_set<int64_t> valueSet;
    for (int i = offset; i < offset + size; i++) {
      keySet.insert(resultKeys->valueAt(i));
      valueSet.insert(resultValues->valueAt(i));
    }

    EXPECT_EQ(keySet, expectedKeys);
    EXPECT_EQ(valueSet, expectedValues);
  };

  validate(0, 3, {1, 2, 3}, {0, 1, 2});
  validate(1, 3, {1, 2, 3}, {6, 7, 8});
  validate(2, 0, {}, {});
  validate(3, 2, {1, 2}, {12, 13});
}

TEST_F(TestReader, readFlatMapsWithNullMaps) {
  // Test reading a flat map where the key filter means that some maps are
  // empty.
  auto keys =
      makeFlatVector<int64_t>(16, [](vector_size_t row) { return row % 4; });
  auto values = makeFlatVector<int64_t>(16, folly::identity);
  auto maps = makeMapVector(
      std::vector<vector_size_t>{0, 4, 4, 8, 8, 12, 12, 16, 16},
      keys,
      values,
      {1, 3, 5, 7});
  auto row = makeRowVector({"a"}, {maps});

  // Set up the config so that the maps are flattened.
  std::shared_ptr<dwrf::Config> config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});

  auto [writer, reader] = createWriterReader({row}, pool(), config);

  auto schema = asRowType(row->type());
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  spec->childByName("a")
      ->childByName(common::ScanSpec::kMapKeysFieldName)
      ->setFilter(common::createBigintValues({1, 2, 3}, false));
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);

  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch = BaseVector::create(schema, 0, pool());

  ASSERT_TRUE(rowReader->next(8, batch));
  auto rowVector = batch->as<RowVector>();
  auto resultMaps = rowVector->childAt(0)->loadedVector()->as<MapVector>();
  ASSERT_EQ(resultMaps->size(), 8);
  auto resultKeys = resultMaps->mapKeys()->as<SimpleVector<int64_t>>();
  auto resultValues = resultMaps->mapValues()->as<SimpleVector<int64_t>>();

  for (int mapIndex = 0; mapIndex < 8; mapIndex++) {
    if (mapIndex % 2 != 0) {
      ASSERT_TRUE(resultMaps->isNullAt(mapIndex));
    } else {
      ASSERT_FALSE(resultMaps->isNullAt(mapIndex));

      vector_size_t offset = resultMaps->offsetAt(mapIndex);
      vector_size_t size = resultMaps->sizeAt(mapIndex);
      ASSERT_EQ(size, 3);

      std::unordered_set<int64_t> keySet;
      std::unordered_set<int64_t> valueSet;
      for (int i = offset; i < offset + size; i++) {
        keySet.insert(resultKeys->valueAt(i));
        valueSet.insert(resultValues->valueAt(i));
      }

      EXPECT_EQ(keySet, (std::unordered_set<int64_t>{1, 2, 3}));
      EXPECT_EQ(
          valueSet,
          (std::unordered_set<int64_t>{
              4 * mapIndex / 2 + 1,
              4 * mapIndex / 2 + 2,
              4 * mapIndex / 2 + 3}));
    }
  }
}

TEST_F(TestReader, readStructWithWholeBatchFiltered) {
  // Test reading a struct with a pushdown filter that filters out all rows
  // for a certain batch.
  auto rowType = ROW({"a"}, {BIGINT()});
  const vector_size_t vectorSize = 20;
  const vector_size_t batchSize = 10;
  std::vector<VectorPtr> children{makeFlatVector<int64_t>(
      vectorSize,
      folly::identity,
      // In the first batch, the parent Rows will all be null.
      [&](auto i) { return i < batchSize; })};

  BufferPtr nulls = AlignedBuffer::allocate<bool>(vectorSize, pool());
  uint64_t* rawNulls = nulls->asMutable<uint64_t>();
  memset(rawNulls, bits::kNotNullByte, nulls->capacity());
  // Mark the Row as null in the first batch.
  for (int i = 0; i < batchSize; ++i) {
    bits::setNull(rawNulls, i, true);
  }

  auto c0 =
      std::make_shared<RowVector>(pool(), rowType, nulls, vectorSize, children);
  auto row = makeRowVector({"c0"}, {c0});

  auto [writer, reader] = createWriterReader({row}, pool());

  auto schema = asRowType(row->type());
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  // Create a filter that will filter out all rows in the first batch.
  spec->childByName("c0")->setFilter(std::make_unique<common::IsNotNull>());
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);

  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch = BaseVector::create(schema, 0, pool());

  ASSERT_TRUE(rowReader->next(batchSize, batch));
  // Confirm that all rows were filtered out.
  ASSERT_EQ(batch->size(), 0);
  ASSERT_TRUE(rowReader->next(batchSize, batch));
  // None of the rows should be filtered out in the second batch.
  ASSERT_EQ(batch->size(), 10);
  // Validate that we successfully read the batch.
  auto rowVector = batch->as<RowVector>();
  auto resultRows = rowVector->childAt(0)->loadedVector()->as<RowVector>();
  ASSERT_EQ(resultRows->size(), batchSize);

  for (int i = 0; i < batchSize; ++i) {
    ASSERT_FALSE(resultRows->isNullAt(i));
  }

  auto resultValues =
      resultRows->childAt(0)->loadedVector()->as<FlatVector<int64_t>>();
  ASSERT_EQ(resultValues->size(), batchSize);

  for (int i = 0; i < batchSize; ++i) {
    ASSERT_FALSE(resultValues->isNullAt(i));
    ASSERT_EQ(resultValues->valueAt(i), i + 10);
  }
}

TEST_F(TestReader, readStringDictionaryAsFlat) {
  std::vector<std::string> dictionary;
  for (int i = 0; i < 26; ++i) {
    dictionary.emplace_back(20 + i, 'a' + i);
  }
  auto indices = allocateIndices(200, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (int i = 0; i < 200; ++i) {
    rawIndices[i] = i % dictionary.size();
  }
  auto batch = makeRowVector({
      BaseVector::wrapInDictionary(
          nullptr, indices, 200, makeFlatVector(dictionary)),
  });
  auto [writer, reader] = createWriterReader(
      {batch},
      pool(),
      std::make_shared<dwrf::Config>(),
      // The always true flush policy would disable dictionary encoding at least
      // for first batch.
      E2EWriterTestUtil::simpleFlushPolicyFactory(false));
  auto rowType = reader->rowType();
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*rowType);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto actual = BaseVector::create(rowType, 0, pool());
  ASSERT_EQ(rowReader->next(20, actual), 20);
  ASSERT_EQ(actual->size(), 20);
  auto* c0 = actual->as<RowVector>()->childAt(0)->loadedVector();
  ASSERT_EQ(c0->encoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_TRUE(c0->valueVector()->isFlatEncoding());
  ASSERT_EQ(c0->valueVector()->size(), dictionary.size());
  dwio::common::RuntimeStatistics stats;
  rowReader->updateRuntimeStats(stats);
  ASSERT_EQ(stats.columnReaderStatistics.flattenStringDictionaryValues, 0);
  spec->childByName("c0")->setFilter(std::make_unique<common::BytesValues>(
      std::vector<std::string>{"aaaaaaaaaaaaaaaaaaaa"}, false));
  spec->resetCachedValues(true);
  rowReader = reader->createRowReader(rowReaderOpts);
  ASSERT_EQ(rowReader->next(20, actual), 20);
  ASSERT_EQ(actual->size(), 1);
  ASSERT_TRUE(actual->as<RowVector>()->childAt(0)->isFlatEncoding());
  stats = {};
  rowReader->updateRuntimeStats(stats);
  ASSERT_EQ(stats.columnReaderStatistics.flattenStringDictionaryValues, 1);
}

// A primitive subfield is missing in file, and result is not reused.
TEST_F(TestReader, missingSubfieldsNoResultReusing) {
  constexpr int kSize = 10;
  auto batch = makeRowVector({
      makeRowVector({
          makeFlatVector<int64_t>(kSize, folly::identity),
      }),
  });
  auto [writer, reader] = createWriterReader({batch}, pool());
  auto schema = ROW({{"c0", ROW({{"c0", BIGINT()}, {"c1", VARCHAR()}})}});
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto actual = BaseVector::create(schema, 0, pool());
  // Hold a second reference to result so it cannot be reused.
  auto actual2 = actual;
  ASSERT_EQ(rowReader->next(1024, actual), 10);
  auto expected = makeRowVector({
      makeRowVector({
          makeFlatVector<int64_t>(kSize, folly::identity),
          BaseVector::createNullConstant(VARCHAR(), kSize, pool()),
      }),
  });
  assertEqualVectors(expected, actual);
}

// Ensure there is enough data before switching to fast path.
TEST_F(TestReader, selectiveStringDirectFastPath) {
  auto genStr = [](auto i) {
    return i == 0 ? "x" : i < 8 ? "" : "xxxxxxxxxxx";
  };
  auto batch = makeRowVector({
      makeFlatVector<int64_t>(17, [](auto i) { return i != 8; }),
      makeFlatVector<StringView>(17, genStr),
  });
  auto [writer, reader] = createWriterReader({batch}, pool());
  auto schema = asRowType(batch->type());
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);
  spec->childByName("c0")->setFilter(common::createBigintValues({1}, false));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto actual = BaseVector::create(schema, 0, pool());
  ASSERT_EQ(rowReader->next(1024, actual), batch->size());
  auto expected = makeRowVector({
      makeConstant<int64_t>(1, 16),
      makeFlatVector<StringView>(16, genStr),
  });
  assertEqualVectors(expected, actual);
}

TEST_F(TestReader, selectiveStringDirect) {
  auto genStr = [](auto i) {
    static const std::string s(2048, 'x');
    return i == 0 || i == 8 ? s.c_str() : "";
  };
  auto batch = makeRowVector({
      makeFlatVector<int64_t>(17, [](auto i) { return i != 15; }),
      makeFlatVector<StringView>(17, genStr),
  });
  auto [writer, reader] = createWriterReader({batch}, pool());
  auto schema = asRowType(batch->type());
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);
  spec->childByName("c0")->setFilter(common::createBigintValues({1}, false));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto actual = BaseVector::create(schema, 0, pool());
  ASSERT_EQ(rowReader->next(1024, actual), batch->size());
  auto expected = makeRowVector({
      makeConstant<int64_t>(1, 16),
      makeFlatVector<StringView>(16, genStr),
  });
  assertEqualVectors(expected, actual);
}

TEST_F(TestReader, selectiveFlatMapFastPathAllInlinedStringKeys) {
  auto maps = makeMapVector<std::string, int64_t>(
      {{{"a", 0}, {"b", 0}}, {{"a", 1}, {"b", 1}}});
  auto row = makeRowVector({"c0"}, {maps});
  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set(dwrf::Config::MAP_FLAT_COLS, {0});
  auto [writer, reader] = createWriterReader({row}, pool(), config);
  auto schema = asRowType(row->type());
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*schema);
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setScanSpec(spec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  VectorPtr batch = BaseVector::create(schema, 0, pool());
  ASSERT_EQ(rowReader->next(10, batch), 2);
  assertEqualVectors(batch, row);
}
