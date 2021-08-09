/*
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
#include "velox/common/base/test_utils/GTestUtils.h"
#include "velox/common/caching/DataCache.h"
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/OrcTest.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include <fmt/core.h>
#include <array>
#include <numeric>

using namespace ::testing;
using namespace facebook::dwio::common;
using namespace facebook::dwio::type::fbhive;
using namespace facebook::velox;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::test;

const std::string structFile(getExampleFilePath("struct.orc"));

TEST(TestReader, testWriterVersions) {
  EXPECT_EQ("original", writerVersionToString(ORIGINAL));
  EXPECT_EQ("dwrf-4.9", writerVersionToString(DWRF_4_9));
  EXPECT_EQ("dwrf-5.0", writerVersionToString(DWRF_5_0));
  EXPECT_EQ("dwrf-6.0", writerVersionToString(DWRF_6_0));
  EXPECT_EQ(
      "future - 99", writerVersionToString(static_cast<WriterVersion>(99)));
}

TEST(TestReader, testCompressionNames) {
  EXPECT_EQ("none", compressionKindToString(CompressionKind_NONE));
  EXPECT_EQ("zlib", compressionKindToString(CompressionKind_ZLIB));
  EXPECT_EQ("snappy", compressionKindToString(CompressionKind_SNAPPY));
  EXPECT_EQ("lzo", compressionKindToString(CompressionKind_LZO));
  EXPECT_EQ("lz4", compressionKindToString(CompressionKind_LZ4));
  EXPECT_EQ("zstd", compressionKindToString(CompressionKind_ZSTD));
  EXPECT_EQ(
      "unknown - 99",
      compressionKindToString(static_cast<CompressionKind>(99)));
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
    const std::string& file,
    const int32_t seeks[],
    const int32_t expectedBatchSize[],
    const int32_t numBatches,
    bool returnFlatVector) {
  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  rowReaderOpts.setReturnFlatVector(returnFlatVector);
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
          id:int,\
      map1:map<int, array<float>>,\
      map2:map<string, map<smallint,bigint>>,\
      map3:map<int,int>,\
      map4:map<int,struct<field1:int,field2:float,field3:string>>,\
      memo:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(requestedType));
  auto reader =
      DwrfReader::create(std::make_unique<FileInputStream>(file), readerOpts);
  auto rowReader = reader->createRowReader(rowReaderOpts);
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

class TestFlatMapReader : public TestWithParam<bool> {};

TEST_P(TestFlatMapReader, testStringKeyLifeCycle) {
  const std::string fmSmall(getExampleFilePath("fm_small.orc"));
  auto returnFlatVector = GetParam();

  VectorPtr batch;
  ReaderOptions readerOptions;

  {
    RowReaderOptions rowReaderOptions;
    rowReaderOptions.setReturnFlatVector(returnFlatVector);

    auto reader = DwrfReader::create(
        std::make_unique<FileInputStream>(fmSmall), readerOptions);
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
  const std::string fmSmall(getExampleFilePath("fm_small.orc"));

  // batch size is set as 1000 in reading
  const std::array<int32_t, 4> seeks{100, 700, 0, 0};
  const std::array<int32_t, 3> expectedBatchSize{200, 200, 100};
  auto returnFlatVector = GetParam();
  verifyFlatMapReading(
      fmSmall,
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      returnFlatVector);
}

TEST_P(TestFlatMapReader, testReadFlatMapSampleSmall) {
  const std::string fmSmall(getExampleFilePath("fm_small.orc"));

  // batch size is set as 1000 in reading
  std::array<int32_t, 5> seeks;
  seeks.fill(0);
  const std::array<int32_t, 4> expectedBatchSize{300, 300, 300, 100};
  auto returnFlatVector = GetParam();
  verifyFlatMapReading(
      fmSmall,
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      returnFlatVector);
}

TEST_P(TestFlatMapReader, testReadFlatMapSampleLarge) {
  const std::string fmLarge(getExampleFilePath("fm_large.orc"));

  // batch size is set as 1000 in reading
  // 3000 per stripe
  std::array<int32_t, 11> seeks;
  seeks.fill(0);
  const std::array<int32_t, 10> expectedBatchSize{
      1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000};
  auto returnFlatVector = GetParam();
  verifyFlatMapReading(
      fmLarge,
      seeks.data(),
      expectedBatchSize.data(),
      expectedBatchSize.size(),
      returnFlatVector);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    FlatMapReaderTests,
    TestFlatMapReader,
    Values(true, false));

class TestFlatMapReaderFlatLayout
    : public TestWithParam<std::tuple<bool, size_t>> {};

TEST_P(TestFlatMapReaderFlatLayout, testCompare) {
  const std::string fmSmall(getExampleFilePath("fm_small.orc"));

  ReaderOptions readerOptions;
  auto reader = DwrfReader::create(
      std::make_unique<FileInputStream>(fmSmall), readerOptions);
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

TEST(TestReader, testReadFlatMapWithKeyFilters) {
  const std::string fmSmall(getExampleFilePath("fm_small.orc"));

  // batch size is set as 1000 in reading
  // file has schema: a int, b struct<a:int, b:float, c:string>, c float
  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
          id:int,\
      map1:map<int, array<float>>,\
      map2:map<string, map<smallint,bigint>>,\
      map3:map<int,int>,\
      map4:map<int,struct<field1:int,field2:float,field3:string>>,\
      memo:string>"));
  // set map key filter for map1 we only need key=1, and map2 only key-1
  auto cs = std::make_shared<ColumnSelector>(
      requestedType, std::vector<std::string>{"map1#[1]", "map2#[\"key-1\"]"});
  rowReaderOpts.select(cs);
  auto reader = DwrfReader::create(
      std::make_unique<FileInputStream>(fmSmall), readerOpts);
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

namespace {
std::unordered_set<uint32_t> getNodeIdsFromColumnNames(
    const std::vector<std::string>& columns,
    const ColumnSelector& cs) {
  const auto schema = cs.getSchemaWithId();
  const auto names = schema->type->as<TypeKind::ROW>().names();

  // Build name to id lookup
  std::unordered_map<std::string, uint32_t> nameToIdLookup;
  for (uint32_t i = 0; i < names.size(); ++i) {
    nameToIdLookup[names[i]] = schema->childAt(i)->id;
  }

  std::unordered_set<uint32_t> mapColumnIdAsStruct;

  for (const auto& column : columns) {
    const auto itName = nameToIdLookup.find(column);
    if (itName != nameToIdLookup.end()) {
      mapColumnIdAsStruct.insert(itName->second);
    }
  }

  return mapColumnIdAsStruct;
};

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
    const std::string& filename,
    size_t batchSize = 1000) {
  ReaderOptions readerOpts;
  auto reader = DwrfReader::create(
      std::make_unique<FileInputStream>(filename), readerOpts);

  const std::vector<int32_t> projections{
      1, 2, 3, 4, 5, -99999999 /* does not exist */};
  const std::string projectedColumn = "map1";
  const vector_size_t projectedColumnIndex = 1;

  auto cs = std::make_shared<ColumnSelector>(
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
          id:int,\
      map1:map<int, array<float>>,\
      map2:map<string, map<smallint,bigint>>,\
      map3:map<int,int>,\
      map4:map<int,struct<field1:int,field2:float,field3:string>>,\
      memo:string>")),
      std::vector<std::string>{
          projectedColumn + "#[" + folly::join(", ", projections) + "]"});

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(cs);

  auto mapEncodingReader = reader->createRowReader(rowReaderOpts);

  rowReaderOpts.setFlatmapNodeIdsAsStruct(
      getNodeIdsFromColumnNames({"map1"}, *cs));
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

      for (size_t i = 0; i < projections.size(); ++i) {
        verifyMapColumnEqual(
            rowMapEncoding->childAt(projectedColumnIndex)->as<MapVector>(),
            rowStructEncoding->childAt(projectedColumnIndex)->as<RowVector>(),
            projections[i],
            i);
      }
    } while (true);
  };
  compare();
}
} // namespace

TEST(TestReader, testFlatmapAsStructSmall) {
  verifyFlatmapStructEncoding(getExampleFilePath("fm_small.orc"));
}

TEST(TestReader, testFlatmapAsStructSmallEmptyInmap) {
  verifyFlatmapStructEncoding(getExampleFilePath("fm_small.orc"), 2);
}

TEST(TestReader, testFlatmapAsStructLarge) {
  verifyFlatmapStructEncoding(getExampleFilePath("fm_large.orc"));
}

TEST(TestReader, testFlatmapAsStructRequiringProjection) {
  const std::string fmSmall(getExampleFilePath("fm_small.orc"));

  ReaderOptions readerOpts;
  auto reader = DwrfReader::create(
      std::make_unique<FileInputStream>(fmSmall), readerOpts);

  auto cs = std::make_shared<ColumnSelector>(
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse("struct<\
          id:int,\
      map1:map<int, array<float>>,\
      map2:map<string, map<smallint,bigint>>,\
      map3:map<int,int>,\
      map4:map<int,struct<field1:int,field2:float,field3:string>>,\
      memo:string>")),
      std::vector<std::string>{});

  RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(cs);
  rowReaderOpts.setFlatmapNodeIdsAsStruct(
      getNodeIdsFromColumnNames({"map1"}, *cs));

  auto rowReader = reader->createRowReader(rowReaderOpts);

  VectorPtr batch;
  EXPECT_THROW(rowReader->next(1000, batch), VeloxException);
}

// TODO: replace with mock
TEST(TestReader, testMismatchSchemaMoreFields) {
  // file has schema: a int, b struct<a:int, b:float, c:string>, c float
  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:int,b:struct<a:int,b:float,c:string>,c:float,d:string>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<uint64_t>{1, 2, 3}));
  auto reader = DwrfReader::create(
      std::make_unique<FileInputStream>(structFile), readerOpts);
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
      std::make_unique<FileInputStream>(structFile), readerOpts);
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

TEST(TestReader, testMismatchSchemaFewerFields) {
  // file has schema: a int, b struct<a:int, b:float, c:string>, c float
  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:int,b:struct<a:int,b:float,c:string>>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<uint64_t>{1}));
  auto reader = DwrfReader::create(
      std::make_unique<FileInputStream>(structFile), readerOpts);
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
      std::make_unique<FileInputStream>(structFile), readerOpts);
  rowReader = reader->createRowReader(rowReaderOpts);
  rowReader->next(1, batch);

  {
    auto root = std::dynamic_pointer_cast<RowVector>(batch);
    // We should have 1 column since projection is pushed
    EXPECT_EQ(1, root->childrenSize());
    EXPECT_EQ(1, root->size());
  }
}

TEST(TestReader, testMismatchSchemaNestedMoreFields) {
  // file has schema: a int, b struct<a:int, b:float>, c float
  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:int,b:struct<a:int,b:float,c:string,d:binary>,c:float>"));
  LOG(INFO) << requestedType->toString();
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<std::string>{"b.b", "b.c", "b.d", "c"}));
  auto reader = DwrfReader::create(
      std::make_unique<FileInputStream>(structFile), readerOpts);
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
      std::make_unique<FileInputStream>(structFile), readerOpts);
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

TEST(TestReader, testMismatchSchemaNestedFewerFields) {
  // file has schema: a int, b struct<a:int, b:float>, c float
  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  std::shared_ptr<const RowType> requestedType =
      std::dynamic_pointer_cast<const RowType>(HiveTypeParser().parse(
          "struct<a:int,b:struct<a:int,b:float>,c:float>"));
  rowReaderOpts.select(std::make_shared<ColumnSelector>(
      requestedType, std::vector<std::string>{"b.b", "c"}));
  auto reader = DwrfReader::create(
      std::make_unique<FileInputStream>(structFile), readerOpts);
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
      std::make_unique<FileInputStream>(structFile), readerOpts);
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

TEST(TestReader, testMismatchSchemaIncompatible) {
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

TEST(TestReader, testUpcastBoolean) {
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
  std::unique_ptr<ColumnReader> reader = ColumnReader::build(
      TypeWithId::create(reqType), TypeWithId::create(rowType), streams, false);

  VectorPtr batch;
  reader->next(104, batch);

  auto lv = std::dynamic_pointer_cast<FlatVector<int32_t>>(
      std::dynamic_pointer_cast<RowVector>(batch)->childAt(0));

  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(lv->valueAt(i), i % 2);
  }
}

TEST(TestReader, testUpcastIntDirect) {
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
  std::unique_ptr<ColumnReader> reader = ColumnReader::build(
      TypeWithId::create(reqType), TypeWithId::create(rowType), streams, false);

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

TEST(TestReader, testUpcastIntDict) {
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
  std::unique_ptr<ColumnReader> reader = ColumnReader::build(
      TypeWithId::create(reqType), TypeWithId::create(rowType), streams, false);

  VectorPtr batch;
  reader->next(100, batch);

  auto lv = std::dynamic_pointer_cast<FlatVector<int64_t>>(
      std::dynamic_pointer_cast<RowVector>(batch)->childAt(0));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(lv->valueAt(i), i);
  }
}

TEST(TestReader, testUpcastFloat) {
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
  std::unique_ptr<ColumnReader> reader = ColumnReader::build(
      TypeWithId::create(reqType), TypeWithId::create(rowType), streams, false);

  VectorPtr batch;
  reader->next(100, batch);

  auto lv = std::dynamic_pointer_cast<FlatVector<double>>(
      std::dynamic_pointer_cast<RowVector>(batch)->childAt(0));
  for (size_t i = 0; i < batch->size(); ++i) {
    EXPECT_EQ(lv->valueAt(i), static_cast<double>(i));
  }
}

TEST(TestReader, testEmptyFile) {
  std::unique_ptr<memory::ScopedMemoryPool> scopedPool =
      memory::getDefaultScopedMemoryPool();
  auto pool = *scopedPool;
  MemorySink sink{pool, 1024};
  DataBufferHolder holder{pool, 1024, 0, DEFAULT_PAGE_GROW_RATIO, &sink};
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

  DataBuffer<char> buf{pool, 1};
  buf.data()[0] = psLen;
  sink.write(std::move(buf));
  auto input = std::make_unique<MemoryInputStream>(sink.getData(), sink.size());

  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;

  auto rowReader = DwrfReader::create(std::move(input), readerOpts)
                       ->createRowReader(rowReaderOpts);
  VectorPtr batch;
  EXPECT_FALSE(rowReader->next(1, batch));
  EXPECT_FALSE(batch);
}
