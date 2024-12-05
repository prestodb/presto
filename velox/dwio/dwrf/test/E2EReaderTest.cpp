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

#include "folly/Conv.h"
#include "folly/Random.h"
#include "folly/String.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/utils/E2EWriterTestUtil.h"
#include "velox/type/fbhive/HiveTypeParser.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::test;
using namespace facebook::velox::type::fbhive;
using namespace facebook::velox;

namespace {

class RemoveGuard {
 public:
  explicit RemoveGuard(std::string path) : path_(std::move(path)) {}

  ~RemoveGuard() {
    std::remove(path_.c_str());
  }

 private:
  std::string path_;
};

enum class Decoding { SERIAL, PARALLEL };
enum class FlatMapAs { MAP, STRUCT };

class ValueTypes {
  static constexpr size_t kParallelismFactor = 2;

 public:
  ValueTypes(
      Decoding decoding,
      FlatMapAs flatMapAs,
      std::initializer_list<std::string> values)
      : values_(std::move(values)),
        executor_{
            decoding == Decoding::PARALLEL
                ? std::make_shared<folly::CPUThreadPoolExecutor>(
                      kParallelismFactor)
                : nullptr},
        asStruct_{flatMapAs == FlatMapAs::STRUCT} {}

  auto size() const {
    return values_.size();
  }

  auto begin() const {
    return values_.begin();
  }

  auto end() const {
    return values_.end();
  }

  const std::shared_ptr<folly::Executor>& decodingExecutor() const {
    return executor_;
  }

  const size_t decodingParallelismFactor() const {
    return executor_ ? kParallelismFactor : 0;
  }

  bool isReadAsStruct() const {
    return asStruct_;
  }

 private:
  std::vector<std::string> values_;
  std::shared_ptr<folly::Executor> executor_;
  bool asStruct_;
};

class E2EReaderTest : public testing::TestWithParam<ValueTypes> {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};
} // namespace

TEST_P(E2EReaderTest, SharedDictionaryFlatmapReadAsStruct) {
  const size_t batchCount = 10;
  size_t size = 1;
  auto pool = memory::memoryManager()->addLeafPool();

  std::vector<uint32_t> flatMapCols(GetParam().size());
  std::iota(flatMapCols.begin(), flatMapCols.end(), 0);

  std::string schema = "struct<";
  for (auto& valueType : GetParam()) {
    schema += folly::to<std::string>("map_val:map<int,", valueType, ">,");
  }
  schema.append(">");
  HiveTypeParser parser;
  auto type = parser.parse(schema);

  auto config = std::make_shared<dwrf::Config>();
  config->set(dwrf::Config::ROW_INDEX_STRIDE, static_cast<uint32_t>(1000));
  config->set(dwrf::Config::FLATTEN_MAP, true);
  config->set<const std::vector<uint32_t>>(
      dwrf::Config::MAP_FLAT_COLS, flatMapCols);
  config->set(dwrf::Config::MAP_FLAT_DISABLE_DICT_ENCODING, false);
  config->set(dwrf::Config::MAP_FLAT_DISABLE_DICT_ENCODING_STRING, false);
  config->set(dwrf::Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD, 1.0f);
  config->set(dwrf::Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD, 1.0f);
  config->set(dwrf::Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD, 0.0f);

  auto suffix = folly::to<std::string>(folly::Random::secureRand32());
  auto path = "/tmp/e2e_reader_test_shared_dictionary_" + suffix + ".orc";
  RemoveGuard guard(path);
  auto localWriteFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink = std::make_unique<WriteFileSink>(std::move(localWriteFile), path);
  auto writer = E2EWriterTestUtil::createWriter(
      std::move(sink),
      type,
      config,
      E2EWriterTestUtil::simpleFlushPolicyFactory(true));

  auto seed = folly::Random::secureRand32();
  LOG(INFO) << "seed: " << seed;
  std::mt19937 gen(seed);
  auto cs = std::make_shared<ColumnSelector>(
      std::dynamic_pointer_cast<const RowType>(type));
  std::unordered_map<uint32_t, std::unordered_set<std::string>>
      structEncodingProtoMap;
  const auto schemaWithId = cs->getSchemaWithId();
  for (size_t i = 0; i < batchCount; ++i) {
    auto batch = BatchMaker::createBatch(type, size, *pool, gen);
    for (size_t col = 0, schemaSize = schemaWithId->size(); col < schemaSize;
         ++col) {
      auto& set = structEncodingProtoMap[schemaWithId->childAt(col)->id()];
      auto keys = batch->as<RowVector>()
                      ->childAt(col)
                      ->as<MapVector>()
                      ->mapKeys()
                      ->as<SimpleVector<int32_t>>();
      for (vector_size_t featureIdx = 0, featureCount = keys->size();
           featureIdx < featureCount;
           ++featureIdx) {
        set.insert(keys->toString(featureIdx));
      }
    }
    writer->write(std::move(batch));
    size = std::min(size * 2, 2048UL);
  }
  writer->close();
  writer.reset();

  dwio::common::ReaderOptions readerOpts{pool.get()};
  auto bufferedInput = std::make_unique<BufferedInput>(
      std::make_shared<LocalReadFile>(path), *pool);
  auto reader = DwrfReader::create(std::move(bufferedInput), readerOpts);

  RowReaderOptions rowReaderOptions;
  rowReaderOptions.setDecodingExecutor(GetParam().decodingExecutor());
  rowReaderOptions.setDecodingParallelismFactor(
      GetParam().decodingParallelismFactor());
  rowReaderOptions.select(cs);

  const bool asStruct = GetParam().isReadAsStruct();
  if (asStruct) {
    std::unordered_map<uint32_t, std::vector<std::string>> structEncodingMap;
    for (auto& [id, keys] : structEncodingProtoMap) {
      structEncodingMap[id].reserve(keys.size());
      for (auto& key : keys) {
        structEncodingMap[id].push_back(key);
      }
    }
    rowReaderOptions.setFlatmapNodeIdsAsStruct(structEncodingMap);
  }

  auto rowReader = reader->createRowReader(rowReaderOptions);

  VectorPtr batch;
  while (rowReader->next(100, batch)) {
    ASSERT_TRUE(type->isRow());
    ASSERT_TRUE(batch->type()->isRow());
    auto& schemaRow = type->as<TypeKind::ROW>();
    auto& resultTypeRow = batch->type()->as<TypeKind::ROW>();
    auto* batchRow = batch->as<RowVector>();
    ASSERT_EQ(schemaRow.size(), resultTypeRow.size());
    for (size_t col = 0, columns = schemaRow.size(); col < columns; ++col) {
      auto& schemaChild = schemaRow.childAt(col)->as<TypeKind::MAP>();
      ASSERT_TRUE(schemaRow.childAt(col)->isMap());
      if (asStruct) {
        // Type should be ROW since it's struct encoding
        ASSERT_TRUE(resultTypeRow.childAt(col)->isRow());
        ASSERT_EQ(batchRow->childAt(col)->typeKind(), TypeKind::ROW);
        auto& resultTypeChild = resultTypeRow.childAt(col)->as<TypeKind::ROW>();
        auto* batchRowChild = batchRow->childAt(col)->as<RowVector>();
        ASSERT_EQ(resultTypeChild.size(), batchRowChild->children().size());
        for (uint32_t feature = 0, features = resultTypeChild.size();
             feature < features;
             ++feature) {
          ASSERT_EQ(
              schemaChild.valueType()->kind(),
              resultTypeChild.childAt(feature)->kind());
          ASSERT_EQ(
              resultTypeChild.childAt(feature)->kind(),
              batchRowChild->childAt(feature)->typeKind());
        }
      } else {
        ASSERT_TRUE(resultTypeRow.childAt(col)->isMap());
        ASSERT_EQ(batchRow->childAt(col)->typeKind(), TypeKind::MAP);
        auto& resultTypeChild = resultTypeRow.childAt(col)->as<TypeKind::MAP>();
        auto* batchRowChild = batchRow->childAt(col)->as<MapVector>();
        ASSERT_EQ(
            resultTypeChild.keyType()->kind(), schemaChild.keyType()->kind());
        ASSERT_EQ(
            resultTypeChild.valueType()->kind(),
            schemaChild.valueType()->kind());
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    SingleTypesSerialMap,
    E2EReaderTest,
    ValuesIn(std::vector<ValueTypes>{
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"tinyint"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"smallint"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"integer"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"bigint"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"string"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"array<tinyint>"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"array<smallint>"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"array<integer>"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"array<bigint>"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::MAP, {"array<string>"})}));

INSTANTIATE_TEST_SUITE_P(
    SingleTypesSerialStruct,
    E2EReaderTest,
    ValuesIn(std::vector<ValueTypes>{
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"tinyint"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"smallint"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"integer"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"bigint"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"string"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"array<tinyint>"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"array<smallint>"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"array<integer>"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"array<bigint>"}),
        ValueTypes(Decoding::SERIAL, FlatMapAs::STRUCT, {"array<string>"})}));

INSTANTIATE_TEST_SUITE_P(
    AllTypesSerialMap,
    E2EReaderTest,
    ValuesIn(std::vector<ValueTypes>{ValueTypes(
        Decoding::SERIAL,
        FlatMapAs::MAP,
        {"tinyint",
         "smallint",
         "integer",
         "bigint",
         "string",
         "array<tinyint>",
         "array<smallint>",
         "array<integer>",
         "array<bigint>",
         "array<string>"})}));

INSTANTIATE_TEST_SUITE_P(
    AllTypesSerialStruct,
    E2EReaderTest,
    ValuesIn(std::vector<ValueTypes>{ValueTypes(
        Decoding::SERIAL,
        FlatMapAs::STRUCT,
        {"tinyint",
         "smallint",
         "integer",
         "bigint",
         "string",
         "array<tinyint>",
         "array<smallint>",
         "array<integer>",
         "array<bigint>",
         "array<string>"})}));

INSTANTIATE_TEST_SUITE_P(
    SingleTypesParallelMap,
    E2EReaderTest,
    ValuesIn(std::vector<ValueTypes>{
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"tinyint"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"smallint"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"integer"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"bigint"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"string"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"array<tinyint>"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"array<smallint>"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"array<integer>"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"array<bigint>"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::MAP, {"array<string>"})}));

INSTANTIATE_TEST_SUITE_P(
    SingleTypesParallelStruct,
    E2EReaderTest,
    ValuesIn(std::vector<ValueTypes>{
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"tinyint"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"smallint"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"integer"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"bigint"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"string"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"array<tinyint>"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"array<smallint>"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"array<integer>"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"array<bigint>"}),
        ValueTypes(Decoding::PARALLEL, FlatMapAs::STRUCT, {"array<string>"})}));

INSTANTIATE_TEST_SUITE_P(
    AllTypesParallelMap,
    E2EReaderTest,
    ValuesIn(std::vector<ValueTypes>{ValueTypes(
        Decoding::PARALLEL,
        FlatMapAs::MAP,
        {"tinyint",
         "smallint",
         "integer",
         "bigint",
         "string",
         "array<tinyint>",
         "array<smallint>",
         "array<integer>",
         "array<bigint>",
         "array<string>"})}));

INSTANTIATE_TEST_SUITE_P(
    AllTypesParallelStruct,
    E2EReaderTest,
    ValuesIn(std::vector<ValueTypes>{ValueTypes(
        Decoding::PARALLEL,
        FlatMapAs::STRUCT,
        {"tinyint",
         "smallint",
         "integer",
         "bigint",
         "string",
         "array<tinyint>",
         "array<smallint>",
         "array<integer>",
         "array<bigint>",
         "array<string>"})}));
