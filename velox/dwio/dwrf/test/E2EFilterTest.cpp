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

#include "velox/common/base/Portability.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/E2EFilterTestBase.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/Writer.h"

#include <folly/init/Init.h>

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;
using namespace facebook::velox;
using namespace facebook::velox::common;

using dwio::common::MemorySink;

class E2EFilterTest : public E2EFilterTestBase {
 protected:
  void testWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations,
      bool tryNoNulls = false,
      bool tryNoVInts = false) {
    for (int32_t noVInts = 0; noVInts < (tryNoVInts ? 2 : 1); ++noVInts) {
      useVInts_ = !noVInts;
      for (int32_t noNulls = 0; noNulls < (tryNoNulls ? 2 : 1); ++noNulls) {
        LOG(INFO) << "Running with " << (noNulls ? " no nulls " : "nulls")
                  << " and " << (noVInts ? " no VInts " : " VInts ")
                  << std::endl;
        auto newCustomize = customize;
        if (noNulls) {
          newCustomize = [&]() {
            customize();
            makeNotNull();
          };
        }

        testScenario(
            columns, newCustomize, wrapInStruct, filterable, numCombinations);
      }
    }
  }

  void writeToMemory(
      const TypePtr& type,
      const std::vector<RowVectorPtr>& batches,
      bool forRowGroupSkip = false) override {
    auto options = createWriterOptions(type);
    int32_t flushCounter = 0;
    // If we test row group skip, we have all the data in one stripe. For
    // scan, we start  a stripe every 'flushEveryNBatches_' batches.
    options.flushPolicyFactory = [&]() {
      return std::make_unique<LambdaFlushPolicy>([&]() {
        return forRowGroupSkip ? false
                               : (++flushCounter % flushEveryNBatches_ == 0);
      });
    };

    auto sink = std::make_unique<MemorySink>(
        200 * 1024 * 1024,
        dwio::common::FileSink::Options{.pool = leafPool_.get()});
    ASSERT_TRUE(sink->isBuffered());
    sinkPtr_ = sink.get();
    options.memoryPool = rootPool_.get();
    writer_ = std::make_unique<dwrf::Writer>(std::move(sink), options);
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->close();
  }

  void setUpRowReaderOptions(
      dwio::common::RowReaderOptions& opts,
      const std::shared_ptr<ScanSpec>& spec) override {
    E2EFilterTestBase::setUpRowReaderOptions(opts, spec);
    if (!flatmapNodeIdsAsStruct_.empty()) {
      opts.setFlatmapNodeIdsAsStruct(flatmapNodeIdsAsStruct_);
    }
  }

  std::unique_ptr<dwio::common::Reader> makeReader(
      const dwio::common::ReaderOptions& opts,
      std::unique_ptr<dwio::common::BufferedInput> input) override {
    return std::make_unique<DwrfReader>(opts, std::move(input));
  }

  std::unordered_set<std::string> flatMapColumns_;

 private:
  dwrf::WriterOptions createWriterOptions(const TypePtr& type) {
    auto config = std::make_shared<dwrf::Config>();
    config->set(dwrf::Config::COMPRESSION, CompressionKind_NONE);
    config->set(dwrf::Config::USE_VINTS, useVInts_);
    auto writerSchema = type;
    if (!flatMapColumns_.empty()) {
      auto& rowType = type->asRow();
      auto columnTypes = rowType.children();
      std::vector<uint32_t> mapFlatCols;
      std::vector<std::vector<std::string>> mapFlatColsStructKeys;
      for (int i = 0; i < rowType.size(); ++i) {
        mapFlatColsStructKeys.emplace_back();
        if (flatMapColumns_.count(rowType.nameOf(i)) == 0 ||
            !columnTypes[i]->isRow()) {
          continue;
        }
        for (auto& name : columnTypes[i]->asRow().names()) {
          mapFlatColsStructKeys.back().push_back(name);
        }
        columnTypes[i] = MAP(VARCHAR(), columnTypes[i]->childAt(0));
      }
      writerSchema = ROW(
          std::vector<std::string>(rowType.names()), std::move(columnTypes));
      auto schemaWithId = TypeWithId::create(writerSchema);
      for (int i = 0; i < rowType.size(); ++i) {
        if (flatMapColumns_.count(rowType.nameOf(i)) == 0) {
          continue;
        }
        auto& child = schemaWithId->childAt(i);
        mapFlatCols.push_back(child->column());
        if (!rowType.childAt(i)->isRow()) {
          continue;
        }
        flatmapNodeIdsAsStruct_[child->id()] = mapFlatColsStructKeys[i];
      }
      config->set(dwrf::Config::FLATTEN_MAP, true);
      config->set(dwrf::Config::MAP_FLAT_DISABLE_DICT_ENCODING, false);
      config->set(dwrf::Config::MAP_FLAT_DISABLE_DICT_ENCODING_STRING, false);

      config->set<const std::vector<uint32_t>>(
          dwrf::Config::MAP_FLAT_COLS, mapFlatCols);
      config->set<const std::vector<std::vector<std::string>>>(
          dwrf::Config::MAP_FLAT_COLS_STRUCT_KEYS, mapFlatColsStructKeys);
    }
    dwrf::WriterOptions options;
    options.config = config;
    options.schema = writerSchema;
    return options;
  }

  std::unique_ptr<dwrf::Writer> writer_;
  std::unordered_map<uint32_t, std::vector<std::string>>
      flatmapNodeIdsAsStruct_;
};

TEST_F(E2EFilterTest, integerDirect) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "long_null:bigint",
      [&]() { makeAllNulls("long_null"); },
      true,
      {"short_val", "int_val", "long_val"},
      20,
      true,
      true);
}

TEST_F(E2EFilterTest, integerDictionary) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int64_t>(
            "long_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            10000000000, // rareMax
            true); // keepNulls

        makeIntDistribution<int32_t>(
            "int_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            100000000, // rareMax
            false); // keepNulls

        makeIntDistribution<int16_t>(
            "short_val",
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -999, // rareMin
            30000, // rareMax
            true); // keepNulls
      },
      true,
      {"short_val", "int_val", "long_val"},
      20,
      true,
      true);
}

TEST_F(E2EFilterTest, byteRle) {
  testWithTypes(
      "tiny_val:tinyint,"
      "bool_val:boolean,"
      "long_val:bigint,"
      "tiny_null:bigint",
      [&]() { makeAllNulls("tiny_null"); },
      true,
      {"tiny_val", "bool_val", "tiny_null"},
      20);
}

TEST_F(E2EFilterTest, floatAndDouble) {
  testWithTypes(
      "float_val:float,"
      "double_val:double,"
      "long_val:bigint,"
      "float_null:float",
      [&]() { makeAllNulls("float_null"); },
      true,
      {"float_val", "double_val", "float_null"},
      20,
      true,
      false);
}

TEST_F(E2EFilterTest, stringDirect) {
  testutil::TestValue::enable();
  bool coverage[2][2]{};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::dwrf::SelectiveStringDirectColumnReader::try8ConsecutiveSmall",
      std::function<void(bool*)>([&](bool* params) {
        coverage[0][params[0]] = true;
        coverage[1][params[1]] = true;
      }));
  flushEveryNBatches_ = 1;
  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringUnique("string_val");
        makeStringUnique("string_val_2");
      },
      true,
      {"string_val", "string_val_2"},
      20,
      true);
#ifndef NDEBUG
  ASSERT_TRUE(coverage[0][0]);
  ASSERT_TRUE(coverage[0][1]);
  ASSERT_TRUE(coverage[1][0]);
  ASSERT_TRUE(coverage[1][1]);
#endif
}

TEST_F(E2EFilterTest, stringDictionary) {
  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringDistribution("string_val", 100, true, false);
        makeStringDistribution("string_val_2", 170, false, true);
      },
      true,
      {"string_val", "string_val_2"},
      20,
      true,
      true);
}

TEST_F(E2EFilterTest, timestamp) {
  testWithTypes(
      "timestamp_val:timestamp,"
      "long_val:bigint",
      [&]() {},
      true,
      {"long_val", "timestamp_val"},
      20,
      true,
      true);
}

TEST_F(E2EFilterTest, listAndMap) {
  int numCombinations = 20;
#if !defined(NDEBUG) || defined(TSAN_BUILD)
  // The test is running slow under dev/debug and TSAN build; reduce the number
  // of combinations to avoid timeout.
  numCombinations = 2;
#endif
  testWithTypes(
      "long_val:bigint,"
      "long_val_2:bigint,"
      "int_val:int,"
      "array_val:array<struct<array_member: array<int>, float_val:float, long_val:bigint, string_val:string>>,"
      "map_val:map<bigint,struct<nested_map: map<int, int>>>",
      [&]() {},
      true,
      {"long_val", "long_val_2", "int_val", "array_val", "map_val"},
      numCombinations);
}

TEST_F(E2EFilterTest, nullCompactRanges) {
  // Makes a dataset with nulls at the beginning. Tries different
  // filter combinations on progressively larger batches. tests for a
  // bug in null compaction where null bits past end of nulls buffer
  // were compacted while there actually were no nulls.

  readSizes_ = {10, 100, 1000, 10000, 10000, 10000};
  testWithTypes(
      "tiny_val:tinyint,"
      "bool_val:boolean,"
      "long_val:bigint,"
      "tiny_null:bigint",

      [&]() { makeNotNull(500); },

      true,
      {"tiny_val", "bool_val", "long_val", "tiny_null"},
      20,
      false,
      false);
}

TEST_F(E2EFilterTest, lazyStruct) {
  testWithTypes(
      "long_val:bigint,"
      "outer_struct: struct<nested1:bigint, "
      "inner_struct: struct<nested2: bigint>>",
      [&]() {},
      true,
      {"long_val"},
      10,
      true,
      false);
}

TEST_F(E2EFilterTest, filterStruct) {
#ifdef TSAN_BUILD
  // The test is running slow under TSAN; reduce the number of combinations to
  // avoid timeout.
  constexpr int kNumCombinations = 10;
#else
  constexpr int kNumCombinations = 40;
#endif
  // The data has a struct member with one second level struct
  // column. Both structs have a column that gets filtered 'nestedxxx'
  // and one that does not 'dataxxx'.
  testWithTypes(
      "long_val:bigint,"
      "outer_struct: struct<nested1:bigint, "
      "  data1: string, "
      "  inner_struct: struct<nested2: bigint, data2: smallint>>",
      [&]() {},
      true,
      {"long_val",
       "outer_struct.inner_struct",
       "outer_struct.nested1",
       "outer_struct.inner_struct.nested2"},
      kNumCombinations,
      true,
      false);
}

TEST_F(E2EFilterTest, flatMapAsStruct) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "long_vals:struct<v1:bigint,v2:bigint,v3:bigint>,"
      "struct_vals:struct<nested1:struct<v1:bigint, v2:float>,nested2:struct<v1:bigint, v2:float>>";
  flatMapColumns_ = {"long_vals", "struct_vals"};
  testWithTypes(kColumns, [] {}, false, {"long_val"}, 10, true);
}

TEST_F(E2EFilterTest, flatMapScalar) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "long_vals:map<tinyint,bigint>,"
      "string_vals:map<string,string>";
  flatMapColumns_ = {"long_vals", "string_vals"};
  auto customize = [this] {
    dataSetBuilder_->makeUniformMapKeys(Subfield("string_vals"));
    dataSetBuilder_->makeMapStringValues(Subfield("string_vals"));
  };
  int numCombinations = 5;
#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(__address_sanitizer__)
  numCombinations = 1;
#endif
#endif
  testWithTypes(
      kColumns,
      customize,
      false,
      {"long_val", "long_vals"},
      numCombinations,
      true);
}

TEST_F(E2EFilterTest, flatMapComplex) {
  constexpr auto kColumns =
      "long_val:bigint,"
      "struct_vals:map<varchar,struct<v1:bigint, v2:float>>,"
      "array_vals:map<tinyint,array<int>>";
  flatMapColumns_ = {"struct_vals", "array_vals"};
  auto customize = [this] {
    dataSetBuilder_->makeUniformMapKeys(Subfield("struct_vals"));
  };
  int numCombinations = 5;
#if defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(__address_sanitizer__)
  numCombinations = 1;
#endif
#endif
#if !defined(NDEBUG)
  numCombinations = 1;
#endif
  testWithTypes(
      kColumns, customize, false, {"long_val"}, numCombinations, true);
}

TEST_F(E2EFilterTest, metadataFilter) {
  testMetadataFilter();
}

TEST_F(E2EFilterTest, subfieldsPruning) {
  testSubfieldsPruning();
}

TEST_F(E2EFilterTest, mutationCornerCases) {
  testMutationCornerCases();
}

// Define main so that gflags get processed.
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
