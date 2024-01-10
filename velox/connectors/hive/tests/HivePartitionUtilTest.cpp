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

#include "velox/connectors/hive/HivePartitionUtil.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/catalog/fbhive/FileUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "gtest/gtest.h"

using namespace facebook::velox::connector::hive;
using namespace facebook::velox;
using namespace facebook::velox::dwio::catalog::fbhive;

class HivePartitionUtilTest : public ::testing::Test,
                              public test::VectorTestBase {
 protected:
  template <typename T>
  VectorPtr makeDictionary(const std::vector<T>& data) {
    auto base = makeFlatVector(data);
    auto indices =
        makeIndices(data.size() * 10, [](auto row) { return row / 10; });
    return wrapInDictionary(indices, data.size(), base);
  };

  RowVectorPtr makePartitionsVector(
      RowVectorPtr input,
      const std::vector<column_index_t>& partitionChannels) {
    std::vector<VectorPtr> partitions;
    std::vector<std::string> partitonKeyNames;
    std::vector<TypePtr> partitionKeyTypes;

    RowTypePtr inputType = asRowType(input->type());
    for (column_index_t channel : partitionChannels) {
      partitions.push_back(input->childAt(channel));
      partitonKeyNames.push_back(inputType->nameOf(channel));
      partitionKeyTypes.push_back(inputType->childAt(channel));
    }

    return std::make_shared<RowVector>(
        pool(),
        ROW(std::move(partitonKeyNames), std::move(partitionKeyTypes)),
        nullptr,
        input->size(),
        partitions);
  }
};

TEST_F(HivePartitionUtilTest, partitionName) {
  {
    RowVectorPtr input = makeRowVector(
        {"flat_bool_col",
         "flat_tinyint_col",
         "flat_smallint_col",
         "flat_int_col",
         "flat_bigint_col",
         "dict_string_col",
         "const_date_col"},
        {makeFlatVector<bool>(std::vector<bool>{false}),
         makeFlatVector<int8_t>(std::vector<int8_t>{10}),
         makeFlatVector<int16_t>(std::vector<int16_t>{100}),
         makeFlatVector<int32_t>(std::vector<int32_t>{1000}),
         makeFlatVector<int64_t>(std::vector<int64_t>{10000}),
         makeDictionary<StringView>(std::vector<StringView>{"str1000"}),
         makeConstant<int32_t>(10000, 1, DATE())});

    std::vector<std::string> expectedPartitionKeyValues{
        "flat_bool_col=false",
        "flat_tinyint_col=10",
        "flat_smallint_col=100",
        "flat_int_col=1000",
        "flat_bigint_col=10000",
        "dict_string_col=str1000",
        "const_date_col=1997-05-19"};

    std::vector<column_index_t> partitionChannels;
    for (auto i = 1; i <= expectedPartitionKeyValues.size(); i++) {
      partitionChannels.resize(i);
      std::iota(partitionChannels.begin(), partitionChannels.end(), 0);

      EXPECT_EQ(
          FileUtils::makePartName(
              extractPartitionKeyValues(
                  makePartitionsVector(input, partitionChannels), 0),
              true),
          folly::join(
              "/",
              std::vector<std::string>(
                  expectedPartitionKeyValues.data(),
                  expectedPartitionKeyValues.data() + i)));
    }
  }

  // Test unsupported partition type.
  {
    RowVectorPtr input = makeRowVector(
        {"map_col"},
        {makeMapVector<int32_t, StringView>(
            {{{1, "str1000"}, {2, "str2000"}}})});

    std::vector<column_index_t> partitionChannels{0};

    VELOX_ASSERT_THROW(
        FileUtils::makePartName(
            extractPartitionKeyValues(
                makePartitionsVector(input, partitionChannels), 0),
            true),
        "Unsupported partition type: MAP");
  }
}

TEST_F(HivePartitionUtilTest, partitionNameForNull) {
  std::vector<std::string> partitionColumnNames{
      "flat_bool_col",
      "flat_tinyint_col",
      "flat_smallint_col",
      "flat_int_col",
      "flat_bigint_col",
      "flat_string_col",
      "const_date_col"};

  RowVectorPtr input = makeRowVector(
      partitionColumnNames,
      {makeNullableFlatVector<bool>({std::nullopt}),
       makeNullableFlatVector<int8_t>({std::nullopt}),
       makeNullableFlatVector<int16_t>({std::nullopt}),
       makeNullableFlatVector<int32_t>({std::nullopt}),
       makeNullableFlatVector<int64_t>({std::nullopt}),
       makeNullableFlatVector<StringView>({std::nullopt}),
       makeConstant<int32_t>(std::nullopt, 1, DATE())});

  for (auto i = 0; i < partitionColumnNames.size(); i++) {
    std::vector<column_index_t> partitionChannels = {(column_index_t)i};
    auto partitionEntries = extractPartitionKeyValues(
        makePartitionsVector(input, partitionChannels), 0);
    EXPECT_EQ(1, partitionEntries.size());
    EXPECT_EQ(partitionColumnNames[i], partitionEntries[0].first);
    EXPECT_EQ("", partitionEntries[0].second);
  }
}
