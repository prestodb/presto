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
#include "velox/dwio/catalog/fbhive/FileUtils.h"
#include "velox/dwio/common/exception/Exception.h"

using namespace ::testing;
using namespace facebook::velox::dwio::catalog::fbhive;
using namespace facebook::velox::dwio::common::exception;

TEST(FileUtilsTests, MakePartName) {
  std::vector<std::pair<std::string, std::string>> pairs{
      {"ds", "2016-01-01"}, {"FOO", ""}, {"a\nb:c", "a#b=c"}};
  ASSERT_EQ(
      FileUtils::makePartName(pairs, true),
      "ds=2016-01-01/foo=__HIVE_DEFAULT_PARTITION__/a%0Ab%3Ac=a%23b%3Dc");
  ASSERT_EQ(
      FileUtils::makePartName(pairs, false),
      "ds=2016-01-01/FOO=__HIVE_DEFAULT_PARTITION__/a%0Ab%3Ac=a%23b%3Dc");
}

TEST(FileUtilsTests, ParsePartKeyValues) {
  ASSERT_THROW(FileUtils::parsePartKeyValues("ds"), LoggedException);

  ASSERT_THAT(
      FileUtils::parsePartKeyValues(
          "ds=2016-01-01/foo=__HIVE_DEFAULT_PARTITION__/a%0Ab%3Ac=a%23b%3Dc"),
      ElementsAre(
          std::make_pair("ds", "2016-01-01"),
          std::make_pair("foo", "__HIVE_DEFAULT_PARTITION__"),
          std::make_pair("a\nb:c", "a#b=c")));
}

TEST(FileUtilsTests, ExtractPartitionName) {
  struct TestCase {
   public:
    TestCase(const std::string& filePath, const std::string& partitionName)
        : filePath{filePath}, partitionName{partitionName} {}

    std::string filePath;
    std::string partitionName;
  };

  std::vector<TestCase> testCases{
      {"", ""},
      // identity
      {"ds=2016-01-01/foo=__HIVE_DEFAULT_PARTITION__/a%0Ab%3Ac=a%23b%3Ac",
       "ds=2016-01-01/foo=__HIVE_DEFAULT_PARTITION__/a%0Ab%3Ac=a%23b%3Ac"},
      // escaped partition name is not a special case.
      {"ds=2016-01-01/foo=__HIVE_DEFAULT_PARTITION__/a%0Ab%3Ac%3Da%23b%3Ac",
       "ds=2016-01-01/foo=__HIVE_DEFAULT_PARTITION__"},
      // full path
      {"ws://nobodycares/notimportant/somethingsomething/yaddayadda/ds=2016-01-01/foo=__HIVE_DEFAULT_PARTITION__/a%0Ab%3Ac=a%23b%3Ac/part0",
       "ds=2016-01-01/foo=__HIVE_DEFAULT_PARTITION__/a%0Ab%3Ac=a%23b%3Ac"}};

  for (auto testCase : testCases) {
    EXPECT_EQ(
        testCase.partitionName,
        FileUtils::extractPartitionName(testCase.filePath));
  }
}
