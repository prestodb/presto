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

#include "folly/Random.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/Common.h"
#include "velox/dwio/dwrf/common/Config.h"

using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwio::common;

TEST(ConfigTests, Set) {
  Config config;
  // set
  auto val = folly::Random::rand32();
  config.set(Config::ROW_INDEX_STRIDE, val);
  EXPECT_EQ(config.get(Config::ROW_INDEX_STRIDE), val);
  // unset
  config.unset(Config::ROW_INDEX_STRIDE);
  EXPECT_EQ(config.get(Config::ROW_INDEX_STRIDE), 10000);
  // reset
  config.set(Config::ROW_INDEX_STRIDE, val);
  config.set(Config::CREATE_INDEX, false);
  config.reset();
  EXPECT_EQ(config.get(Config::ROW_INDEX_STRIDE), 10000);
  EXPECT_TRUE(config.get(Config::CREATE_INDEX));
}

TEST(ConfigTests, EnumConfig) {
  Config config;
  config.set(Config::COMPRESSION, CompressionKind::CompressionKind_ZLIB);
  EXPECT_EQ(
      config.get(Config::COMPRESSION), CompressionKind::CompressionKind_ZLIB);
  config.set(Config::COMPRESSION, CompressionKind::CompressionKind_NONE);
  EXPECT_EQ(
      config.get(Config::COMPRESSION), CompressionKind::CompressionKind_NONE);
}

TEST(ConfigTests, UInt32Config) {
  Config config;
  auto val = folly::Random::rand32();
  config.set(Config::ROW_INDEX_STRIDE, val);
  EXPECT_EQ(config.get(Config::ROW_INDEX_STRIDE), val);
}

TEST(ConfigTests, BoolConfig) {
  Config config;
  config.set(Config::CREATE_INDEX, false);
  EXPECT_FALSE(config.get(Config::CREATE_INDEX));
  config.set(Config::CREATE_INDEX, true);
  EXPECT_TRUE(config.get(Config::CREATE_INDEX));
}

struct ConfigTestParams {
  std::string inputCols{""}; // input spec
  std::vector<uint32_t> expectedCols{}; // do we expect the spec to be valid
};

class ConfigTests : public ::testing::TestWithParam<ConfigTestParams> {};
TEST_P(ConfigTests, FlatMapCols) {
  const auto& params = GetParam();
  std::map<std::string, std::string> inputConfigMap{
      {"orc.map.flat.cols", params.inputCols}};
  auto config = Config::fromMap(inputConfigMap);
  EXPECT_EQ(config->get(Config::MAP_FLAT_COLS), params.expectedCols);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    ConfigTestsSuite,
    ConfigTests,
    ::testing::Values(
        ConfigTestParams{"12,13,14", {12, 13, 14}},
        ConfigTestParams{"12,13,14,", {12, 13, 14}},
        ConfigTestParams{"12, 13, 14", {12, 13, 14}},
        ConfigTestParams{"", {}},
        ConfigTestParams{" ", {}}));
