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
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/writer/Writer.h"

using namespace facebook::velox::common;
using namespace facebook::velox::dwrf;

template <typename T>
T getConfig(
    const std::shared_ptr<const Config>& config,
    const Config::Entry<T>& entry) {
  return config->get(entry);
}

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

TEST(ConfigTests, writerOptionsDefaultConfig) {
  WriterOptions options;
  const facebook::velox::config::ConfigBase base({});
  const facebook::velox::config::ConfigBase emptySession({});

  options.processConfigs(base, emptySession);
  auto config = options.config;
  ASSERT_EQ(getConfig(config, Config::STRIPE_SIZE), 64L * 1024L * 1024L);
  ASSERT_EQ(
      getConfig(config, Config::MAX_DICTIONARY_SIZE), 16L * 1024L * 1024L);
  ASSERT_TRUE(getConfig(config, Config::INTEGER_DICTIONARY_ENCODING_ENABLED));
  ASSERT_TRUE(getConfig(config, Config::STRING_DICTIONARY_ENCODING_ENABLED));
  ASSERT_EQ(getConfig(config, Config::COMPRESSION_BLOCK_SIZE_MIN), 1024);
  ASSERT_EQ(getConfig(config, Config::ZLIB_COMPRESSION_LEVEL), 4);
  ASSERT_EQ(getConfig(config, Config::ZSTD_COMPRESSION_LEVEL), 3);
  ASSERT_TRUE(getConfig(config, Config::LINEAR_STRIPE_SIZE_HEURISTICS));
}

TEST(ConfigTests, writerOptionsOverrideConfig) {
  std::unordered_map<std::string, std::string> configFromFile = {
      {Config::kOrcWriterMaxStripeSize, "100MB"},
      {Config::kOrcWriterMaxDictionaryMemory, "100MB"},
      {Config::kOrcWriterIntegerDictionaryEncodingEnabled, "false"},
      {Config::kOrcWriterStringDictionaryEncodingEnabled, "false"},
      {Config::kOrcWriterLinearStripeSizeHeuristics, "false"},
      {Config::kOrcWriterMinCompressionSize, "512"},
      {Config::kOrcWriterCompressionLevel, "1"}};
  const facebook::velox::config::ConfigBase base(std::move(configFromFile));
  const facebook::velox::config::ConfigBase emptySession({});

  WriterOptions options;
  options.processConfigs(base, emptySession);
  auto config = options.config;
  ASSERT_EQ(getConfig(config, Config::STRIPE_SIZE), 100L * 1024L * 1024L);
  ASSERT_EQ(
      getConfig(config, Config::MAX_DICTIONARY_SIZE), 100L * 1024L * 1024L);
  ASSERT_FALSE(getConfig(config, Config::INTEGER_DICTIONARY_ENCODING_ENABLED));
  ASSERT_FALSE(getConfig(config, Config::STRING_DICTIONARY_ENCODING_ENABLED));
  ASSERT_EQ(getConfig(config, Config::COMPRESSION_BLOCK_SIZE_MIN), 512);
  ASSERT_EQ(getConfig(config, Config::ZLIB_COMPRESSION_LEVEL), 1);
  ASSERT_EQ(getConfig(config, Config::ZSTD_COMPRESSION_LEVEL), 1);
  ASSERT_FALSE(getConfig(config, Config::LINEAR_STRIPE_SIZE_HEURISTICS));
}

TEST(ConfigTests, writerOptionsOverrideSession) {
  const facebook::velox::config::ConfigBase base({});
  std::unordered_map<std::string, std::string> sessionOverride = {
      {Config::kOrcWriterMaxStripeSizeSession, "22MB"},
      {Config::kOrcWriterMaxDictionaryMemorySession, "24MB"},
      {Config::kOrcWriterIntegerDictionaryEncodingEnabledSession, "false"},
      {Config::kOrcWriterStringDictionaryEncodingEnabledSession, "false"},
      {Config::kOrcWriterMinCompressionSizeSession, "512"},
      {Config::kOrcWriterCompressionLevelSession, "1"},
      {Config::kOrcWriterLinearStripeSizeHeuristicsSession, "false"}};
  const facebook::velox::config::ConfigBase session(std::move(sessionOverride));
  WriterOptions options;
  options.compressionKind = facebook::velox::common::CompressionKind_ZLIB;
  options.processConfigs(base, session);
  auto config = options.config;
  ASSERT_EQ(
      getConfig(config, Config::COMPRESSION),
      facebook::velox::common::CompressionKind_ZLIB);
  ASSERT_EQ(getConfig(config, Config::STRIPE_SIZE), 22L * 1024L * 1024L);
  ASSERT_EQ(
      getConfig(config, Config::MAX_DICTIONARY_SIZE), 24L * 1024L * 1024L);
  ASSERT_FALSE(getConfig(config, Config::INTEGER_DICTIONARY_ENCODING_ENABLED));
  ASSERT_FALSE(getConfig(config, Config::STRING_DICTIONARY_ENCODING_ENABLED));
  ASSERT_EQ(getConfig(config, Config::COMPRESSION_BLOCK_SIZE_MIN), 512);
  ASSERT_EQ(getConfig(config, Config::ZLIB_COMPRESSION_LEVEL), 1);
  ASSERT_EQ(getConfig(config, Config::ZSTD_COMPRESSION_LEVEL), 1);
  ASSERT_FALSE(getConfig(config, Config::LINEAR_STRIPE_SIZE_HEURISTICS));
}

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
