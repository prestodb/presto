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

#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"

namespace facebook::velox::config {

class ConfigTest : public ::testing::Test {};

enum TestEnum { ENUM_0 = 0, ENUM_1 = 1, ENUM_2 = 2, UNKNOWN = 3 };

class TestConfig : public ConfigBase {
 public:
  template <typename T>
  using Entry = ConfigBase::Entry<T>;

  static Entry<int32_t> kInt32Entry;
  static Entry<uint64_t> kUint64Entry;
  static Entry<bool> kBoolEntry;
  static Entry<std::string> kStringEntry;
  static Entry<TestEnum> kEnumEntry;

  TestConfig(
      std::unordered_map<std::string, std::string>&& configs,
      bool _mutable)
      : ConfigBase(std::move(configs), _mutable) {}
};

// Definition needs to be outside of class
TestConfig::Entry<int32_t> TestConfig::kInt32Entry("int32_entry", -32);
TestConfig::Entry<uint64_t> TestConfig::kUint64Entry("uint64_entry", 64);
TestConfig::Entry<bool> TestConfig::kBoolEntry("bool_entry", true);
TestConfig::Entry<std::string> TestConfig::kStringEntry(
    "string_entry",
    "default.string.value");
TestConfig::Entry<TestEnum> TestConfig::kEnumEntry(
    "enum_entry",
    TestEnum::ENUM_0,
    [](const TestEnum& value) {
      if (value == TestEnum::ENUM_0) {
        return "ENUM_0";
      }
      if (value == TestEnum::ENUM_1) {
        return "ENUM_1";
      }
      if (value == TestEnum::ENUM_2) {
        return "ENUM_2";
      }
      return "UNKNOWN";
    },
    [](const std::string& /* unused */, const std::string& v) {
      if (v == "ENUM_0") {
        return TestEnum::ENUM_0;
      }
      if (v == "ENUM_1") {
        return TestEnum::ENUM_1;
      }
      if (v == "ENUM_2") {
        return TestEnum::ENUM_2;
      }
      return TestEnum::UNKNOWN;
    });

TEST_F(ConfigTest, creation) {
  {
    std::unordered_map<std::string, std::string> rawConfigs{};
    auto config = std::make_shared<TestConfig>(std::move(rawConfigs), false);
    ASSERT_EQ(config->rawConfigs().size(), 0);
    ASSERT_EQ(config->rawConfigsCopy().size(), 0);
  }

  {
    std::unordered_map<std::string, std::string> rawConfigs{};
    rawConfigs.emplace("int32_entry", "-3200");
    auto config = std::make_shared<TestConfig>(std::move(rawConfigs), true);
    ASSERT_EQ(config->rawConfigsCopy().size(), 1);
    VELOX_ASSERT_THROW(
        config->rawConfigs(),
        "Mutable config cannot return unprotected reference to raw configs.");
  }
}

TEST_F(ConfigTest, immutableConfig) {
  // Testing default values
  auto config = std::make_shared<TestConfig>(
      std::unordered_map<std::string, std::string>(), false);
  ASSERT_EQ(config->get(TestConfig::kInt32Entry), -32);
  ASSERT_EQ(config->get(TestConfig::kUint64Entry), 64);
  ASSERT_EQ(config->get(TestConfig::kBoolEntry), true);
  ASSERT_EQ(config->get(TestConfig::kStringEntry), "default.string.value");
  ASSERT_EQ(config->get(TestConfig::kEnumEntry), TestEnum::ENUM_0);

  std::unordered_map<std::string, std::string> rawConfigs{
      {TestConfig::kInt32Entry.key, "-3200"},
      {TestConfig::kUint64Entry.key, "6400"},
      {TestConfig::kStringEntry.key, "not.default.string.value"},
      {TestConfig::kBoolEntry.key, "false"},
      {TestConfig::kEnumEntry.key, "ENUM_2"},
  };

  auto expectedRawConfigs = rawConfigs;

  config = std::make_shared<TestConfig>(std::move(rawConfigs), false);

  // Testing behavior when trying to modify the immutable config
  VELOX_ASSERT_THROW(config->set(TestConfig::kInt32Entry, 100), "Cannot set");
  VELOX_ASSERT_THROW(
      config->set(TestConfig::kInt32Entry.key, "100"), "Cannot set");
  VELOX_ASSERT_THROW(config->unset(TestConfig::kInt32Entry), "Cannot unset");
  VELOX_ASSERT_THROW(config->reset(), "Cannot reset");

  // Ensure values are unchanged after attempted modifications
  ASSERT_EQ(config->get(TestConfig::kInt32Entry), -3200);
  ASSERT_EQ(config->get(TestConfig::kUint64Entry), 6400);
  ASSERT_EQ(config->get(TestConfig::kBoolEntry), false);
  ASSERT_EQ(config->get(TestConfig::kStringEntry), "not.default.string.value");
  ASSERT_EQ(config->get(TestConfig::kEnumEntry), TestEnum::ENUM_2);
  ASSERT_EQ(
      config->get(
          TestConfig::kInt32Entry.key, TestConfig::kInt32Entry.defaultVal),
      -3200);
  ASSERT_EQ(
      config->get(
          TestConfig::kUint64Entry.key, TestConfig::kUint64Entry.defaultVal),
      6400);
  ASSERT_EQ(
      config->get(
          TestConfig::kBoolEntry.key, TestConfig::kBoolEntry.defaultVal),
      false);
  ASSERT_EQ(
      config->get(
          TestConfig::kStringEntry.key, TestConfig::kStringEntry.defaultVal),
      "not.default.string.value");
  ASSERT_EQ(
      config->get<TestEnum>(
          TestConfig::kEnumEntry.key,
          TestConfig::kEnumEntry.defaultVal,
          TestConfig::kEnumEntry.toT),
      TestEnum::ENUM_2);
  ASSERT_TRUE(config->get<int32_t>(TestConfig::kInt32Entry.key).has_value());
  ASSERT_EQ(config->get<int32_t>(TestConfig::kInt32Entry.key).value(), -3200);
  ASSERT_FALSE(config->get<int32_t>("wrong_int32_key").has_value());

  // Testing value existence
  ASSERT_TRUE(config->valueExists(TestConfig::kInt32Entry.key));
  ASSERT_FALSE(config->valueExists("non_existent_entry"));

  // Testing retrieval of raw configurations
  ASSERT_EQ(expectedRawConfigs, config->rawConfigs());
  ASSERT_EQ(expectedRawConfigs, config->rawConfigsCopy());
}

TEST_F(ConfigTest, mutableConfig) {
  // Create a mutable configuration with some initial values
  std::unordered_map<std::string, std::string> initialConfigs{
      {TestConfig::kInt32Entry.key, "-3200"},
      {TestConfig::kUint64Entry.key, "6400"},
      {TestConfig::kStringEntry.key, "initial.string.value"},
      {TestConfig::kBoolEntry.key, "false"},
      {TestConfig::kEnumEntry.key, "ENUM_2"},
  };

  auto config = std::make_shared<TestConfig>(std::move(initialConfigs), true);

  // Test setting new values
  (*config)
      .set(TestConfig::kInt32Entry, 123)
      .set(TestConfig::kStringEntry, std::string("modified.string.value"))
      .set(TestConfig::kBoolEntry.key, "true")
      .set(TestConfig::kEnumEntry.key, "ENUM_1");

  ASSERT_EQ(config->get(TestConfig::kInt32Entry), 123);
  ASSERT_EQ(config->get(TestConfig::kStringEntry), "modified.string.value");
  ASSERT_EQ(config->get(TestConfig::kBoolEntry), true);
  ASSERT_EQ(config->get(TestConfig::kEnumEntry), TestEnum::ENUM_1);

  // Test unsetting values
  ASSERT_EQ(config->get(TestConfig::kUint64Entry), 6400);
  config->unset(TestConfig::kUint64Entry);
  ASSERT_EQ(
      config->get(TestConfig::kUint64Entry),
      TestConfig::kUint64Entry.defaultVal);

  // Test resetting the configuration
  config->reset();
  auto rawConfigsCopy = config->rawConfigsCopy();
  ASSERT_TRUE(rawConfigsCopy.empty());
  ASSERT_FALSE(config->valueExists(TestConfig::kUint64Entry.key));
}

TEST_F(ConfigTest, capacityConversion) {
  folly::Random::DefaultGenerator rng;
  rng.seed(1);

  std::unordered_map<CapacityUnit, std::vector<std::string>> unitStrLookup{
      {CapacityUnit::BYTE, {"b", "B"}},
      {CapacityUnit::KILOBYTE, {"kb", "kB", "Kb", "KB"}},
      {CapacityUnit::MEGABYTE, {"mb", "mB", "Mb", "MB"}},
      {CapacityUnit::GIGABYTE, {"gb", "gB", "Gb", "GB"}},
      {CapacityUnit::TERABYTE, {"tb", "tB", "Tb", "TB"}},
      {CapacityUnit::PETABYTE, {"pb", "pB", "Pb", "PB"}}};

  std::vector<std::pair<CapacityUnit, double>> units{
      {CapacityUnit::BYTE, 1},
      {CapacityUnit::KILOBYTE, 1024},
      {CapacityUnit::MEGABYTE, 1024 * 1024},
      {CapacityUnit::GIGABYTE, 1024 * 1024 * 1024},
      {CapacityUnit::TERABYTE, 1024ll * 1024 * 1024 * 1024},
      {CapacityUnit::PETABYTE, 1024ll * 1024 * 1024 * 1024 * 1024}};
  for (int32_t i = 0; i < units.size(); i++) {
    for (int32_t j = 0; j < units.size(); j++) {
      // We use this diffRatio to prevent float conversion overflow when
      // converting from one unit to another.
      uint64_t diffRatio = i < j ? units[j].second / units[i].second
                                 : units[i].second / units[j].second;
      uint64_t randNumber = folly::Random::rand64(rng);
      uint64_t testNumber = i > j ? randNumber / diffRatio : randNumber;
      const auto& unitStrs = unitStrLookup[units[i].first];
      for (int32_t k = 0; k < unitStrs.size(); k++) {
        ASSERT_EQ(
            toCapacity(
                std::string(std::to_string(testNumber) + unitStrs[k]),
                units[j].first),
            (uint64_t)(testNumber * (units[i].second / units[j].second)));
      }
    }
  }
}

TEST_F(ConfigTest, durationConversion) {
  folly::Random::DefaultGenerator rng;
  rng.seed(1);

  std::vector<std::pair<std::string, uint64_t>> units{
      {"ns", 1},
      {"us", 1000},
      {"ms", 1000 * 1000},
      {"s", 1000ll * 1000 * 1000},
      {"m", 1000ll * 1000 * 1000 * 60},
      {"h", 1000ll * 1000 * 1000 * 60 * 60},
      {"d", 1000ll * 1000 * 1000 * 60 * 60 * 24}};
  for (uint32_t i = 0; i < units.size(); i++) {
    auto testNumber = folly::Random::rand32(rng) % 10000;
    auto duration =
        toDuration(std::string(std::to_string(testNumber) + units[i].first));
    ASSERT_EQ(
        testNumber * units[i].second,
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
  }
}
} // namespace facebook::velox::config
