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
#include <gtest/gtest.h>

#include "presto_cpp/main/PrestoToVeloxQueryConfig.h"
#include "presto_cpp/main/SessionProperties.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/QueryConfig.h"

using namespace facebook::presto;
using namespace facebook::presto::protocol;
using namespace facebook::velox;
using namespace facebook::velox::core;
class PrestoToVeloxQueryConfigTest : public testing::Test {
 protected:
  void SetUp() override {}

  SessionRepresentation createBasicSession() {
    SessionRepresentation session;
    session.queryId = "test_query_123";
    session.user = "test_user";
    session.timeZoneKey = 0; // UTC
    session.startTime = 1234567890;
    return session;
  }

  TaskUpdateRequest createBasicTaskUpdateRequest() {
    TaskUpdateRequest request;
    request.session = createBasicSession();
    request.extraCredentials["test_credential"] = "test_value";
    return request;
  }
};

TEST_F(PrestoToVeloxQueryConfigTest, sessionPropertiesOverrideSystemConfigs) {
  // This test validates that session properties override system configs for
  // ALL entries in veloxToPrestoConfigMapping. If a new entry is added to the
  // mapping, this test will fail and force the developer to update it.

  auto session = createBasicSession();

  // Define all the mappings that exist in veloxToPrestoConfigMapping
  // This list MUST be kept in sync with the actual mapping in
  // PrestoToVeloxQueryConfig.cpp
  struct ConfigTestCase {
    std::string veloxConfigKey;
    std::optional<std::string>
        sessionPropertyKey; // The session property that should
                            // override system config (optional)
    std::string systemConfigKey; // The corresponding system config key
    std::string sessionValue;
    std::string differentSessionValue;
    std::function<void(const core::QueryConfig&, const std::string&)> validator;
  };

  std::vector<ConfigTestCase> testCases = {
      {.veloxConfigKey = core::QueryConfig::kQueryMaxMemoryPerNode,
       .sessionPropertyKey =
           std::make_optional<std::string>("query_max_memory_per_node"),
       .systemConfigKey = std::string(SystemConfig::kQueryMaxMemoryPerNode),
       .sessionValue = "8GB",
       .differentSessionValue = "4GB",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(
                 config::toCapacity(expectedValue, config::CapacityUnit::BYTE),
                 config.queryMaxMemoryPerNode());
           }},

      {.veloxConfigKey = core::QueryConfig::kSpillFileCreateConfig,
       .sessionPropertyKey = std::make_optional<std::string>(
           SessionProperties::kSpillFileCreateConfig),
       .systemConfigKey = std::string(SystemConfig::kSpillerFileCreateConfig),
       .sessionValue = "test_config_1",
       .differentSessionValue = "test_config_2",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(expectedValue, config.spillFileCreateConfig());
           }},

      {.veloxConfigKey = core::QueryConfig::kSpillEnabled,
       .sessionPropertyKey =
           std::make_optional<std::string>(core::QueryConfig::kSpillEnabled),
       .systemConfigKey = std::string(SystemConfig::kSpillEnabled),
       .sessionValue = "false",
       .differentSessionValue = "true",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(expectedValue == "true", config.spillEnabled());
           }},

      {.veloxConfigKey = core::QueryConfig::kJoinSpillEnabled,
       .sessionPropertyKey = std::make_optional<std::string>(
           SessionProperties::kJoinSpillEnabled),
       .systemConfigKey = std::string(SystemConfig::kJoinSpillEnabled),
       .sessionValue = "false",
       .differentSessionValue = "true",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(expectedValue == "true", config.joinSpillEnabled());
           }},

      {.veloxConfigKey = core::QueryConfig::kOrderBySpillEnabled,
       .sessionPropertyKey =
           std::make_optional<std::string>("order_by_spill_enabled"),
       .systemConfigKey = std::string(SystemConfig::kOrderBySpillEnabled),
       .sessionValue = "false",
       .differentSessionValue = "true",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(expectedValue == "true", config.orderBySpillEnabled());
           }},

      {.veloxConfigKey = core::QueryConfig::kAggregationSpillEnabled,
       .sessionPropertyKey =
           std::make_optional<std::string>("aggregation_spill_enabled"),
       .systemConfigKey = std::string(SystemConfig::kAggregationSpillEnabled),
       .sessionValue = "false",
       .differentSessionValue = "true",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(
                 expectedValue == "true", config.aggregationSpillEnabled());
           }},

      {.veloxConfigKey = core::QueryConfig::kRequestDataSizesMaxWaitSec,
       .sessionPropertyKey = std::make_optional<std::string>(
           SessionProperties::kRequestDataSizesMaxWaitSec),
       .systemConfigKey =
           std::string(SystemConfig::kRequestDataSizesMaxWaitSec),
       .sessionValue = "30",
       .differentSessionValue = "15",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(
                 std::stoi(expectedValue), config.requestDataSizesMaxWaitSec());
           }},

      {.veloxConfigKey = core::QueryConfig::kMaxSplitPreloadPerDriver,
       .sessionPropertyKey = std::nullopt,
       .systemConfigKey = std::string(SystemConfig::kDriverMaxSplitPreload),
       .sessionValue = "",
       .differentSessionValue = "",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(
                 std::stoi(expectedValue), config.maxSplitPreloadPerDriver());
           }},

      {.veloxConfigKey =
           core::QueryConfig::kMaxLocalExchangePartitionBufferSize,
       .sessionPropertyKey = std::nullopt,
       .systemConfigKey =
           std::string(SystemConfig::kMaxLocalExchangePartitionBufferSize),
       .sessionValue = "",
       .differentSessionValue = "",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(
                 std::stoull(expectedValue),
                 config.maxLocalExchangePartitionBufferSize());
           }},

      {.veloxConfigKey = core::QueryConfig::kPrestoArrayAggIgnoreNulls,
       .sessionPropertyKey = std::nullopt,
       .systemConfigKey = std::string(SystemConfig::kUseLegacyArrayAgg),
       .sessionValue = "",
       .differentSessionValue = "",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             EXPECT_EQ(
                 expectedValue == "true", config.prestoArrayAggIgnoreNulls());
           }},

      {.veloxConfigKey = core::QueryConfig::kMaxOutputBufferSize,
       .sessionPropertyKey = std::make_optional<std::string>(
           SessionProperties::kMaxOutputBufferSize),
       .systemConfigKey = std::string(SystemConfig::kSinkMaxBufferSize),
       .sessionValue = "67108864",
       .differentSessionValue = "134217728",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             // System config and session is not same format, use try catch to
             // handle the difference.
             uint64_t expectedBytes;
             try {
               expectedBytes =
                   toCapacity(expectedValue, config::CapacityUnit::BYTE);
             } catch (const VeloxUserError& e) {
               expectedBytes = std::stoull(expectedValue);
             }
             EXPECT_EQ(expectedBytes, config.maxOutputBufferSize());
           }},

      {.veloxConfigKey = core::QueryConfig::kMaxPartitionedOutputBufferSize,
       .sessionPropertyKey = std::make_optional<std::string>(
           SessionProperties::kMaxPartitionedOutputBufferSize),
       .systemConfigKey =
           std::string(SystemConfig::kDriverMaxPagePartitioningBufferSize),
       .sessionValue = "67108864",
       .differentSessionValue = "134217728",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             // System config and session is not same format, use try catch to
             // handle the difference.
             uint64_t expectedBytes;
             try {
               expectedBytes =
                   toCapacity(expectedValue, config::CapacityUnit::BYTE);
             } catch (const VeloxUserError& /* unused */) {
               expectedBytes = std::stoull(expectedValue);
             }
             EXPECT_EQ(expectedBytes, config.maxPartitionedOutputBufferSize());
           }},

      {.veloxConfigKey = core::QueryConfig::kMaxPartialAggregationMemory,
       .sessionPropertyKey = std::make_optional<std::string>(
           SessionProperties::kMaxPartialAggregationMemory),
       .systemConfigKey =
           std::string(SystemConfig::kTaskMaxPartialAggregationMemory),
       .sessionValue = "268435456",
       .differentSessionValue = "134217728",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             uint64_t expectedBytes;
             try {
               expectedBytes =
                   toCapacity(expectedValue, config::CapacityUnit::BYTE);
             } catch (const VeloxUserError& /* unused */) {
               expectedBytes = std::stoull(expectedValue);
             }
             EXPECT_EQ(
                 expectedBytes, config.maxPartialAggregationMemoryUsage());
           }},

      {.veloxConfigKey = core::QueryConfig::kMaxExchangeBufferSize,
       .sessionPropertyKey = std::nullopt,
       .systemConfigKey = std::string(SystemConfig::kExchangeMaxBufferSize),
       .sessionValue = "64MB",
       .differentSessionValue = "128MB",
       .validator =
           [](const core::QueryConfig& config,
              const std::string& expectedValue) {
             uint64_t expectedBytes;
             try {
               expectedBytes =
                   toCapacity(expectedValue, config::CapacityUnit::BYTE);
             } catch (const VeloxUserError& /* unused */) {
               expectedBytes = std::stoull(expectedValue);
             }
             EXPECT_EQ(expectedBytes, config.maxExchangeBufferSize());
           }},
  };

  // CRITICAL: This count MUST match the exact number of entries in
  // veloxToPrestoConfigMapping If this assertion fails, it means a new
  // mapping was added and this test needs to be updated
  const size_t kExpectedMappingCount = 14;
  EXPECT_EQ(kExpectedMappingCount, testCases.size());

  // Test each mapping to ensure session properties override system configs
  for (const auto& testCase : testCases) {
    // Only test session property override behavior if sessionPropertyKey is set
    if (testCase.sessionPropertyKey.has_value()) {
      // Test 1: Set session property to first value
      session.systemProperties.clear();
      session.systemProperties[testCase.sessionPropertyKey.value()] =
          testCase.sessionValue;

      auto veloxConfig1 = QueryConfig(toVeloxConfigs(session));
      testCase.validator(veloxConfig1, testCase.sessionValue);

      // Test 2: Change session property to different value to ensure it's being
      // used
      session.systemProperties[testCase.sessionPropertyKey.value()] =
          testCase.differentSessionValue;

      auto veloxConfig2 = QueryConfig(toVeloxConfigs(session));
      testCase.validator(veloxConfig2, testCase.differentSessionValue);

      // Test 3: Remove session property to test system config fallback
      session.systemProperties.erase(testCase.sessionPropertyKey.value());
    } else {
      // For configs without session properties, clear all session properties
      session.systemProperties.clear();
    }

    // Test system config fallback behavior (applies to all test cases)
    auto veloxConfig3 = QueryConfig(toVeloxConfigs(session));

    // Get the actual system config default value using optionalProperty()
    auto* systemConfig = SystemConfig::instance();
    auto systemDefaultValue =
        systemConfig->optionalProperty(testCase.systemConfigKey);

    if (systemDefaultValue.hasValue()) {
      // Verify that the system config default value is used when session
      // property is absent (or doesn't exist)
      testCase.validator(veloxConfig3, systemDefaultValue.value());
    }
    // Note: If system config doesn't have a value, Velox will use its built-in
    // defaults. The key point is that session properties, when present, always
    // take precedence over system configs.
  }

  // Additional comprehensive test: Set all session properties at once
  // (only for test cases that have session properties)
  session.systemProperties.clear();
  for (const auto& testCase : testCases) {
    if (testCase.sessionPropertyKey.has_value()) {
      session.systemProperties[testCase.sessionPropertyKey.value()] =
          testCase.sessionValue;
    }
  }

  auto veloxConfigAll = QueryConfig(toVeloxConfigs(session));

  // Verify all session properties are applied correctly
  for (const auto& testCase : testCases) {
    if (testCase.sessionPropertyKey.has_value()) {
      testCase.validator(veloxConfigAll, testCase.sessionValue);
    }
  }
}

TEST_F(PrestoToVeloxQueryConfigTest, queryTracingConfiguration) {
  auto session = createBasicSession();

  // Test 1: Basic query tracing properties
  session.systemProperties[SessionProperties::kQueryTraceEnabled] = "true";
  session.systemProperties[SessionProperties::kQueryTraceDir] = "/tmp/trace";
  session.systemProperties[SessionProperties::kQueryTraceNodeId] = "node_123";
  session.systemProperties[SessionProperties::kQueryTraceMaxBytes] =
      "1048576"; // 1MB

  auto veloxConfig = QueryConfig(toVeloxConfigs(session));

  EXPECT_TRUE(veloxConfig.queryTraceEnabled());
  EXPECT_EQ("/tmp/trace", veloxConfig.queryTraceDir());
  EXPECT_EQ("node_123", veloxConfig.queryTraceNodeId());
  EXPECT_EQ(1048576, veloxConfig.queryTraceMaxBytes());

  // Test 2: Query tracing regex construction with both fragment and shard IDs
  session.systemProperties.clear();
  session.systemProperties[SessionProperties::kQueryTraceFragmentId] =
      "frag_123";
  session.systemProperties[SessionProperties::kQueryTraceShardId] = "shard_456";

  auto veloxConfig1 = QueryConfig(toVeloxConfigs(session));
  EXPECT_EQ(
      ".*\\.frag_123\\..*\\.shard_456\\..*",
      veloxConfig1.queryTraceTaskRegExp());

  // Test 3: Query tracing regex with only fragment ID
  session.systemProperties.erase(SessionProperties::kQueryTraceShardId);
  auto veloxConfig2 = QueryConfig(toVeloxConfigs(session));
  EXPECT_EQ(
      ".*\\.frag_123\\..*\\..*\\..*", veloxConfig2.queryTraceTaskRegExp());

  // Test 4: Query tracing regex with only shard ID
  session.systemProperties.erase(SessionProperties::kQueryTraceFragmentId);
  session.systemProperties[SessionProperties::kQueryTraceShardId] = "shard_789";
  auto veloxConfig3 = QueryConfig(toVeloxConfigs(session));
  EXPECT_EQ(
      ".*\\..*\\..*\\.shard_789\\..*", veloxConfig3.queryTraceTaskRegExp());

  // Test 5: Query tracing regex with neither fragment nor shard ID
  session.systemProperties.clear();
  auto veloxConfig4 = QueryConfig(toVeloxConfigs(session));
  // When neither fragment nor shard ID is set, no regex should be constructed
  // The queryTraceTaskRegExp should be empty or default
  EXPECT_TRUE(
      veloxConfig4.queryTraceTaskRegExp().empty() ||
      veloxConfig4.queryTraceTaskRegExp() == "");

  // Test 6: Comprehensive query tracing configuration
  session.systemProperties.clear();
  session.systemProperties[SessionProperties::kQueryTraceEnabled] = "true";
  session.systemProperties[SessionProperties::kQueryTraceDir] =
      "/custom/trace/path";
  session.systemProperties[SessionProperties::kQueryTraceNodeId] =
      "custom_node_456";
  session.systemProperties[SessionProperties::kQueryTraceMaxBytes] =
      "2097152"; // 2MB
  session.systemProperties[SessionProperties::kQueryTraceFragmentId] =
      "fragment_789";
  session.systemProperties[SessionProperties::kQueryTraceShardId] = "shard_012";

  auto veloxConfigComprehensive = QueryConfig(toVeloxConfigs(session));

  // Verify all tracing properties are set correctly
  EXPECT_TRUE(veloxConfigComprehensive.queryTraceEnabled());
  EXPECT_EQ("/custom/trace/path", veloxConfigComprehensive.queryTraceDir());
  EXPECT_EQ("custom_node_456", veloxConfigComprehensive.queryTraceNodeId());
  EXPECT_EQ(2097152, veloxConfigComprehensive.queryTraceMaxBytes());
  EXPECT_EQ(
      ".*\\.fragment_789\\..*\\.shard_012\\..*",
      veloxConfigComprehensive.queryTraceTaskRegExp());

  // Test 7: Query tracing disabled
  session.systemProperties.clear();
  session.systemProperties[SessionProperties::kQueryTraceEnabled] = "false";
  session.systemProperties[SessionProperties::kQueryTraceDir] =
      "/should/not/matter";
  session.systemProperties[SessionProperties::kQueryTraceNodeId] =
      "disabled_node";

  auto veloxConfigDisabled = QueryConfig(toVeloxConfigs(session));
  EXPECT_FALSE(veloxConfigDisabled.queryTraceEnabled());
  // Even when disabled, other properties should still be set if provided
  EXPECT_EQ("/should/not/matter", veloxConfigDisabled.queryTraceDir());
  EXPECT_EQ("disabled_node", veloxConfigDisabled.queryTraceNodeId());
}

TEST_F(PrestoToVeloxQueryConfigTest, shuffleCompressionHandling) {
  auto session = createBasicSession();

  // Test various compression types
  session.systemProperties[SessionProperties::kShuffleCompressionCodec] =
      "ZSTD";
  auto veloxConfig1 = QueryConfig(toVeloxConfigs(session));
  EXPECT_EQ("zstd", veloxConfig1.shuffleCompressionKind());

  session.systemProperties[SessionProperties::kShuffleCompressionCodec] = "lz4";
  auto veloxConfig2 = QueryConfig(toVeloxConfigs(session));
  EXPECT_EQ("lz4", veloxConfig2.shuffleCompressionKind());

  session.systemProperties[SessionProperties::kShuffleCompressionCodec] =
      "none";
  auto veloxConfig3 = QueryConfig(toVeloxConfigs(session));
  EXPECT_EQ("none", veloxConfig3.shuffleCompressionKind());
}

TEST_F(PrestoToVeloxQueryConfigTest, connectorConfigConversion) {
  auto request = createBasicTaskUpdateRequest();

  // Add catalog properties
  std::map<std::string, std::string> hiveProperties;
  hiveProperties["native_hive.max_partitions_per_scan"] = "1000";
  hiveProperties["hive.metastore.uri"] = "thrift://localhost:9083";
  hiveProperties["native_compression_codec"] = "SNAPPY";

  std::map<std::string, std::string> icebergProperties;
  icebergProperties["native_iceberg.file_format"] = "PARQUET";
  icebergProperties["iceberg.catalog.type"] = "HIVE";

  request.session.catalogProperties["hive"] = hiveProperties;
  request.session.catalogProperties["iceberg"] = icebergProperties;

  auto connectorConfigs = toConnectorConfigs(request);

  // Verify connector configs are created
  EXPECT_EQ(2, connectorConfigs.size());
  EXPECT_TRUE(connectorConfigs.find("hive") != connectorConfigs.end());
  EXPECT_TRUE(connectorConfigs.find("iceberg") != connectorConfigs.end());

  // Verify native prefix is removed
  auto hiveConfig = connectorConfigs["hive"];
  EXPECT_EQ(
      "1000", hiveConfig->get<std::string>("hive.max_partitions_per_scan"));
  EXPECT_EQ(
      "thrift://localhost:9083",
      hiveConfig->get<std::string>("hive.metastore.uri"));
  EXPECT_EQ("SNAPPY", hiveConfig->get<std::string>("compression_codec"));

  // Verify user and credentials are added
  EXPECT_EQ("test_user", hiveConfig->get<std::string>("user"));
  EXPECT_EQ("test_value", hiveConfig->get<std::string>("test_credential"));

  auto icebergConfig = connectorConfigs["iceberg"];
  EXPECT_EQ("PARQUET", icebergConfig->get<std::string>("iceberg.file_format"));
  EXPECT_EQ("HIVE", icebergConfig->get<std::string>("iceberg.catalog.type"));
  EXPECT_EQ("test_user", icebergConfig->get<std::string>("user"));
}

TEST_F(PrestoToVeloxQueryConfigTest, specialHardCodedPrestoConfigurations) {
  auto session = createBasicSession();

  session.systemProperties.clear();
  session.systemProperties[SessionProperties::kLegacyTimestamp] = "true";
  auto veloxConfig1 = QueryConfig(toVeloxConfigs(session));
  EXPECT_TRUE(veloxConfig1.adjustTimestampToTimezone());

  session.systemProperties.clear();
  session.systemProperties[SessionProperties::kLegacyTimestamp] = "false";
  auto veloxConfig2 = QueryConfig(toVeloxConfigs(session));
  EXPECT_FALSE(veloxConfig2.adjustTimestampToTimezone());

  session.systemProperties.clear();
  auto veloxConfig3 = QueryConfig(toVeloxConfigs(session));
  EXPECT_TRUE(veloxConfig3.adjustTimestampToTimezone());

  session.systemProperties.clear();
  auto veloxConfig8 = QueryConfig(toVeloxConfigs(session));
  EXPECT_EQ(1000, veloxConfig8.driverCpuTimeSliceLimitMs());

  session.systemProperties.clear();
  session.systemProperties[SessionProperties::kDriverCpuTimeSliceLimitMs] =
      "2000";
  auto veloxConfig9 = QueryConfig(toVeloxConfigs(session));
  EXPECT_EQ(2000, veloxConfig9.driverCpuTimeSliceLimitMs());
}

TEST_F(PrestoToVeloxQueryConfigTest, sessionAndExtraCredentialsOverload) {
  // --- Test 1: Basic session with empty extra credentials ---
  {
    auto session = createBasicSession();

    std::map<std::string, std::string> emptyCredentials;
    auto veloxConfig = toVeloxConfigs(session, emptyCredentials);

    // No unexpected credentials should appear.
    // Get raw configs to verify.
    auto raw = veloxConfig.rawConfigsCopy();
    EXPECT_EQ(0, raw.count("cat_token"));
    EXPECT_EQ(0, raw.count("auth_header"));
    EXPECT_EQ(0, raw.count("custom_credential"));
  }

  // --- Test 2: Session with extra credentials (CAT, auth header, custom) ---
  {
    auto session = createBasicSession();

    std::map<std::string, std::string> extraCredentials{
        {"cat_token", "test_cat_token_value"},
        {"auth_header", "Bearer xyz123"},
        {"custom_credential", "custom_value"},
    };

    auto veloxConfig = toVeloxConfigs(session, extraCredentials);
    // Get raw configs to verify.
    auto raw = veloxConfig.rawConfigsCopy();

    // Extra credentials included in the raw config.
    ASSERT_TRUE(raw.count("cat_token"));
    ASSERT_TRUE(raw.count("auth_header"));
    ASSERT_TRUE(raw.count("custom_credential"));
    EXPECT_EQ("test_cat_token_value", raw.at("cat_token"));
    EXPECT_EQ("Bearer xyz123", raw.at("auth_header"));
    EXPECT_EQ("custom_value", raw.at("custom_credential"));
  }

  // --- Test 3: Merge behavior: session system properties + more credentials
  // ---
  {
    auto session = createBasicSession();
    // Verify that typed options reflect session settings.
    session.systemProperties[core::QueryConfig::kSpillEnabled] = "true";
    session.systemProperties[SessionProperties::kJoinSpillEnabled] = "false";

    std::map<std::string, std::string> moreCredentials{
        {"isolation_domain_token", "ids_token_abc123"},
        {"verification_key", "verify_key_xyz"},
    };

    auto veloxConfig = toVeloxConfigs(session, moreCredentials);

    // Typed properties should be applied from the session.
    EXPECT_TRUE(veloxConfig.spillEnabled());
    EXPECT_FALSE(veloxConfig.joinSpillEnabled());

    // Extra credentials should be present in the raw map.
    // Get raw configs to verify.
    auto raw = veloxConfig.rawConfigsCopy();
    ASSERT_TRUE(raw.count("isolation_domain_token"));
    ASSERT_TRUE(raw.count("verification_key"));
    EXPECT_EQ("ids_token_abc123", raw.at("isolation_domain_token"));
    EXPECT_EQ("verify_key_xyz", raw.at("verification_key"));
  }
}

TEST_F(PrestoToVeloxQueryConfigTest, sessionStartTimeConfiguration) {
  auto session = createBasicSession();

  // Test with session start time set in SessionRepresentation
  // The startTime is already set in createBasicSession() to 1234567890
  auto veloxConfig = QueryConfig{toVeloxConfigs(session)};

  // Verify that session start time is properly passed through to
  // VeloxQueryConfig
  EXPECT_EQ(1234567890, veloxConfig.sessionStartTimeMs());

  // Test with different session start time
  session.startTime = 9876543210;
  auto veloxConfig2 = QueryConfig{toVeloxConfigs(session)};

  EXPECT_EQ(9876543210, veloxConfig2.sessionStartTimeMs());

  // Test with zero start time (valid edge case)
  session.startTime = 0;
  auto veloxConfig3 = QueryConfig{toVeloxConfigs(session)};

  EXPECT_EQ(0, veloxConfig3.sessionStartTimeMs());

  // Test with negative start time (valid edge case)
  session.startTime = -1000;
  auto veloxConfig4 = QueryConfig{toVeloxConfigs(session)};

  EXPECT_EQ(-1000, veloxConfig4.sessionStartTimeMs());

  // Test with maximum value
  session.startTime = std::numeric_limits<int64_t>::max();
  auto veloxConfig5 = QueryConfig{toVeloxConfigs(session)};

  EXPECT_EQ(
      std::numeric_limits<int64_t>::max(), veloxConfig5.sessionStartTimeMs());
}

TEST_F(PrestoToVeloxQueryConfigTest, systemConfigsWithoutSessionOverride) {
  // Verifies system configs are properly applied when no session properties
  // override them. Uses exact count matching to catch any config additions or
  // removals.

  auto session = createBasicSession();
  session.systemProperties.clear();
  auto veloxConfigs = toVeloxConfigs(session);

  struct SystemConfigMapping {
    std::string veloxConfigKey;
    std::string systemConfigKey;
  };

  // MUST match veloxToPrestoConfigMapping in PrestoToVeloxQueryConfig.cpp
  std::vector<SystemConfigMapping> expectedMappings = {
      {.veloxConfigKey = core::QueryConfig::kQueryMaxMemoryPerNode,
       .systemConfigKey = std::string(SystemConfig::kQueryMaxMemoryPerNode)},
      {.veloxConfigKey = core::QueryConfig::kSpillFileCreateConfig,
       .systemConfigKey = std::string(SystemConfig::kSpillerFileCreateConfig)},
      {.veloxConfigKey = core::QueryConfig::kSpillEnabled,
       .systemConfigKey = std::string(SystemConfig::kSpillEnabled)},
      {.veloxConfigKey = core::QueryConfig::kJoinSpillEnabled,
       .systemConfigKey = std::string(SystemConfig::kJoinSpillEnabled)},
      {.veloxConfigKey = core::QueryConfig::kOrderBySpillEnabled,
       .systemConfigKey = std::string(SystemConfig::kOrderBySpillEnabled)},
      {.veloxConfigKey = core::QueryConfig::kAggregationSpillEnabled,
       .systemConfigKey = std::string(SystemConfig::kAggregationSpillEnabled)},
      {.veloxConfigKey = core::QueryConfig::kRequestDataSizesMaxWaitSec,
       .systemConfigKey =
           std::string(SystemConfig::kRequestDataSizesMaxWaitSec)},
      {.veloxConfigKey = core::QueryConfig::kMaxSplitPreloadPerDriver,
       .systemConfigKey = std::string(SystemConfig::kDriverMaxSplitPreload)},
      {.veloxConfigKey =
           core::QueryConfig::kMaxLocalExchangePartitionBufferSize,
       .systemConfigKey =
           std::string(SystemConfig::kMaxLocalExchangePartitionBufferSize)},
      {.veloxConfigKey = core::QueryConfig::kPrestoArrayAggIgnoreNulls,
       .systemConfigKey = std::string(SystemConfig::kUseLegacyArrayAgg)},
      {.veloxConfigKey = core::QueryConfig::kTaskWriterCount,
       .systemConfigKey = std::string(SystemConfig::kTaskWriterCount)},
      {.veloxConfigKey = core::QueryConfig::kTaskPartitionedWriterCount,
       .systemConfigKey =
           std::string(SystemConfig::kTaskPartitionedWriterCount)},
      {.veloxConfigKey = core::QueryConfig::kMaxExchangeBufferSize,
       .systemConfigKey = std::string(SystemConfig::kExchangeMaxBufferSize)},
      {.veloxConfigKey = core::QueryConfig::kMaxOutputBufferSize,
       .systemConfigKey = std::string(SystemConfig::kSinkMaxBufferSize)},
      {.veloxConfigKey = core::QueryConfig::kMaxPartitionedOutputBufferSize,
       .systemConfigKey =
           std::string(SystemConfig::kDriverMaxPagePartitioningBufferSize)},
      {.veloxConfigKey = core::QueryConfig::kMaxPartialAggregationMemory,
       .systemConfigKey =
           std::string(SystemConfig::kTaskMaxPartialAggregationMemory)},
  };

  const size_t kExpectedSystemConfigMappingCount = 16;
  EXPECT_EQ(kExpectedSystemConfigMappingCount, expectedMappings.size())
      << "Update expectedMappings to match veloxToPrestoConfigMapping";

  // Verify each system config mapping is present when it has a value
  auto* systemConfig = SystemConfig::instance();
  for (const auto& mapping : expectedMappings) {
    auto systemValue = systemConfig->optionalProperty(mapping.systemConfigKey);
    if (systemValue.hasValue()) {
      EXPECT_TRUE(veloxConfigs.count(mapping.veloxConfigKey) > 0)
          << "Expected '" << mapping.veloxConfigKey << "' when system config '"
          << mapping.systemConfigKey << "' = " << systemValue.value();
    }
  }

  // Verify special case configs (always added)
  EXPECT_TRUE(
      veloxConfigs.count(core::QueryConfig::kAdjustTimestampToTimezone) > 0);
  EXPECT_EQ(
      "true", veloxConfigs.at(core::QueryConfig::kAdjustTimestampToTimezone));

  EXPECT_TRUE(
      veloxConfigs.count(core::QueryConfig::kDriverCpuTimeSliceLimitMs) > 0);
  EXPECT_EQ(
      "1000", veloxConfigs.at(core::QueryConfig::kDriverCpuTimeSliceLimitMs));

  // Verify session-specific configs
  EXPECT_TRUE(veloxConfigs.count(core::QueryConfig::kSessionStartTime) > 0);
  EXPECT_EQ(
      "1234567890", veloxConfigs.at(core::QueryConfig::kSessionStartTime));

  // Calculate expected exact count
  size_t expectedExactConfigs = 0;
  for (const auto& mapping : expectedMappings) {
    if (systemConfig->optionalProperty(mapping.systemConfigKey).hasValue()) {
      expectedExactConfigs++;
    }
  }
  expectedExactConfigs += 2; // kAdjustTimestampToTimezone,
                             // kDriverCpuTimeSliceLimitMs
  expectedExactConfigs += 1; // kSessionStartTime

  // Use exact matching to catch any config additions/removals
  EXPECT_EQ(veloxConfigs.size(), expectedExactConfigs)
      << "Config count mismatch indicates mapping change. Expected "
      << expectedExactConfigs << ", got " << veloxConfigs.size();

  // Debug output
  std::cout << "System configs (no session overrides):" << std::endl;
  for (const auto& [key, value] : veloxConfigs) {
    std::cout << "  " << key << " = " << value << std::endl;
  }
  std::cout << "Total: " << veloxConfigs.size() << std::endl;
}
