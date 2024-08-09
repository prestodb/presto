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

#include "presto_cpp/main/SessionProperties.h"

using namespace facebook::presto;

class SessionPropertiesTest : public testing::Test {};

TEST_F(SessionPropertiesTest, validateSessionProperties) {
  SessionProperties sessionProperties;
  const std::vector<std::string> names = {
      "legacy_timestamp",
      "driver_cpu_time_slice_limit_ms",
      "native_spill_compression_codec"};
  const std::vector<std::shared_ptr<SessionProperty>> expectedSessionProps = {
      std::make_shared<SessionProperty>(
          "legacy_timestamp",
          "Native Execution only. Use legacy TIME & TIMESTAMP semantics. Warning: "
          "this will be removed",
          "BOOLEAN",
          false,
          "adjust_timestamp_to_session_timezone",
          "true"),
      std::make_shared<SessionProperty>(
          "driver_cpu_time_slice_limit_ms",
          "Native Execution only. The cpu time slice limit in ms that a driver thread. "
          "If not zero, can continuously run without yielding. If it is zero, then "
          "there is no limit.",
          "INTEGER",
          false,
          "driver_cpu_time_slice_limit_ms",
          "1000"),
      std::make_shared<SessionProperty>(
          "native_spill_compression_codec",
          "Native Execution only. The compression algorithm type to compress the "
          "spilled data.\n Supported compression codecs are: ZLIB, SNAPPY, LZO, "
          "ZSTD, LZ4 and GZIP. NONE means no compression.",
          "VARCHAR",
          false,
          "spill_compression_codec",
          "none")};

  for (auto i = 0; i < names.size(); i++) {
    auto property = sessionProperties.getSessionProperty(names[i]);
    EXPECT_EQ(*expectedSessionProps[i], *property);
  }
}

TEST_F(SessionPropertiesTest, sessionPropertyReporter) {
  SessionPropertyReporter propertyReporter;
  const std::vector<std::string> sessionPropertyNames = {
      "native_max_spill_level", "driver_cpu_time_slice_limit_ms"};
  const std::vector<std::string> expectedJsonStrings = {
      R"(
      {
        "defaultValue":"1",
        "description": "Native Execution only. The maximum allowed spilling level for hash join build. 0 is the initial spilling level, -1 means unlimited.",
        "hidden": false,
        "name":"native_max_spill_level",
        "typeSignature":"INTEGER"
      }
    )",
      R"(
      {
        "defaultValue":"1000",
        "description":"Native Execution only. The cpu time slice limit in ms that a driver thread. If not zero, can continuously run without yielding. If it is zero, then there is no limit.",
        "hidden": false,
        "name":"driver_cpu_time_slice_limit_ms",
        "typeSignature":"INTEGER"
      }
    )"};

  for (auto i = 0; i < sessionPropertyNames.size(); i++) {
    json expectedJson = json::parse(expectedJsonStrings[i]);
    json actualJson =
        propertyReporter.getSessionPropertyMetadata(sessionPropertyNames[i]);
    EXPECT_EQ(expectedJson, actualJson);
  }
}

TEST_F(SessionPropertiesTest, reportAllProperties) {
  SessionPropertyReporter propertyReporter;
  json allProperties = propertyReporter.getSessionPropertiesMetadata();
  for (const auto& property : allProperties) {
    auto name = property["name"];
    json expectedProperty = propertyReporter.getSessionPropertyMetadata(name);
    EXPECT_EQ(property, expectedProperty);
  }
}
