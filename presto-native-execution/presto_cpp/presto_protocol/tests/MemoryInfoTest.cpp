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

#include "presto_cpp/main/common/tests/test_json.h"

using namespace facebook::presto::protocol;

class MemoryInfoTest : public ::testing::Test {};

TEST_F(MemoryInfoTest, systemMemoryBytesRoundTrip) {
  // The native worker populates systemMemoryBytes from
  // LinuxMemoryChecker::systemUsedMemoryBytes(). Confirm it survives
  // a JSON round-trip through the generated to_json/from_json pair.
  // The DataSize serializer canonicalizes to six decimal places; input must
  // match that for testJsonRoundtrip's strict JSON equality to hold.
  std::string str = R"({
    "totalNodeMemory": "4.000000GB",
    "systemMemoryBytes": 123456789,
    "pools": {}
  })";

  json j = json::parse(str);
  MemoryInfo p = j;

  ASSERT_EQ(p.systemMemoryBytes, 123456789);
  ASSERT_TRUE(p.pools.empty());

  testJsonRoundtrip(j, p);
}

TEST_F(MemoryInfoTest, legacyPayloadWithoutSystemMemoryBytes) {
  // A pre-upgrade coordinator or worker may send a MemoryInfo payload that
  // omits systemMemoryBytes entirely. The guarded from_json must default the
  // field to 0 instead of throwing.
  std::string str = R"({
    "totalNodeMemory": "4.00GB",
    "pools": {}
  })";

  json j = json::parse(str);
  MemoryInfo p = j;

  ASSERT_EQ(p.systemMemoryBytes, 0);
  ASSERT_TRUE(p.pools.empty());
}

TEST_F(MemoryInfoTest, zeroSystemMemoryBytesIsEmitted) {
  // Even when the value is 0 (e.g. memory checker disabled), the field must
  // appear in the serialized JSON so consumers can distinguish "upgraded
  // worker reporting zero" from "legacy worker".
  MemoryInfo p;
  p.totalNodeMemory = DataSize(4, DataUnit::GIGABYTE);
  p.systemMemoryBytes = 0;

  json j = p;
  ASSERT_TRUE(j.contains("systemMemoryBytes"));
  ASSERT_EQ(j["systemMemoryBytes"], 0);
}
