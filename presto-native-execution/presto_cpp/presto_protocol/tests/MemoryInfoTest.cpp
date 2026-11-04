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
  // When present, the field must survive a JSON round-trip bit-for-bit.
  std::string str = R"({
    "totalNodeMemory": "4.000000GB",
    "systemMemoryBytes": 123456789,
    "pools": {}
  })";

  json j = json::parse(str);
  MemoryInfo p = j;

  ASSERT_NE(p.systemMemoryBytes, nullptr);
  ASSERT_EQ(*p.systemMemoryBytes, 123456789);
  ASSERT_TRUE(p.pools.empty());

  testJsonRoundtrip(j, p);
}

TEST_F(MemoryInfoTest, legacyPayloadWithoutSystemMemoryBytes) {
  // A pre-upgrade coordinator or worker may send a MemoryInfo payload that
  // omits systemMemoryBytes. The generated shared_ptr<int64_t> deserializer
  // must leave the pointer null (meaning "not sampled"), not throw.
  std::string str = R"({
    "totalNodeMemory": "4.000000GB",
    "pools": {}
  })";

  json j = json::parse(str);
  MemoryInfo p = j;

  ASSERT_EQ(p.systemMemoryBytes, nullptr);
}
