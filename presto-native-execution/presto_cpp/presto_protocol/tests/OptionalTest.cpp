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
#include "presto_cpp/presto_protocol/presto_protocol.h"

using namespace facebook::presto::protocol;

class TestPrestoProtocol : public ::testing::Test {};

TEST_F(TestPrestoProtocol, TestOptionalPresent) {
  std::string str = R"( { "type": "ALL", "role": "My Role" })";

  json j = json::parse(str);
  SelectedRole p = j;

  // Check some values ...
  ASSERT_EQ(p.type, SelectedRoleType::ALL);
  ASSERT_NE(p.role, nullptr);
  ASSERT_EQ(*p.role, "My Role");

  testJsonRoundtrip(j, p);
}

TEST_F(TestPrestoProtocol, TestOptionalAbsent) {
  std::string str = R"( { "type": "ALL" })";

  json j = json::parse(str);
  SelectedRole p = j;

  // Check some values ...
  ASSERT_EQ(p.type, SelectedRoleType::ALL);
  ASSERT_EQ(p.role, nullptr);

  testJsonRoundtrip(j, p);
}
