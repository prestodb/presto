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

TEST(LifespanTest, taskWide) {
  std::string str = R"("TaskWide")";

  json j = json::parse(str);
  Lifespan p = j;

  // Check some values ...
  ASSERT_EQ(p.isgroup, false);
  ASSERT_EQ(p.groupid, 0);

  testJsonRoundtrip(j, p);
}

TEST(LifespanTest, group) {
  std::string str = R"("Group1001")";

  json j = json::parse(str);
  Lifespan p = j;

  // Check some values ...
  ASSERT_EQ(p.isgroup, true);
  ASSERT_EQ(p.groupid, 1001);

  testJsonRoundtrip(j, p);
}
