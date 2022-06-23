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
#include "presto_cpp/main/types/PrestoTaskId.h"
#include <gtest/gtest.h>

using facebook::presto::PrestoTaskId;

TEST(PrestoTaskIdTest, basic) {
  PrestoTaskId id("20201107_130540_00011_wrpkw.1.2.3");

  EXPECT_EQ(id.queryId(), "20201107_130540_00011_wrpkw");
  EXPECT_EQ(id.stageId(), 1);
  EXPECT_EQ(id.stageExecutionId(), 2);
  EXPECT_EQ(id.id(), 3);
}

TEST(PrestoTaskIdTest, malformed) {
  ASSERT_THROW(PrestoTaskId(""), std::invalid_argument);
  ASSERT_THROW(
      PrestoTaskId("20201107_130540_00011_wrpkw."), std::invalid_argument);
  ASSERT_THROW(PrestoTaskId("q.1.2"), std::invalid_argument);
}
