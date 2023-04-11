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
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class TimestampWithTimeZoneTypeTest : public testing::Test,
                                      public TypeTestBase {
 public:
  TimestampWithTimeZoneTypeTest() {
    registerTimestampWithTimeZoneType();
  }
};

TEST_F(TimestampWithTimeZoneTypeTest, basic) {
  ASSERT_EQ(TIMESTAMP_WITH_TIME_ZONE()->name(), "TIMESTAMP WITH TIME ZONE");
  ASSERT_EQ(TIMESTAMP_WITH_TIME_ZONE()->kindName(), "ROW");
  ASSERT_TRUE(TIMESTAMP_WITH_TIME_ZONE()->parameters().empty());
  ASSERT_EQ(TIMESTAMP_WITH_TIME_ZONE()->toString(), "TIMESTAMP WITH TIME ZONE");

  ASSERT_TRUE(hasType("TIMESTAMP WITH TIME ZONE"));
  ASSERT_EQ(
      *getType("TIMESTAMP WITH TIME ZONE", {}), *TIMESTAMP_WITH_TIME_ZONE());
}

TEST_F(TimestampWithTimeZoneTypeTest, serde) {
  testTypeSerde(TIMESTAMP_WITH_TIME_ZONE());
}
} // namespace facebook::velox::test
