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
#include "velox/functions/prestosql/types/BingTileType.h"
#include "velox/functions/prestosql/types/BingTileRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class BingTileTypeTest : public testing::Test, public TypeTestBase {
 public:
  BingTileTypeTest() {
    registerBingTileType();
  }
};

TEST_F(BingTileTypeTest, basic) {
  ASSERT_STREQ(BINGTILE()->name(), "BINGTILE");
  ASSERT_STREQ(BINGTILE()->kindName(), "BIGINT");
  ASSERT_TRUE(BINGTILE()->parameters().empty());
  ASSERT_EQ(BINGTILE()->toString(), "BINGTILE");

  ASSERT_TRUE(hasType("BINGTILE"));
  ASSERT_EQ(*getType("BINGTILE", {}), *BINGTILE());
}

TEST_F(BingTileTypeTest, serde) {
  testTypeSerde(BINGTILE());
}

} // namespace facebook::velox::test
