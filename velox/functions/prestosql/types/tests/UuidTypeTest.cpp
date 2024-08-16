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
#include "velox/functions/prestosql/types/UuidType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class UuidTypeTest : public testing::Test, public TypeTestBase {
 public:
  UuidTypeTest() {
    registerUuidType();
  }
};

TEST_F(UuidTypeTest, basic) {
  ASSERT_STREQ(UUID()->name(), "UUID");
  ASSERT_STREQ(UUID()->kindName(), "HUGEINT");
  ASSERT_TRUE(UUID()->parameters().empty());
  ASSERT_EQ(UUID()->toString(), "UUID");

  ASSERT_TRUE(hasType("UUID"));
  ASSERT_EQ(*getType("UUID", {}), *UUID());
}

TEST_F(UuidTypeTest, serde) {
  testTypeSerde(UUID());
}
} // namespace facebook::velox::test
