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
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class JsonTypeTest : public testing::Test, public TypeTestBase {
 public:
  JsonTypeTest() {
    registerJsonType();
  }
};

TEST_F(JsonTypeTest, basic) {
  ASSERT_STREQ(JSON()->name(), "JSON");
  ASSERT_STREQ(JSON()->kindName(), "VARCHAR");
  ASSERT_TRUE(JSON()->parameters().empty());
  ASSERT_EQ(JSON()->toString(), "JSON");

  ASSERT_TRUE(hasType("JSON"));
  ASSERT_EQ(*getType("JSON", {}), *JSON());
}

TEST_F(JsonTypeTest, serde) {
  testTypeSerde(JSON());
}
} // namespace facebook::velox::test
