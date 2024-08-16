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
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class HyperLogLogTypeTest : public testing::Test, public TypeTestBase {
 public:
  HyperLogLogTypeTest() {
    registerHyperLogLogType();
  }
};

TEST_F(HyperLogLogTypeTest, basic) {
  ASSERT_STREQ(HYPERLOGLOG()->name(), "HYPERLOGLOG");
  ASSERT_STREQ(HYPERLOGLOG()->kindName(), "VARBINARY");
  ASSERT_TRUE(HYPERLOGLOG()->parameters().empty());
  ASSERT_EQ(HYPERLOGLOG()->toString(), "HYPERLOGLOG");

  ASSERT_TRUE(hasType("HYPERLOGLOG"));
  ASSERT_EQ(*getType("HYPERLOGLOG", {}), *HYPERLOGLOG());
}

TEST_F(HyperLogLogTypeTest, serde) {
  testTypeSerde(HYPERLOGLOG());
}
} // namespace facebook::velox::test
