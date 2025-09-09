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
#include "velox/functions/prestosql/types/P4HyperLogLogType.h"
#include "velox/functions/prestosql/types/P4HyperLogLogRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class P4HyperLogLogTypeTest : public testing::Test, public TypeTestBase {
 public:
  P4HyperLogLogTypeTest() {
    registerP4HyperLogLogType();
  }
};

TEST_F(P4HyperLogLogTypeTest, basic) {
  ASSERT_STREQ(P4HYPERLOGLOG()->name(), "P4HYPERLOGLOG");
  ASSERT_STREQ(P4HYPERLOGLOG()->kindName(), "VARBINARY");
  ASSERT_TRUE(P4HYPERLOGLOG()->parameters().empty());
  ASSERT_EQ(P4HYPERLOGLOG()->toString(), "P4HYPERLOGLOG");

  ASSERT_TRUE(hasType("P4HYPERLOGLOG"));
  ASSERT_EQ(*getType("P4HYPERLOGLOG", {}), *P4HYPERLOGLOG());

  ASSERT_FALSE(P4HYPERLOGLOG()->isOrderable());
}

TEST_F(P4HyperLogLogTypeTest, serde) {
  testTypeSerde(P4HYPERLOGLOG());
}
} // namespace facebook::velox::test
