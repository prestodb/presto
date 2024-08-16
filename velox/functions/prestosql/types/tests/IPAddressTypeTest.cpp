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
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class IPAddressTypeTest : public testing::Test, public TypeTestBase {
 public:
  IPAddressTypeTest() {
    registerIPAddressType();
  }
};

TEST_F(IPAddressTypeTest, basic) {
  ASSERT_STREQ(IPADDRESS()->name(), "IPADDRESS");
  ASSERT_STREQ(IPADDRESS()->kindName(), "HUGEINT");
  ASSERT_TRUE(IPADDRESS()->parameters().empty());
  ASSERT_EQ(IPADDRESS()->toString(), "IPADDRESS");

  ASSERT_TRUE(hasType("IPADDRESS"));
  ASSERT_EQ(*getType("IPADDRESS", {}), *IPADDRESS());
}

TEST_F(IPAddressTypeTest, serde) {
  testTypeSerde(IPADDRESS());
}
} // namespace facebook::velox::test
