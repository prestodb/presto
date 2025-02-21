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
#include "velox/functions/prestosql/types/IPAddressRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class IPAddressTypeTest : public testing::Test, public TypeTestBase {
 public:
  IPAddressTypeTest() {
    registerIPAddressType();
  }

  int128_t getIPv6asInt128FromStringUnchecked(const std::string& ipAddr) {
    auto ret = ipaddress::tryGetIPv6asInt128FromString(ipAddr);
    return ret.value();
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

TEST_F(IPAddressTypeTest, compare) {
  auto ipAddr = IPADDRESS();
  // Baisc IPv4 test
  ASSERT_EQ(
      0,
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("1.1.1.1"),
          getIPv6asInt128FromStringUnchecked("1.1.1.1")));
  ASSERT_EQ(
      0,
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("255.255.255.255"),
          getIPv6asInt128FromStringUnchecked("255.255.255.255")));
  ASSERT_GT(
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("1.2.3.4"),
          getIPv6asInt128FromStringUnchecked("1.1.1.1")),
      0);
  ASSERT_GT(
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("1.1.1.2"),
          getIPv6asInt128FromStringUnchecked("1.1.1.1")),
      0);
  ASSERT_GT(
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("1.1.2.1"),
          getIPv6asInt128FromStringUnchecked("1.1.1.2")),
      0);
  ASSERT_LT(
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("1.1.1.1"),
          getIPv6asInt128FromStringUnchecked("1.1.2.1")),
      0);
  ASSERT_LT(
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("1.1.1.1"),
          getIPv6asInt128FromStringUnchecked("255.1.1.1")),
      0);

  // Basic IPv6 test
  ASSERT_EQ(
      0,
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("::1"),
          getIPv6asInt128FromStringUnchecked("::1")));
  ASSERT_EQ(
      0,
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked(
              "2001:0db8:0000:0000:0000:ff00:0042:8329"),
          getIPv6asInt128FromStringUnchecked("2001:db8::ff00:42:8329")));
  ASSERT_EQ(
      0,
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("::ffff:1.2.3.4"),
          getIPv6asInt128FromStringUnchecked("1.2.3.4")));
  ASSERT_LT(
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("::ffff:0.1.1.1"),
          getIPv6asInt128FromStringUnchecked("::ffff:1.1.1.0")),
      0);

  ASSERT_GT(
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("::FFFF:FFFF:FFFF"),
          getIPv6asInt128FromStringUnchecked("::0001:255.255.255.255")),
      0);
  ASSERT_LT(
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("::0001:255.255.255.255"),
          getIPv6asInt128FromStringUnchecked("255.255.255.255")),
      0);
  ASSERT_EQ(
      0,
      ipAddr->compare(
          getIPv6asInt128FromStringUnchecked("::ffff:ffff:ffff"),
          getIPv6asInt128FromStringUnchecked("255.255.255.255")));
}
} // namespace facebook::velox::test
