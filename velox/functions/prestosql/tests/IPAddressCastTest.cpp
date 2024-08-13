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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::prestosql {

namespace {

class IPAddressCastTest : public functions::test::FunctionBaseTest {
 protected:
  std::optional<std::string> castToVarchar(
      const std::optional<std::string> input) {
    auto result = evaluateOnce<std::string>(
        "cast(cast(c0 as ipaddress) as varchar)", input);
    return result;
  }

  std::optional<int128_t> castFromVarbinary(
      const std::optional<std::string> input) {
    auto result =
        evaluateOnce<int128_t>("cast(from_hex(c0) as ipaddress)", input);
    return result;
  }

  std::optional<std::string> allCasts(const std::optional<std::string> input) {
    auto result = evaluateOnce<std::string>(
        "cast(cast(cast(cast(c0 as ipaddress) as varbinary) as ipaddress) as varchar)",
        input);
    return result;
  }
};

int128_t stringToInt128(std::string value) {
  int128_t res = 0;
  for (char c : value) {
    res = res * 10 + c - '0';
  }
  return res;
}

TEST_F(IPAddressCastTest, castToVarchar) {
  EXPECT_EQ(castToVarchar("::ffff:1.2.3.4"), "1.2.3.4");
  EXPECT_EQ(castToVarchar("0:0:0:0:0:0:13.1.68.3"), "::13.1.68.3");
  EXPECT_EQ(castToVarchar("1.2.3.4"), "1.2.3.4");
  EXPECT_EQ(castToVarchar("192.168.0.0"), "192.168.0.0");
  EXPECT_EQ(
      castToVarchar("2001:0db8:0000:0000:0000:ff00:0042:8329"),
      "2001:db8::ff00:42:8329");
  EXPECT_EQ(castToVarchar("2001:db8::ff00:42:8329"), "2001:db8::ff00:42:8329");
  EXPECT_EQ(castToVarchar("2001:db8:0:0:1:0:0:1"), "2001:db8::1:0:0:1");
  EXPECT_EQ(castToVarchar("2001:db8:0:0:1::1"), "2001:db8::1:0:0:1");
  EXPECT_EQ(castToVarchar("2001:db8::1:0:0:1"), "2001:db8::1:0:0:1");
  EXPECT_EQ(
      castToVarchar("2001:DB8::FF00:ABCD:12EF"), "2001:db8::ff00:abcd:12ef");
  VELOX_ASSERT_THROW(
      castToVarchar("facebook.com"), "Invalid IP address 'facebook.com'");
  VELOX_ASSERT_THROW(
      castToVarchar("localhost"), "Invalid IP address 'localhost'");
  VELOX_ASSERT_THROW(
      castToVarchar("2001:db8::1::1"), "Invalid IP address '2001:db8::1::1'");
  VELOX_ASSERT_THROW(
      castToVarchar("2001:zxy::1::1"), "Invalid IP address '2001:zxy::1::1'");
  VELOX_ASSERT_THROW(
      castToVarchar("789.1.1.1"), "Invalid IP address '789.1.1.1'");
}

TEST_F(IPAddressCastTest, castFromVarbinary) {
  EXPECT_EQ(
      castFromVarbinary("00000000000000000000ffff01020304"),
      stringToInt128("281470698652420"));
  EXPECT_EQ(castFromVarbinary("01020304"), stringToInt128("281470698652420"));
  EXPECT_EQ(castFromVarbinary("c0a80000"), stringToInt128("281473913978880"));
  EXPECT_EQ(
      castFromVarbinary("20010db8000000000000ff0000428329"),
      stringToInt128("42540766411282592856904265327123268393"));
  EXPECT_THROW(castFromVarbinary("f000001100"), VeloxUserError);
}

TEST_F(IPAddressCastTest, allCasts) {
  EXPECT_EQ(allCasts("::ffff:1.2.3.4"), "1.2.3.4");
  EXPECT_EQ(
      allCasts("2001:0db8:0000:0000:0000:ff00:0042:8329"),
      "2001:db8::ff00:42:8329");
  EXPECT_EQ(allCasts("2001:db8::ff00:42:8329"), "2001:db8::ff00:42:8329");
}

TEST_F(IPAddressCastTest, nullTest) {
  EXPECT_EQ(castToVarchar(std::nullopt), std::nullopt);
  EXPECT_EQ(castFromVarbinary(std::nullopt), std::nullopt);
}

TEST_F(IPAddressCastTest, castRoundTrip) {
  auto strings = makeFlatVector<std::string>(
      {"87a0:ce14:8989:44c9:826e:b4d8:73f9:1542",
       "7cd6:bcec:1216:5c20:4b67:b1bd:173:ced",
       "192.128.0.0"});

  auto ipaddresses =
      evaluate("cast(c0 as ipaddress)", makeRowVector({strings}));
  auto stringsCopy =
      evaluate("cast(c0 as varchar)", makeRowVector({ipaddresses}));
  auto ipaddressesCopy =
      evaluate("cast(c0 as ipaddress)", makeRowVector({stringsCopy}));

  velox::test::assertEqualVectors(strings, stringsCopy);
  velox::test::assertEqualVectors(ipaddresses, ipaddressesCopy);
}
} // namespace

} // namespace facebook::velox::functions::prestosql
