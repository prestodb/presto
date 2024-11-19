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

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::functions::prestosql {

class IPPrefixTypeTest : public functions::test::FunctionBaseTest {
 protected:
  std::optional<std::string> castToVarchar(
      const std::optional<std::string>& input) {
    auto result = evaluateOnce<std::string>(
        "cast(cast(c0 as ipprefix) as varchar)", input);
    return result;
  }
};

TEST_F(IPPrefixTypeTest, castToVarchar) {
  EXPECT_EQ(castToVarchar("::ffff:1.2.3.4/24"), "1.2.3.0/24");
  EXPECT_EQ(castToVarchar("192.168.0.0/24"), "192.168.0.0/24");
  EXPECT_EQ(castToVarchar("255.2.3.4/0"), "0.0.0.0/0");
  EXPECT_EQ(castToVarchar("255.2.3.4/1"), "128.0.0.0/1");
  EXPECT_EQ(castToVarchar("255.2.3.4/2"), "192.0.0.0/2");
  EXPECT_EQ(castToVarchar("255.2.3.4/4"), "240.0.0.0/4");
  EXPECT_EQ(castToVarchar("1.2.3.4/8"), "1.0.0.0/8");
  EXPECT_EQ(castToVarchar("1.2.3.4/16"), "1.2.0.0/16");
  EXPECT_EQ(castToVarchar("1.2.3.4/24"), "1.2.3.0/24");
  EXPECT_EQ(castToVarchar("1.2.3.255/25"), "1.2.3.128/25");
  EXPECT_EQ(castToVarchar("1.2.3.255/26"), "1.2.3.192/26");
  EXPECT_EQ(castToVarchar("1.2.3.255/28"), "1.2.3.240/28");
  EXPECT_EQ(castToVarchar("1.2.3.255/30"), "1.2.3.252/30");
  EXPECT_EQ(castToVarchar("1.2.3.255/32"), "1.2.3.255/32");
  EXPECT_EQ(
      castToVarchar("2001:0db8:0000:0000:0000:ff00:0042:8329/128"),
      "2001:db8::ff00:42:8329/128");
  EXPECT_EQ(
      castToVarchar("2001:db8::ff00:42:8329/128"),
      "2001:db8::ff00:42:8329/128");
  EXPECT_EQ(castToVarchar("2001:db8:0:0:1:0:0:1/128"), "2001:db8::1:0:0:1/128");
  EXPECT_EQ(castToVarchar("2001:db8:0:0:1::1/128"), "2001:db8::1:0:0:1/128");
  EXPECT_EQ(castToVarchar("2001:db8::1:0:0:1/128"), "2001:db8::1:0:0:1/128");
  EXPECT_EQ(
      castToVarchar("2001:DB8::FF00:ABCD:12EF/128"),
      "2001:db8::ff00:abcd:12ef/128");
  EXPECT_EQ(castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/0"), "::/0");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/1"), "8000::/1");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/2"), "c000::/2");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/4"), "f000::/4");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/8"), "ff00::/8");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/16"), "ffff::/16");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/32"),
      "ffff:ffff::/32");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/48"),
      "ffff:ffff:ffff::/48");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/64"),
      "ffff:ffff:ffff:ffff::/64");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/80"),
      "ffff:ffff:ffff:ffff:ffff::/80");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/96"),
      "ffff:ffff:ffff:ffff:ffff:ffff::/96");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/112"),
      "ffff:ffff:ffff:ffff:ffff:ffff:ffff:0/112");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/120"),
      "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ff00/120");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/124"),
      "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fff0/124");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/126"),
      "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffc/126");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/127"),
      "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe/127");
  EXPECT_EQ(
      castToVarchar("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128"),
      "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128");
  EXPECT_THROW(castToVarchar("facebook.com/32"), VeloxUserError);
  EXPECT_THROW(castToVarchar("localhost/32"), VeloxUserError);
  EXPECT_THROW(castToVarchar("2001:db8::1::1/128"), VeloxUserError);
  EXPECT_THROW(castToVarchar("2001:zxy::1::1/128"), VeloxUserError);
  EXPECT_THROW(castToVarchar("789.1.1.1/32"), VeloxUserError);
  EXPECT_THROW(castToVarchar("192.1.1.1"), VeloxUserError);
  EXPECT_THROW(castToVarchar("192.1.1.1/128"), VeloxUserError);
}
} // namespace facebook::velox::functions::prestosql
