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
#include "velox/functions/prestosql/BinaryFunctions.h"
#include <gtest/gtest.h>
#include <array>
#include <limits>
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

std::string hexToDec(const std::string& str) {
  VELOX_CHECK_EQ(str.size() % 2, 0);
  std::string out;
  out.resize(str.size() / 2);
  for (int i = 0; i < out.size(); ++i) {
    int high = facebook::velox::functions::fromHex(str[2 * i]);
    int low = facebook::velox::functions::fromHex(str[2 * i + 1]);
    out[i] = (high << 4) | (low & 0xf);
  }
  return out;
}

class BinaryFunctionsTest : public FunctionBaseTest {};

TEST_F(BinaryFunctionsTest, md5) {
  const auto md5 = [&](std::optional<std::string> arg) {
    return evaluateOnce<std::string>("md5(c0)", VARBINARY(), arg);
  };

  EXPECT_EQ(hexToDec("533f6357e0210e67d91f651bc49e1278"), md5("hashme"));
  EXPECT_EQ(hexToDec("eb2ac5b04180d8d6011a016aeb8f75b3"), md5("Infinity"));
  EXPECT_EQ(hexToDec("d41d8cd98f00b204e9800998ecf8427e"), md5(""));

  EXPECT_EQ(std::nullopt, md5(std::nullopt));
}

TEST_F(BinaryFunctionsTest, sha1) {
  const auto sha1 = [&](std::optional<std::string> arg) {
    return evaluateOnce<std::string>("sha1(c0)", VARBINARY(), arg);
  };

  // The result values were obtained from Presto Java sha1 function.

  EXPECT_EQ(hexToDec("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709"), sha1(""));
  EXPECT_EQ(std::nullopt, sha1(std::nullopt));

  EXPECT_EQ(hexToDec("86F7E437FAA5A7FCE15D1DDCB9EAEAEA377667B8"), sha1("a"));
  EXPECT_EQ(hexToDec("382758154F5D9F9775B6A9F28B6EDD55773C87E3"), sha1("AB "));
  EXPECT_EQ(hexToDec("B858CB282617FB0956D960215C8E84D1CCF909C6"), sha1(" "));
  EXPECT_EQ(
      hexToDec("A47779C6198B85A1A2595C7C9AAAB26199EA8084"),
      sha1("               "));
  EXPECT_EQ(
      hexToDec("082DE68D348CBB63316DF2B7B74B0A2DBB716F4A"),
      sha1("SPECIAL_#@,$|%/^~?{}+-"));
  EXPECT_EQ(
      hexToDec("01B307ACBA4F54F55AAFC33BB06BBBF6CA803E9A"), sha1("1234567890"));
  EXPECT_EQ(
      hexToDec("E46990399602E8321A69285244B955816738981E"),
      sha1("12345.67890"));
  EXPECT_EQ(
      hexToDec("17BC9B38933EB1C0D5D1F8F6D9B6C375851B9685"),
      sha1("more_than_12_characters_string"));
}

TEST_F(BinaryFunctionsTest, sha256) {
  const auto sha256 = [&](std::optional<std::string> arg) {
    return evaluateOnce<std::string>("sha256(c0)", VARBINARY(), arg);
  };

  EXPECT_EQ(
      hexToDec(
          "02208b9403a87df9f4ed6b2ee2657efaa589026b4cce9accc8e8a5bf3d693c86"),
      sha256("hashme"));
  EXPECT_EQ(
      hexToDec(
          "d0067cad9a63e0813759a2bb841051ca73570c0da2e08e840a8eb45db6a7a010"),
      sha256("Infinity"));
  EXPECT_EQ(
      hexToDec(
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
      sha256(""));

  EXPECT_EQ(std::nullopt, sha256(std::nullopt));
}

TEST_F(BinaryFunctionsTest, sha512) {
  const auto sha512 = [&](std::optional<std::string> arg) {
    return evaluateOnce<std::string>("sha512(c0)", VARBINARY(), arg);
  };

  EXPECT_EQ(
      hexToDec(
          "1f6b05823a0453c1ec55009555087e8226d774c7c49d099784317b8460a0623ddaa083334f9218dda8075e0a0dc8319f89199f04e6b8f3980a73556866b388ae"),
      sha512("prestodb"));
  EXPECT_EQ(
      hexToDec(
          "7de872ed1c41ce3901bb7f12f20b0c0106331fe5b5ecc5fbbcf3ce6c79df4da595ebb7e221ab8b7fc5d918583eac6890ade1c26436335d3835828011204b7679"),
      sha512("Infinity"));
  EXPECT_EQ(
      hexToDec(
          "30163935c002fc4e1200906c3d30a9c4956b4af9f6dcaef1eb4b1fcb8fba69e7a7acdc491ea5b1f2864ea8c01b01580ef09defc3b11b3f183cb21d236f7f1a6b"),
      sha512("hash"));
  EXPECT_EQ(
      hexToDec(
          "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"),
      sha512(""));
  EXPECT_EQ(std::nullopt, sha512(std::nullopt));
}

TEST_F(BinaryFunctionsTest, spookyHashV232) {
  const auto spookyHashV232 = [&](std::optional<std::string> arg) {
    return evaluateOnce<std::string>("spooky_hash_v2_32(c0)", VARBINARY(), arg);
  };

  // The result values were obtained from Presto Java spooky_hash_v2_32
  // function.

  EXPECT_EQ(hexToDec("6BF50919"), spookyHashV232(""));
  EXPECT_EQ(std::nullopt, spookyHashV232(std::nullopt));

  EXPECT_EQ(hexToDec("D382E6CA"), spookyHashV232("hello"));
  EXPECT_EQ(hexToDec("4DB3FC9E"), spookyHashV232("       "));
  EXPECT_EQ(hexToDec("DC33E6F0"), spookyHashV232("special_#@,$|%/^~?{}+-"));
  EXPECT_EQ(hexToDec("C5CD219B"), spookyHashV232("1234567890"));
  EXPECT_EQ(
      hexToDec("B95F627C"), spookyHashV232("more_than_12_characters_string"));
}

TEST_F(BinaryFunctionsTest, spookyHashV264) {
  const auto spookyHashV264 = [&](std::optional<std::string> arg) {
    return evaluateOnce<std::string>("spooky_hash_v2_64(c0)", VARBINARY(), arg);
  };

  // The result values were obtained from Presto Java spooky_hash_v2_64
  // function.

  EXPECT_EQ(hexToDec("232706FC6BF50919"), spookyHashV264(""));
  EXPECT_EQ(std::nullopt, spookyHashV264(std::nullopt));

  EXPECT_EQ(hexToDec("3768826AD382E6CA"), spookyHashV264("hello"));
  EXPECT_EQ(hexToDec("8A63CCE34DB3FC9E"), spookyHashV264("       "));
  EXPECT_EQ(
      hexToDec("AAF4B42DDC33E6F0"), spookyHashV264("special_#@,$|%/^~?{}+-"));
  EXPECT_EQ(hexToDec("D9426F48C5CD219B"), spookyHashV264("1234567890"));
  EXPECT_EQ(
      hexToDec("3493AE21B95F627C"),
      spookyHashV264("more_than_12_characters_string"));
}

TEST_F(BinaryFunctionsTest, HmacSha1) {
  const auto hmacSha1 = [&](std::optional<std::string> arg,
                            std::optional<std::string> key) {
    return evaluateOnce<std::string>(
        "hmac_sha1(c0, c1)", {VARBINARY(), VARBINARY()}, arg, key);
  };
  // Use python hmac lib results as the expected value.
  // >>> import hmac
  // >>> def sha1(data, key):
  //         print(hmac.new(key, data, digestmod='sha1').hexdigest())
  // >>> sha1(b"hashme", b"velox")
  // d49c944625bdde6c47ad93ea63952bfcf16a630a
  // >>> sha1(b"Infinity", b"velox")
  // c19b6b753fe4ac28579c7e84d18feb29760a0d1c
  // >>> sha1(b"", b"velox")
  // d0569c4a4f3df995b04ec497b12872c4a2f97517
  // >>> sha1(b"12345abcde54321", b"velox")
  // 183054bdaf8c83320fee4376e76ffd7e773a650f
  // sha1(b"velox", b"")
  // 3ec5ea98df0f5ddb139231ecee2c8a9810a82e08
  EXPECT_EQ(
      hexToDec("d49c944625bdde6c47ad93ea63952bfcf16a630a"),
      hmacSha1("hashme", "velox"));
  EXPECT_EQ(
      hexToDec("c19b6b753fe4ac28579c7e84d18feb29760a0d1c"),
      hmacSha1("Infinity", "velox"));
  EXPECT_EQ(
      hexToDec("d0569c4a4f3df995b04ec497b12872c4a2f97517"),
      hmacSha1("", "velox"));
  EXPECT_EQ(std::nullopt, hmacSha1(std::nullopt, "velox"));
  EXPECT_EQ(
      hexToDec("183054bdaf8c83320fee4376e76ffd7e773a650f"),
      hmacSha1("12345abcde54321", "velox"));
  EXPECT_EQ(
      hexToDec("3ec5ea98df0f5ddb139231ecee2c8a9810a82e08"),
      hmacSha1("velox", ""));
  EXPECT_EQ(std::nullopt, hmacSha1("velox", std::nullopt));
}

TEST_F(BinaryFunctionsTest, HmacSha256) {
  const auto hmacSha256 = [&](std::optional<std::string> arg,
                              std::optional<std::string> key) {
    return evaluateOnce<std::string>(
        "hmac_sha256(c0, c1)", {VARBINARY(), VARBINARY()}, arg, key);
  };
  // Use python hmac lib results as the expected value.
  // >>> import hmac
  // >>> def sha256(data, key):
  //         print(hmac.new(key, data, digestmod='sha256').hexdigest())
  // >>> sha256(b"hashme", b"velox")
  // 24bb7fa25fd592ef6a4c939d4fb91b7f7f04f8813260961101117ec30f865794
  // >>> sha256(b"Infinity", b"velox")
  // f45718c9586ae7d761194485d15cbf6284b5b606ade4f9d5820fbdd1eaf52b75
  // >>> sha256(b"", b"velox")
  // fd8658b6a6b6601155fecf9a39b6f95cf030863e550073423a8e250a35c6f5a4
  EXPECT_EQ(
      hexToDec(
          "24bb7fa25fd592ef6a4c939d4fb91b7f7f04f8813260961101117ec30f865794"),
      hmacSha256("hashme", "velox"));
  EXPECT_EQ(
      hexToDec(
          "f45718c9586ae7d761194485d15cbf6284b5b606ade4f9d5820fbdd1eaf52b75"),
      hmacSha256("Infinity", "velox"));
  EXPECT_EQ(
      hexToDec(
          "fd8658b6a6b6601155fecf9a39b6f95cf030863e550073423a8e250a35c6f5a4"),
      hmacSha256("", "velox"));
  EXPECT_EQ(std::nullopt, hmacSha256(std::nullopt, "velox"));
}

TEST_F(BinaryFunctionsTest, HmacSha512) {
  const auto hmacSha512 = [&](std::optional<std::string> arg,
                              std::optional<std::string> key) {
    return evaluateOnce<std::string>(
        "hmac_sha512(c0, c1)", {VARBINARY(), VARBINARY()}, arg, key);
  };
  // Use the same expected value from TestVarbinaryFunctions of presto java
  EXPECT_EQ(
      hexToDec(
          "84FA5AA0279BBC473267D05A53EA03310A987CECC4C1535FF29B6D76B8F1444A728DF3AADB89D4A9A6709E1998F373566E8F824A8CA93B1821F0B69BC2A2F65E"),
      hmacSha512("", "key"));
  EXPECT_EQ(
      hexToDec(
          "FEFA712B67DED871E1ED987F8B20D6A69EB9FCC87974218B9A1A6D5202B54C18ECDA4839A979DED22F07E0881CF40B762691992D120408F49D6212E112509D72"),
      hmacSha512("hashme", "key"));
  EXPECT_EQ(std::nullopt, hmacSha512(std::nullopt, "velox"));
}

TEST_F(BinaryFunctionsTest, HmacMd5) {
  const auto hmacMd5 = [&](std::optional<std::string> arg,
                           std::optional<std::string> key) {
    return evaluateOnce<std::string>(
        "hmac_md5(c0, c1)", {VARBINARY(), VARBINARY()}, arg, key);
  };
  // The result values were obtained from Presto Java hmac_md5 function.
  EXPECT_EQ(
      hexToDec("ff66d72875f01e26fcbe71d973eaf524"), hmacMd5("hashme", "velox"));
  EXPECT_EQ(
      hexToDec("ed706a89f46773b7a478ee5d8f83db86"),
      hmacMd5("Infinity", "velox"));
  EXPECT_EQ(hexToDec("f05e7a0086c6633b496ee411646da51c"), hmacMd5("", "velox"));
  EXPECT_EQ(std::nullopt, hmacMd5(std::nullopt, "velox"));
}

TEST_F(BinaryFunctionsTest, crc32) {
  const auto crc32 = [&](std::optional<std::string> value) {
    return evaluateOnce<int64_t>("crc32(c0)", VARBINARY(), value);
  };
  // use python3 zlib result as the expected values,
  // >>> import zlib
  // >>> print(zlib.crc32(b"DEAD_BEEF"))
  // 2634114297
  // >>> print(zlib.crc32(b"CRC32"))
  // 4128576900
  // >>> print(zlib.crc32(b"velox is an open source unified execution engine."))
  // 2173230066
  EXPECT_EQ(std::nullopt, crc32(std::nullopt));
  EXPECT_EQ(2634114297L, crc32("DEAD_BEEF"));
  EXPECT_EQ(4128576900L, crc32("CRC32"));
  EXPECT_EQ(
      2173230066L, crc32("velox is an open source unified execution engine."));
}

TEST_F(BinaryFunctionsTest, xxhash64) {
  const auto xxhash64 = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("xxhash64(c0)", VARBINARY(), value);
  };

  const auto toVarbinary = [](const int64_t input) {
    std::string out;
    out.resize(sizeof(input));
    std::memcpy(out.data(), &input, sizeof(input));
    return out;
  };

  EXPECT_EQ(hexToDec("EF46DB3751D8E999"), xxhash64(""));
  EXPECT_EQ(std::nullopt, xxhash64(std::nullopt));

  EXPECT_EQ(hexToDec("F9D96E0E1165E892"), xxhash64("hashme"));
  EXPECT_EQ(hexToDec("26C7827D889F6DA3"), xxhash64("hello"));
  EXPECT_EQ(hexToDec("8B29AA4768367C53"), xxhash64("ABC "));
  EXPECT_EQ(hexToDec("2C32708C2F5068F9"), xxhash64("       "));
  EXPECT_EQ(hexToDec("C2B3E0336D3E0F35"), xxhash64("special_#@,$|%/^~?{}+-"));
  EXPECT_EQ(hexToDec("A9D4D4132EFF23B6"), xxhash64("1234567890"));
  EXPECT_EQ(
      hexToDec("D73C92CF24E6EC82"), xxhash64("more_than_12_characters_string"));
}

TEST_F(BinaryFunctionsTest, toHex) {
  const auto toHex = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("to_hex(cast(c0 as varbinary))", value);
  };

  EXPECT_EQ(std::nullopt, toHex(std::nullopt));
  EXPECT_EQ("", toHex(""));
  EXPECT_EQ("61", toHex("a"));
  EXPECT_EQ("616263", toHex("abc"));
  EXPECT_EQ("68656C6C6F20776F726C64", toHex("hello world"));
  EXPECT_EQ(
      "48656C6C6F20576F726C642066726F6D2056656C6F7821",
      toHex("Hello World from Velox!"));

  const auto toHexFromBase64 = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("to_hex(from_base64(c0))", value);
  };

  EXPECT_EQ(
      "D763DAB175DA5814349354FCF23885",
      toHexFromBase64("12PasXXaWBQ0k1T88jiF"));
}

TEST_F(BinaryFunctionsTest, fromHex) {
  const auto fromHex = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("from_hex(c0)", value);
  };

  EXPECT_EQ(std::nullopt, fromHex(std::nullopt));
  EXPECT_EQ("", fromHex(""));
  EXPECT_EQ("a", fromHex("61"));
  EXPECT_EQ("abc", fromHex("616263"));
  EXPECT_EQ("azo", fromHex("617a6f"));
  EXPECT_EQ("azo", fromHex("617a6F"));
  EXPECT_EQ("azo", fromHex("617A6F"));
  EXPECT_EQ("hello world", fromHex("68656C6C6F20776F726C64"));
  EXPECT_EQ(
      "Hello World from Velox!",
      fromHex("48656C6C6F20576F726C642066726F6D2056656C6F7821"));

  EXPECT_THROW(fromHex("f/"), VeloxUserError);
  EXPECT_THROW(fromHex("f:"), VeloxUserError);
  EXPECT_THROW(fromHex("f@"), VeloxUserError);
  EXPECT_THROW(fromHex("f`"), VeloxUserError);
  EXPECT_THROW(fromHex("fg"), VeloxUserError);
  EXPECT_THROW(fromHex("fff"), VeloxUserError);

  const auto fromHexToBase64 = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("to_base64(from_hex(c0))", value);
  };
  EXPECT_EQ(
      "12PasXXaWBQ0k1T88jiF",
      fromHexToBase64("D763DAB175DA5814349354FCF23885"));
}

TEST_F(BinaryFunctionsTest, toBase64) {
  const auto toBase64 = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("to_base64(cast(c0 as varbinary))", value);
  };
  const auto fromHex = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("from_hex(c0)", value);
  };

  EXPECT_EQ(std::nullopt, toBase64(std::nullopt));
  EXPECT_EQ("", toBase64(""));
  EXPECT_EQ("YQ==", toBase64("a"));
  EXPECT_EQ("YWJj", toBase64("abc"));
  EXPECT_EQ("aGVsbG8gd29ybGQ=", toBase64("hello world"));
  EXPECT_EQ(
      "SGVsbG8gV29ybGQgZnJvbSBWZWxveCE=", toBase64("Hello World from Velox!"));
  EXPECT_EQ("/0+/UA==", toBase64(fromHex("FF4FBF50")));
}

TEST_F(BinaryFunctionsTest, toBase64Url) {
  const auto toBase64Url = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>(
        "to_base64url(cast(c0 as varbinary))", value);
  };
  const auto fromHex = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("from_hex(c0)", value);
  };

  EXPECT_EQ(std::nullopt, toBase64Url(std::nullopt));
  EXPECT_EQ("", toBase64Url(""));
  EXPECT_EQ("YQ==", toBase64Url("a"));
  EXPECT_EQ("YWJj", toBase64Url("abc"));
  EXPECT_EQ("aGVsbG8gd29ybGQ=", toBase64Url("hello world"));
  EXPECT_EQ(
      "SGVsbG8gV29ybGQgZnJvbSBWZWxveCE=",
      toBase64Url("Hello World from Velox!"));
  EXPECT_EQ("_0-_UA==", toBase64Url(fromHex("FF4FBF50")));
}

TEST_F(BinaryFunctionsTest, fromBase64) {
  const auto fromBase64 = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("from_base64(c0)", value);
  };

  EXPECT_EQ(std::nullopt, fromBase64(std::nullopt));
  EXPECT_EQ("", fromBase64(""));
  EXPECT_EQ("a", fromBase64("YQ=="));
  EXPECT_EQ("ab", fromBase64("YWI="));
  EXPECT_EQ("abc", fromBase64("YWJj"));
  EXPECT_EQ("hello world", fromBase64("aGVsbG8gd29ybGQ="));
  EXPECT_EQ(
      "Hello World from Velox!",
      fromBase64("SGVsbG8gV29ybGQgZnJvbSBWZWxveCE="));

  EXPECT_THROW(fromBase64("YQ="), VeloxUserError);
  EXPECT_THROW(fromBase64("YQ==="), VeloxUserError);

  // Check encoded strings without padding
  EXPECT_EQ("a", fromBase64("YQ"));
  EXPECT_EQ("ab", fromBase64("YWI"));
  EXPECT_EQ("abcd", fromBase64("YWJjZA"));
}

TEST_F(BinaryFunctionsTest, fromBase64Url) {
  const auto fromHex = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("from_hex(cast(c0 as varchar))", value);
  };
  const auto fromBase64Url = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("from_base64url(c0)", value);
  };

  EXPECT_EQ(std::nullopt, fromBase64Url(std::nullopt));
  EXPECT_EQ("", fromBase64Url(""));
  EXPECT_EQ("a", fromBase64Url("YQ=="));
  EXPECT_EQ("a", fromBase64Url("YQ"));
  EXPECT_EQ("abc", fromBase64Url("YWJj"));
  EXPECT_EQ("hello world", fromBase64Url("aGVsbG8gd29ybGQ="));
  EXPECT_EQ(
      "Hello World from Velox!",
      fromBase64Url("SGVsbG8gV29ybGQgZnJvbSBWZWxveCE="));

  EXPECT_EQ(fromHex("FF4FBF50"), fromBase64Url("_0-_UA=="));
  // the encoded string input from base 64 url should be multiple of 4 and must
  // not contain invalid char like '+' and '/'
  EXPECT_THROW(fromBase64Url("YQ="), VeloxUserError);
  EXPECT_THROW(fromBase64Url("YQ==="), VeloxUserError);
  EXPECT_THROW(fromBase64Url("YQ=+"), VeloxUserError);
  EXPECT_THROW(fromBase64Url("YQ=/"), VeloxUserError);
}

TEST_F(BinaryFunctionsTest, fromBigEndian32) {
  const auto fromBigEndian32 = [&](const std::optional<std::string>& arg) {
    return evaluateOnce<int32_t>("from_big_endian_32(c0)", VARBINARY(), arg);
  };

  EXPECT_EQ(std::nullopt, fromBigEndian32(std::nullopt));
  EXPECT_THROW(fromBigEndian32(hexToDec("01")), VeloxUserError);
  EXPECT_THROW(fromBigEndian32(hexToDec("0000000000000001")), VeloxUserError);
  EXPECT_THROW(fromBigEndian32("123456789123456789"), VeloxUserError);
  EXPECT_THROW(fromBigEndian32("ABC-+/?"), VeloxUserError);

  EXPECT_EQ(0, fromBigEndian32(hexToDec("00000000")));
  EXPECT_EQ(1, fromBigEndian32(hexToDec("00000001")));
  EXPECT_EQ(-1, fromBigEndian32(hexToDec("FFFFFFFF")));
  EXPECT_EQ(12345678, fromBigEndian32(hexToDec("00BC614E")));
  EXPECT_EQ(-12345678, fromBigEndian32(hexToDec("FF439EB2")));
  // INT_MAX.
  EXPECT_EQ(2147483647, fromBigEndian32(hexToDec("7FFFFFFF")));
  // INT_MIN + 1.
  EXPECT_EQ(-2147483647, fromBigEndian32(hexToDec("80000001")));
  // INT_MIN.
  EXPECT_EQ(-2147483648, fromBigEndian32(hexToDec("80000000")));
  // INT overflow.
  EXPECT_EQ(-1, fromBigEndian32(hexToDec("FFFFFFFF")));
  EXPECT_EQ(-8, fromBigEndian32(hexToDec("FFFFFFF8")));
}

TEST_F(BinaryFunctionsTest, toBigEndian32) {
  const auto toBigEndian32 = [&](const std::optional<int32_t>& arg) {
    return evaluateOnce<std::string>("to_big_endian_32(c0)", arg);
  };

  EXPECT_EQ(std::nullopt, toBigEndian32(std::nullopt));

  EXPECT_EQ(hexToDec("00000000"), toBigEndian32(0));
  EXPECT_EQ(hexToDec("00000001"), toBigEndian32(1));
  EXPECT_EQ(hexToDec("FFFFFFFF"), toBigEndian32(-1));
  EXPECT_EQ(hexToDec("00BC614E"), toBigEndian32(12345678));
  EXPECT_EQ(hexToDec("FF439EB2"), toBigEndian32(-12345678));
  // INT_MAX.
  EXPECT_EQ(hexToDec("7FFFFFFF"), toBigEndian32(2147483647));
  // INT_MIN + 1.
  EXPECT_EQ(hexToDec("80000001"), toBigEndian32(-2147483647));
  // INT_MIN.
  EXPECT_EQ(hexToDec("80000000"), toBigEndian32(-2147483648));
}

TEST_F(BinaryFunctionsTest, fromBigEndian64) {
  const auto fromBigEndian64 = [&](const std::optional<std::string>& arg) {
    return evaluateOnce<int64_t>("from_big_endian_64(c0)", VARBINARY(), arg);
  };

  EXPECT_EQ(std::nullopt, fromBigEndian64(std::nullopt));
  EXPECT_THROW(fromBigEndian64(hexToDec("01")), VeloxUserError);
  EXPECT_THROW(fromBigEndian64(hexToDec("00BC614E")), VeloxUserError);
  EXPECT_THROW(fromBigEndian64(hexToDec("000000000000000001")), VeloxUserError);
  EXPECT_THROW(fromBigEndian64("123456789123456789"), VeloxUserError);
  EXPECT_THROW(fromBigEndian64("ABC-+/?"), VeloxUserError);

  EXPECT_EQ(0, fromBigEndian64(hexToDec("0000000000000000")));
  EXPECT_EQ(1, fromBigEndian64(hexToDec("0000000000000001")));
  EXPECT_EQ(-1, fromBigEndian64(hexToDec("FFFFFFFFFFFFFFFF")));
  EXPECT_EQ(12345678, fromBigEndian64(hexToDec("0000000000BC614E")));
  EXPECT_EQ(-12345678, fromBigEndian64(hexToDec("FFFFFFFFFF439EB2")));
  // INT_MAX.
  EXPECT_EQ(2147483647, fromBigEndian64(hexToDec("000000007FFFFFFF")));
  // INT_MIN + 1.
  EXPECT_EQ(-2147483647, fromBigEndian64(hexToDec("FFFFFFFF80000001")));
  // INT_MIN.
  EXPECT_EQ(-2147483648, fromBigEndian64(hexToDec("FFFFFFFF80000000")));
  // LONG_MAX.
  EXPECT_EQ(
      (int64_t)9223372036854775807,
      fromBigEndian64(hexToDec("7FFFFFFFFFFFFFFF")));
  // LONG_MIN + 1.
  EXPECT_EQ(
      (int64_t)-9223372036854775807,
      fromBigEndian64(hexToDec("8000000000000001")));
  // LONG_MIN.
  EXPECT_EQ(
      (int64_t)-9223372036854775807 - 1,
      fromBigEndian64(hexToDec("8000000000000000")));
  // LONG overflow.
  EXPECT_EQ(-1, fromBigEndian64(hexToDec("FFFFFFFFFFFFFFFF")));
  EXPECT_EQ(-8, fromBigEndian64(hexToDec("FFFFFFFFFFFFFFF8")));
}

TEST_F(BinaryFunctionsTest, toBigEndian64) {
  const auto toBigEndian64 = [&](const std::optional<int64_t>& arg) {
    return evaluateOnce<std::string>("to_big_endian_64(c0)", arg);
  };

  EXPECT_EQ(std::nullopt, toBigEndian64(std::nullopt));

  EXPECT_EQ(hexToDec("0000000000000000"), toBigEndian64(0));
  EXPECT_EQ(hexToDec("0000000000000001"), toBigEndian64(1));
  EXPECT_EQ(hexToDec("FFFFFFFFFFFFFFFF"), toBigEndian64(-1));
  EXPECT_EQ(hexToDec("0000000000BC614E"), toBigEndian64(12345678));
  EXPECT_EQ(hexToDec("FFFFFFFFFF439EB2"), toBigEndian64(-12345678));
  // INT_MAX.
  EXPECT_EQ(hexToDec("000000007FFFFFFF"), toBigEndian64(2147483647));
  // INT_MIN + 1.
  EXPECT_EQ(hexToDec("FFFFFFFF80000001"), toBigEndian64(-2147483647));
  // INT_MIN.
  EXPECT_EQ(hexToDec("FFFFFFFF80000000"), toBigEndian64(-2147483648));
  // LONG_MAX.
  EXPECT_EQ(
      hexToDec("7FFFFFFFFFFFFFFF"),
      toBigEndian64((int64_t)9223372036854775807));
  // LONG_MIN + 1.
  EXPECT_EQ(
      hexToDec("8000000000000001"),
      toBigEndian64((int64_t)-9223372036854775807));
  // LONG_MIN.
  EXPECT_EQ(
      hexToDec("8000000000000000"),
      toBigEndian64((int64_t)-9223372036854775807 - 1));
}

TEST_F(BinaryFunctionsTest, toIEEE754Bits64) {
  const auto toIEEE754Bits64 = [&](std::optional<double> value) {
    return evaluateOnce<std::string, double>("to_ieee754_64(c0)", value);
  };

  EXPECT_EQ(std::nullopt, toIEEE754Bits64(std::nullopt));
  EXPECT_EQ(hexToDec("0000000000000000"), toIEEE754Bits64(0.0));
  EXPECT_EQ(hexToDec("3FF0000000000000"), toIEEE754Bits64(1.0));
  EXPECT_EQ(hexToDec("8000000000000001"), toIEEE754Bits64(-5e-324));
  EXPECT_EQ(
      hexToDec("402499999999999a"),
      toIEEE754Bits64(1.03000000000000007105427357601E1));
  EXPECT_EQ(
      hexToDec("400921f9f01b866e"),
      toIEEE754Bits64(3.14158999999999988261834005243E0));
  EXPECT_EQ(
      hexToDec("3fb999999999999a"),
      toIEEE754Bits64(1.00000000000000005551115123126E-1));
  EXPECT_EQ(
      hexToDec("bfb999999999999a"),
      toIEEE754Bits64(-1.00000000000000005551115123126E-1));
  EXPECT_EQ(hexToDec("FFEFFFFC57CA82AE"), toIEEE754Bits64(-1.79769E+308));
  EXPECT_EQ(hexToDec("7FEFFFFC57CA82AE"), toIEEE754Bits64(1.79769E+308));
  EXPECT_EQ(hexToDec("0043FFD47E080F89"), toIEEE754Bits64(2.225E-307));
  // largest negative double value
  EXPECT_EQ(hexToDec("8043FFD47E080F89"), toIEEE754Bits64(-2.225E-307));
  EXPECT_EQ(
      hexToDec("0010000000000000"),
      toIEEE754Bits64(std::numeric_limits<double>::min()));
  EXPECT_EQ(
      hexToDec("7FEFFFFFFFFFFFFF"),
      toIEEE754Bits64(std::numeric_limits<double>::max()));
  EXPECT_EQ(
      hexToDec("7FF8000000000000"),
      toIEEE754Bits64(std::numeric_limits<double>::quiet_NaN()));
  EXPECT_EQ(
      hexToDec("7FF4000000000000"),
      toIEEE754Bits64(std::numeric_limits<double>::signaling_NaN()));
  EXPECT_EQ(
      hexToDec("FFF0000000000000"),
      toIEEE754Bits64(-std::numeric_limits<double>::infinity()));
  EXPECT_EQ(
      hexToDec("7FF0000000000000"),
      toIEEE754Bits64(std::numeric_limits<double>::infinity()));
}

TEST_F(BinaryFunctionsTest, fromIEEE754Bits64) {
  const auto fromIEEE754Bits64 = [&](const std::optional<std::string>& arg) {
    return evaluateOnce<double>("from_ieee754_64(c0)", VARBINARY(), arg);
  };

  const auto toIEEE754Bits64 = [&](std::optional<double> arg) {
    return evaluateOnce<std::string>("to_ieee754_64(c0)", arg);
  };

  EXPECT_EQ(std::nullopt, fromIEEE754Bits64(std::nullopt));
  EXPECT_EQ(1, fromIEEE754Bits64(hexToDec("3FF0000000000000")));
  EXPECT_EQ(1.0124, fromIEEE754Bits64(hexToDec("3FF032CA57A786C2")));
  EXPECT_EQ(
      -1.0123999999999715, fromIEEE754Bits64(hexToDec("BFF032CA57A78642")));
  EXPECT_EQ(3.1415926, fromIEEE754Bits64(hexToDec("400921fb4d12d84a")));
  EXPECT_EQ(
      std::numeric_limits<double>::infinity(),
      fromIEEE754Bits64(hexToDec("7ff0000000000000")));
  EXPECT_EQ(
      1.7976931348623157E308, fromIEEE754Bits64(hexToDec("7fefffffffffffff")));
  EXPECT_EQ(
      -1.7976931348623157E308, fromIEEE754Bits64(hexToDec("ffefffffffffffff")));
  EXPECT_EQ(4.9E-324, fromIEEE754Bits64(hexToDec("0000000000000001")));
  EXPECT_EQ(-4.9E-324, fromIEEE754Bits64(hexToDec("8000000000000001")));
  EXPECT_THROW(fromIEEE754Bits64("YQ"), VeloxUserError);
  EXPECT_EQ(3.1415926, fromIEEE754Bits64(toIEEE754Bits64(3.1415926)));
  EXPECT_EQ(4.9E-324, fromIEEE754Bits64(toIEEE754Bits64(4.9E-324)));
  EXPECT_EQ(-4.9E-324, fromIEEE754Bits64(toIEEE754Bits64(-4.9E-324)));
  EXPECT_EQ(
      std::numeric_limits<double>::infinity(),
      fromIEEE754Bits64(
          toIEEE754Bits64(std::numeric_limits<double>::infinity())));
}

TEST_F(BinaryFunctionsTest, toIEEE754Bits32) {
  const auto toIEEE754Bits32 = [&](std::optional<float> value) {
    return evaluateOnce<std::string, float>(
        "to_ieee754_32(cast(c0 as real))", value);
  };

  EXPECT_EQ(hexToDec("00000000"), toIEEE754Bits32(0.0));
  EXPECT_EQ(hexToDec("3f800000"), toIEEE754Bits32(1.0));
  EXPECT_EQ(hexToDec("40490FDA"), toIEEE754Bits32(3.1415926));
  EXPECT_EQ(hexToDec("7F800000"), toIEEE754Bits32(1.7976931348623157E308));
  EXPECT_EQ(hexToDec("FF800000"), toIEEE754Bits32(-1.7976931348623157E308));
  EXPECT_EQ(hexToDec("00000000"), toIEEE754Bits32(4.9E-324));
  EXPECT_EQ(hexToDec("80000000"), toIEEE754Bits32(-4.9E-324));
  EXPECT_EQ(toIEEE754Bits32(100.0), toIEEE754Bits32(100));
  EXPECT_EQ(std::nullopt, toIEEE754Bits32(std::nullopt));
  EXPECT_EQ(
      hexToDec("7FC00000"),
      toIEEE754Bits32(std::numeric_limits<float>::quiet_NaN()));
  EXPECT_EQ(
      hexToDec("7F800000"),
      toIEEE754Bits32(std::numeric_limits<float>::infinity()));
  EXPECT_EQ(
      hexToDec("FF800000"),
      toIEEE754Bits32(-std::numeric_limits<float>::infinity()));
  EXPECT_EQ(
      hexToDec("00800000"), toIEEE754Bits32(std::numeric_limits<float>::min()));
  EXPECT_EQ(
      hexToDec("7F7FFFFF"), toIEEE754Bits32(std::numeric_limits<float>::max()));
  EXPECT_EQ(
      hexToDec("FF7FFFFF"),
      toIEEE754Bits32(std::numeric_limits<float>::lowest()));
}

TEST_F(BinaryFunctionsTest, fromIEEE754Bits32) {
  const auto fromIEEE754Bits32 = [&](const std::optional<std::string>& arg) {
    return evaluateOnce<float>("from_ieee754_32(c0)", VARBINARY(), arg);
  };

  const auto toIEEE754Bits32 = [&](std::optional<float> arg) {
    return evaluateOnce<std::string>("to_ieee754_32(c0)", arg);
  };

  EXPECT_EQ(std::nullopt, fromIEEE754Bits32(std::nullopt));
  EXPECT_EQ(1.0f, fromIEEE754Bits32(hexToDec("3F800000")));
  EXPECT_EQ(3.14f, fromIEEE754Bits32(hexToDec("4048F5C3")));
  EXPECT_EQ(3.4028235E38f, fromIEEE754Bits32(hexToDec("7f7fffff")));
  EXPECT_EQ(-3.4028235E38f, fromIEEE754Bits32(hexToDec("ff7fffff")));
  EXPECT_EQ(1.4E-45f, fromIEEE754Bits32(hexToDec("00000001")));
  EXPECT_EQ(-1.4E-45f, fromIEEE754Bits32(hexToDec("80000001")));
  EXPECT_EQ(
      std::numeric_limits<float>::infinity(),
      fromIEEE754Bits32(hexToDec("7f800000")));
  EXPECT_EQ(
      -std::numeric_limits<float>::infinity(),
      fromIEEE754Bits32(hexToDec("ff800000")));
  EXPECT_THROW(fromIEEE754Bits32("YQ"), VeloxUserError);
  EXPECT_EQ(3.4028235E38f, fromIEEE754Bits32(toIEEE754Bits32(3.4028235E38f)));
  EXPECT_EQ(-3.4028235E38f, fromIEEE754Bits32(toIEEE754Bits32(-3.4028235E38f)));
  EXPECT_EQ(1.4E-45f, fromIEEE754Bits32(toIEEE754Bits32(1.4E-45f)));
  EXPECT_EQ(-1.4E-45f, fromIEEE754Bits32(toIEEE754Bits32(-1.4E-45f)));
  EXPECT_EQ(
      std::numeric_limits<float>::infinity(),
      fromIEEE754Bits32(
          toIEEE754Bits32(std::numeric_limits<float>::infinity())));
  EXPECT_EQ(
      -std::numeric_limits<float>::infinity(),
      fromIEEE754Bits32(
          toIEEE754Bits32(-std::numeric_limits<float>::infinity())));
  EXPECT_EQ(
      std::numeric_limits<float>::max(),
      fromIEEE754Bits32(toIEEE754Bits32(std::numeric_limits<float>::max())));
  EXPECT_EQ(
      std::numeric_limits<float>::min(),
      fromIEEE754Bits32(toIEEE754Bits32(std::numeric_limits<float>::min())));
  EXPECT_TRUE(
      std::isnan(fromIEEE754Bits32(
                     toIEEE754Bits32(std::numeric_limits<float>::quiet_NaN()))
                     .value()));
  EXPECT_TRUE(std::isnan(
      fromIEEE754Bits32(
          toIEEE754Bits32(std::numeric_limits<float>::signaling_NaN()))
          .value()));
  EXPECT_TRUE(
      std::isnan(fromIEEE754Bits32(toIEEE754Bits32(std::nan("nan"))).value()));
  VELOX_ASSERT_THROW(
      fromIEEE754Bits32(hexToDec("0000000000000001")),
      "Input floating-point value must be exactly 4 bytes long");
}
} // namespace
