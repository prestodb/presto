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
#include <gmock/gmock.h>
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using string_t = std::string;

namespace facebook::velox {

namespace {
class URLFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  void validate(
      const string_t& url,
      const string_t& expectedProtocol,
      const string_t& expectedHost,
      const string_t& expectedPath,
      const string_t& expectedFragment,
      const string_t& expectedQuery,
      const std::optional<int32_t> expectedPort) {
    const auto extractFn = [&](const string_t& fn,
                               const std::optional<string_t>& a) {
      return evaluateOnce<string_t>(fmt::format("url_extract_{}(c0)", fn), a);
    };

    const auto extractPort = [&](const std::optional<string_t>& a) {
      return evaluateOnce<int64_t>("url_extract_port(c0)", a);
    };

    EXPECT_EQ(extractFn("protocol", url), expectedProtocol);
    EXPECT_EQ(extractFn("host", url).value(), expectedHost);
    EXPECT_EQ(extractFn("path", url), expectedPath);
    EXPECT_EQ(extractFn("fragment", url), expectedFragment);
    EXPECT_EQ(extractFn("query", url), expectedQuery);
    EXPECT_EQ(extractPort(url), expectedPort);
  }
};

TEST_F(URLFunctionsTest, validateURL) {
  validate(
      "http://example.com/path1/p.php?k1=v1&k2=v2#Ref1",
      "http",
      "example.com",
      "/path1/p.php",
      "Ref1",
      "k1=v1&k2=v2",
      std::nullopt);
  validate(
      "http://example.com/path1/p.php?",
      "http",
      "example.com",
      "/path1/p.php",
      "",
      "",
      std::nullopt);
  validate(
      "HTTP://example.com/path1/p.php?",
      "HTTP",
      "example.com",
      "/path1/p.php",
      "",
      "",
      std::nullopt);
  validate(
      "http://example.com:8080/path1/p.php?k1=v1&k2=v2#Ref1",
      "http",
      "example.com",
      "/path1/p.php",
      "Ref1",
      "k1=v1&k2=v2",
      8080);
  validate(
      "https://username@example.com",
      "https",
      "example.com",
      "",
      "",
      "",
      std::nullopt);
  validate(
      "https:/auth/login.html",
      "https",
      "",
      "/auth/login.html",
      "",
      "",
      std::nullopt);
  validate("foo", "", "", "", "", "", std::nullopt);
}

TEST_F(URLFunctionsTest, extractParameter) {
  const auto extractParam = [&](const std::optional<std::string>& a,
                                const std::optional<std::string>& b) {
    return evaluateOnce<std::string>("url_extract_parameter(c0, c1)", a, b);
  };

  EXPECT_EQ(
      extractParam("http://example.com/path1/p.php?k1=v1&k2=v2#Ref1", "k2"),
      "v2");
  EXPECT_EQ(
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1", "k1"),
      "v1");
  EXPECT_EQ(
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1", "k3"),
      "");
  EXPECT_EQ(
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1", "k6"),
      std::nullopt);
  EXPECT_EQ(extractParam("foo", ""), std::nullopt);
}

TEST_F(URLFunctionsTest, urlEncode) {
  const auto urlEncode = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("url_encode(c0)", value);
  };

  EXPECT_EQ(std::nullopt, urlEncode(std::nullopt));
  EXPECT_EQ("", urlEncode(""));
  EXPECT_EQ("http%3A%2F%2Ftest", urlEncode("http://test"));
  EXPECT_EQ(
      "http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd", urlEncode("http://test?a=b&c=d"));
  EXPECT_EQ(
      "http%3A%2F%2F%E3%83%86%E3%82%B9%E3%83%88",
      urlEncode("http://\u30c6\u30b9\u30c8"));
  EXPECT_EQ("%7E%40%3A.-*_%2B+%E2%98%83", urlEncode("~@:.-*_+ \u2603"));
  EXPECT_EQ("test", urlEncode("test"));
}

TEST_F(URLFunctionsTest, urlDecode) {
  const auto urlDecode = [&](std::optional<std::string> value) {
    return evaluateOnce<std::string>("url_decode(c0)", value);
  };

  EXPECT_EQ(std::nullopt, urlDecode(std::nullopt));
  EXPECT_EQ("", urlDecode(""));
  EXPECT_EQ("http://test", urlDecode("http%3A%2F%2Ftest"));
  EXPECT_EQ(
      "http://test?a=b&c=d", urlDecode("http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd"));
  EXPECT_EQ(
      "http://\u30c6\u30b9\u30c8",
      urlDecode("http%3A%2F%2F%E3%83%86%E3%82%B9%E3%83%88"));
  EXPECT_EQ("~@:.-*_+ \u2603", urlDecode("%7E%40%3A.-*_%2B+%E2%98%83"));
  EXPECT_EQ("test", urlDecode("test"));

  EXPECT_THROW(urlDecode("http%3A%2F%2"), VeloxUserError);
  EXPECT_THROW(urlDecode("http%3A%2F%"), VeloxUserError);
  EXPECT_THROW(urlDecode("http%3A%2F%2H"), VeloxUserError);
}

} // namespace
} // namespace facebook::velox
