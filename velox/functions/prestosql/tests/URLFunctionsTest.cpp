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

namespace facebook::velox {

namespace {
class URLFunctionsTest : public functions::test::FunctionBaseTest {
 protected:
  void validate(
      const std::string& url,
      const std::optional<std::string>& expectedProtocol,
      const std::optional<std::string>& expectedHost,
      const std::optional<std::string>& expectedPath,
      const std::optional<std::string>& expectedFragment,
      const std::optional<std::string>& expectedQuery,
      const std::optional<int32_t> expectedPort) {
    const auto extractFn = [&](const std::string& fn,
                               const std::optional<std::string>& a) {
      return evaluateOnce<std::string>(
          fmt::format("url_extract_{}(c0)", fn), a);
    };

    const auto extractPort = [&](const std::optional<std::string>& a) {
      return evaluateOnce<int64_t>("url_extract_port(c0)", a);
    };

    EXPECT_EQ(extractFn("protocol", url), expectedProtocol);
    EXPECT_EQ(extractFn("host", url), expectedHost);
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
  validate(
      "https://www.ucu.edu.uy/agenda/evento/%%UCUrlCompartir%%",
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt);
  validate("foo", "", "", "foo", "", "", std::nullopt);
  validate(
      "foo ",
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt);
  validate(
      "IC6S!8hGVRpo+!,yTaJEy/$RUZpqcr",
      "",
      "",
      "IC6S!8hGVRpo+!,yTaJEy/$RUZpqcr",
      "",
      "",
      std::nullopt);

  // Some examples from Wikipedia.
  // https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
  validate(
      "https://john.doe@www.example.com:1234/forum/questions/?tag=networking&order=newest#top",
      "https",
      "www.example.com",
      "/forum/questions/",
      "top",
      "tag=networking&order=newest",
      1234);
  validate(
      "https://john.doe@www.example.com:1234/forum/questions/?tag=networking&order=newest#:~:text=whatever",
      "https",
      "www.example.com",
      "/forum/questions/",
      ":~:text=whatever",
      "tag=networking&order=newest",
      1234);
  validate(
      "ldap://[2001:db8::7]/c=GB?objectClass?one",
      "ldap",
      "[2001:db8::7]",
      "/c=GB",
      "",
      "objectClass?one",
      std::nullopt);
  validate(
      "mailto:John.Doe@example.com",
      "mailto",
      "",
      "John.Doe@example.com",
      "",
      "",
      std::nullopt);
  validate(
      "news:comp.infosystems.www.servers.unix",
      "news",
      "",
      "comp.infosystems.www.servers.unix",
      "",
      "",
      std::nullopt);
  validate(
      "tel:+1-816-555-1212",
      "tel",
      "",
      "+1-816-555-1212",
      "",
      "",
      std::nullopt);
  validate("telnet://192.0.2.16:80/", "telnet", "192.0.2.16", "/", "", "", 80);
  validate(
      "urn:oasis:names:specification:docbook:dtd:xml:4.1.2",
      "urn",
      "",
      "oasis:names:specification:docbook:dtd:xml:4.1.2",
      "",
      "",
      std::nullopt);
}

TEST_F(URLFunctionsTest, extractProtocol) {
  const auto extractProtocol = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_protocol(c0)", url);
  };

  // Test minimal protocol.
  EXPECT_EQ("a", extractProtocol("a://www.yahoo.com"));
  // Test all valid characters
  EXPECT_EQ(
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+-.",
      extractProtocol(
          "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+-.://www.yahoo.com"));

  // Test empty protocol.
  EXPECT_EQ(std::nullopt, extractProtocol("://www.yahoo.com/"));
  // Test protocol starts with digit.
  EXPECT_EQ(std::nullopt, extractProtocol("1abc://www.yahoo.com/"));
}

TEST_F(URLFunctionsTest, extractHostIPv4) {
  const auto extractHost = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_host(c0)", url);
  };

  // Upper bounds of the ranges of the rules for IPv4 dec-octets.
  EXPECT_EQ("255.249.199.99", extractHost("http://255.249.199.99"));
  // Lower bounds of the ranges of the rules for IPv4 dec-octets.
  EXPECT_EQ("250.200.100.10", extractHost("http://250.200.100.10"));
  // All single digits.
  EXPECT_EQ("9.8.1.0", extractHost("http://9.8.1.0"));
  // All two digits.
  EXPECT_EQ("99.98.11.10", extractHost("http://99.98.11.10"));
  // All three digits.
  EXPECT_EQ("254.237.150.100", extractHost("http://254.237.150.100"));

  // We don't test invalid cases here as they will match the reg-name rule, we
  // test them under the IPv6 cases below as these are distinguishable from
  // reg-name.
}

TEST_F(URLFunctionsTest, extractHostIPv6) {
  const auto extractHost = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_host(c0)", url);
  };

  // 8 hex blocks.
  EXPECT_EQ(
      "[0123:4567:89ab:cdef:0123:4567:89ab:cdef]",
      extractHost("http://[0123:4567:89ab:cdef:0123:4567:89ab:cdef]"));
  // 6 hex blocks followed by an IPv4 address.
  EXPECT_EQ(
      "[0123:4567:89ab:cdef:0123:4567:0.1.8.9]",
      extractHost("http://[0123:4567:89ab:cdef:0123:4567:0.1.8.9]"));
  // compression followed by 7 hex blocks.
  EXPECT_EQ(
      "[::456:89a:cde:012:456:89a:cde]",
      extractHost("http://[::456:89a:cde:012:456:89a:cde]"));
  // compression followed by 5 hex blocks followed by an IPv4 address.
  EXPECT_EQ(
      "[::456:89a:cde:012:456:10.11.98.99]",
      extractHost("http://[::456:89a:cde:012:456:10.11.98.99]"));
  // 1 hex block flowed by a compression followed by 6 hex blocks.
  EXPECT_EQ(
      "[12::45:89:cd:01:45:89]", extractHost("http://[12::45:89:cd:01:45:89]"));
  // 1 hex block flowed by a compression followed by 4 hex blocks followed by an
  // IPv4 address.
  EXPECT_EQ(
      "[12::45:89:cd:01:254.237.150.100]",
      extractHost("http://[12::45:89:cd:01:254.237.150.100]"));
  // 7 hex blocks followed by a compression.
  EXPECT_EQ("[0:4:8:c:0:4:8::]", extractHost("http://[0:4:8:c:0:4:8::]"));
  // 5 hex blocks followed by a compression followed by an IPv4 address.
  EXPECT_EQ(
      "[0:4:8:c:0::255.249.199.99]",
      extractHost("http://[0:4:8:c:0::255.249.199.99]"));
  // Compression followed by an IPv4 address.
  EXPECT_EQ("[::250.200.100.10]", extractHost("http://[::250.200.100.10]"));
  // Just a compression.
  EXPECT_EQ("[::]", extractHost("http://[::]"));

  // Too many hex blocks.
  EXPECT_EQ(
      std::nullopt,
      extractHost("http://[0123:4567:89ab:cdef:0123:4567:89ab:cdef:0123]"));
  // Too many hex blocks with a compression.
  EXPECT_EQ(
      std::nullopt,
      extractHost("http://[0123:4567:89ab:cdef:0123:4567:89ab::cdef]"));
  // Too many hex blocks with an IPv4 address.
  EXPECT_EQ(
      std::nullopt,
      extractHost(
          "http://[0123:4567:89ab:cdef:0123:4567:89ab:250.200.100.10]"));
  // Too few hex blocks.
  EXPECT_EQ(
      std::nullopt, extractHost("http://[0123:4567:89ab:cdef:0123:4567:89ab]"));
  // Too few hex blocks with an IPv4 address.
  EXPECT_EQ(
      std::nullopt,
      extractHost("http://[0123:4567:89ab:cdef:0123:250.200.100.10]"));
  // End on a colon.
  EXPECT_EQ(
      std::nullopt,
      extractHost("http://[0123:4567:89ab:cdef:0123:4567:89ab:cdef:]"));
  // Hex blocks after an IPv4 address.
  EXPECT_EQ(
      std::nullopt,
      extractHost("http://[0123:4567:89ab:cdef:250.200.100.10:89ab:cdef]"));
  // Compression after an IPv4 address.
  EXPECT_EQ(
      std::nullopt,
      extractHost("http://[0123:4567:89ab:cdef:250.200.100.10::]"));
  // Two compressions.
  EXPECT_EQ(std::nullopt, extractHost("http://[0123::4567::]"));
  EXPECT_EQ(std::nullopt, extractHost("http://[::0123::]"));
  // IPv6 address doesn't end with ']'.
  EXPECT_EQ(std::nullopt, extractHost("http://[0123:4567:89ab::cdef:0123"));

  // Invalid IPv4 addresses.
  // Too many dec-octets.
  EXPECT_EQ(std::nullopt, extractHost("http://[::255.249.199.99.10]"));
  // Too few dec-octets.
  EXPECT_EQ(std::nullopt, extractHost("http://[::250.200.100]"));
  // Dec-octets outside of range
  EXPECT_EQ(std::nullopt, extractHost("http://[::256.8.1.0]"));
  // Negative dec-octet.
  EXPECT_EQ(std::nullopt, extractHost("http://[::99.98.-11.10]"));
  // Hex in dec-octet.
  EXPECT_EQ(std::nullopt, extractHost("http://[::254.dae.150.100]"));
}

TEST_F(URLFunctionsTest, extractHostIPvFuture) {
  const auto extractHost = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_host(c0)", url);
  };

  // Test minimal.
  EXPECT_EQ("[v0.a]", extractHost("http://[v0.a]"));
  // Test all valid characters.
  EXPECT_EQ(
      "[v0123456789abcdefABCDEF.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:]",
      extractHost(
          "http://[v0123456789abcdefABCDEF.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:]"));

  // Missing v.
  EXPECT_EQ(std::nullopt, extractHost("http://[0.a]"));
  // Empty hex string.
  EXPECT_EQ(std::nullopt, extractHost("http://[v.a]"));
  // Invalid hex character.
  EXPECT_EQ(std::nullopt, extractHost("http://[v0g.a]"));
  // Missing period.
  EXPECT_EQ(std::nullopt, extractHost("http://[v0a]"));
  // Empty suffix.
  EXPECT_EQ(std::nullopt, extractHost("http://[v0.]"));
  // Invalid character in suffix.
  EXPECT_EQ(std::nullopt, extractHost("http://[v0.a/]"));
  // IPvFuture address doesn't end with ']'.
  EXPECT_EQ(std::nullopt, extractHost("http://[v0.a"));
}

TEST_F(URLFunctionsTest, extractHostRegName) {
  const auto extractHost = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_host(c0)", url);
  };

  // Test minimal.
  EXPECT_EQ("a", extractHost("http://a"));
  // Test all valid ASCII characters.
  EXPECT_EQ(
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=",
      extractHost(
          "http://abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;="));
  // Test prefix is valid IPv4 address.
  EXPECT_EQ(
      "123.456.789.012.abcdefg", extractHost("http://123.456.789.012.abcdefg"));
  // Test percent encoded.
  EXPECT_EQ("a b", extractHost("http://a%20b"));
  // Valid UTF-8 in host reg name.
  EXPECT_EQ("你好", extractHost("https://你好"));
  // Valid UTF-8 in userinfo.
  EXPECT_EQ("foo", extractHost("https://你好@foo"));

  // Invalid ASCII character.
  EXPECT_EQ(std::nullopt, extractHost("http://a b"));
  // Inalid UTF-8 in host reg name (it should be a 3 byte character but there's
  // only 2 bytes).
  EXPECT_EQ(std::nullopt, extractHost("https://\xe0\xb8"));
  // Inalid UTF-8 in userinfo (it should be a 3 byte character but there's only
  // 2 bytes).
  EXPECT_EQ(std::nullopt, extractHost("https://\xe0\xb8@foo"));
  // Valid UTF-8 in host reg name but character is not allowed (it's a control
  // character).
  EXPECT_EQ(std::nullopt, extractHost("https://\x82"));
  // Valid UTF-8 in userinfo but character is not allowed (it's a control
  // character).
  EXPECT_EQ(std::nullopt, extractHost("https://\x82@foo"));
  // Valid UTF-8 in host reg name but character is not allowed (it's white
  // space: THREE-PER-EM SPACE).
  EXPECT_EQ(std::nullopt, extractHost("https://\xe2\x80\x84"));
  // Valid UTF-8 in userinfo but character is not allowed (it's white space:
  // THREE-PER-EM SPACE).
  EXPECT_EQ(std::nullopt, extractHost("https://\xe2\x80\x84@foo"));
}

TEST_F(URLFunctionsTest, extractPath) {
  const auto extractPath = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_path(c0)", url);
  };

  EXPECT_EQ(
      "/media/set/Books and Magazines.php",
      extractPath(
          "https://www.cnn.com/media/set/Books%20and%20Magazines.php?foo=bar"));

  EXPECT_EQ(
      "java-net@java.sun.com", extractPath("mailto:java-net@java.sun.com"));
  EXPECT_EQ(
      std::nullopt,
      extractPath("https://www.ucu.edu.uy/agenda/evento/%%UCUrlCompartir%%"));
  EXPECT_EQ("foo", extractPath("foo"));
  EXPECT_EQ(std::nullopt, extractPath("BAD URL!"));
  EXPECT_EQ("", extractPath("http://www.yahoo.com"));
  // All valid ASCII characters.
  EXPECT_EQ(
      "/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@",
      extractPath(
          "/abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@"));
  // Valid UTF-8 in path.
  EXPECT_EQ("/你好", extractPath("https://foo.com/你好"));
  // Inalid UTF-8 in path (it should be a 3 byte character but there's only 2
  // bytes).
  EXPECT_EQ(std::nullopt, extractPath("https://foo.com/\xe0\xb8"));
  // Valid UTF-8 but character is not allowed (it's a control character).
  EXPECT_EQ(std::nullopt, extractPath("https://foo.com/\xc2\x82"));
  // Valid UTF-8 but character is not allowed (it's white space: THREE-PER-EM
  // SPACE).
  EXPECT_EQ(std::nullopt, extractPath("https://foo.com/\xe2\x80\x84"));
}

TEST_F(URLFunctionsTest, extractPort) {
  const auto extractPort = [&](const std::optional<std::string>& url) {
    return evaluateOnce<int64_t>("url_extract_port(c0)", url);
  };

  // 0-4 valid.
  EXPECT_EQ(43210, extractPort("http://a:43210"));
  // 5-9 valid.
  EXPECT_EQ(98765, extractPort("http://a:98765"));

  // Empty port.
  EXPECT_EQ(std::nullopt, extractPort("http://a:"));
  // Hex invalid.
  EXPECT_EQ(std::nullopt, extractPort("http://a:deadbeef"));
}

TEST_F(URLFunctionsTest, extractHostWithUserInfo) {
  const auto extractHost = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_host(c0)", url);
  };

  // Test extracting a host when user info is present.

  // Test empty user info.
  EXPECT_EQ("a", extractHost("http://@a"));
  // Test all valid characters.
  EXPECT_EQ(
      "a",
      extractHost(
          "http://abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:%20@a"));
  // Test with user info and port present.
  EXPECT_EQ("a", extractHost("http://xyz@a:123"));
}

TEST_F(URLFunctionsTest, extractQuery) {
  const auto extractQuery = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_query(c0)", url);
  };

  // Test empty query.
  EXPECT_EQ("", extractQuery("http://www.yahoo.com?"));
  // Test non-empty query.
  EXPECT_EQ("a", extractQuery("http://www.yahoo.com?a"));
  // Test all valid ASCII characters.
  EXPECT_EQ(
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@ []",
      extractQuery(
          "http://www.yahoo.com?abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@%20[]"));
  // Valid UTF-8 in query.
  EXPECT_EQ("你好", extractQuery("https://foo.com?你好"));
  // Inalid UTF-8 in query (it should be a 3 byte character but there's only 2
  // bytes).
  EXPECT_EQ(std::nullopt, extractQuery("https://foo.com?\xe0\xb8"));
  // Valid UTF-8 but character is not allowed (it's a control character).
  EXPECT_EQ(std::nullopt, extractQuery("https://foo.com?\xc2\x82"));
  // Valid UTF-8 but character is not allowed (it's white space: THREE-PER-EM
  // SPACE).
  EXPECT_EQ(std::nullopt, extractQuery("https://foo.com?\xe2\x80\x84"));
}

TEST_F(URLFunctionsTest, extractFragment) {
  const auto extractFragment = [&](const std::optional<std::string>& url) {
    return evaluateOnce<std::string>("url_extract_fragment(c0)", url);
  };

  // Test empty fragment.
  EXPECT_EQ("", extractFragment("http://www.yahoo.com#"));
  // Test non-empty fragment.
  EXPECT_EQ("a", extractFragment("http://www.yahoo.com#a"));
  // Test all valid ASCII characters.
  EXPECT_EQ(
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@ []",
      extractFragment(
          "http://www.yahoo.com#abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@%20[]"));
  // Valid UTF-8 in fgrament.
  EXPECT_EQ("你好", extractFragment("https://foo.com#你好"));
  // Inalid UTF-8 in fragment (it should be a 3 byte character but there's only
  // 2 bytes).
  EXPECT_EQ(std::nullopt, extractFragment("https://foo.com#\xe0\xb8"));
  // Valid UTF-8 but character is not allowed (it's a control character).
  EXPECT_EQ(std::nullopt, extractFragment("https://foo.com#\xc2\x82"));
  // Valid UTF-8 but character is not allowed (it's white space: THREE-PER-EM
  // SPACE).
  EXPECT_EQ(std::nullopt, extractFragment("https://foo.com#\xe2\x80\x84"));
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
  // Tests unescaping functionality.
  EXPECT_EQ(
      extractParam("http://example.com/path1/p.php?k1=v1%2Bv2#Ref1", "k1"),
      "v1+v2");
  EXPECT_EQ(
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1", "k3"),
      "");
  EXPECT_EQ(
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1", "k6"),
      std::nullopt);
  EXPECT_EQ(extractParam("foo", ""), std::nullopt);
  EXPECT_EQ(
      "",
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3%26k4=v4", "k3"));
  EXPECT_EQ(
      "v3",
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3=v3%26k4=v4", "k3"));
  EXPECT_EQ(
      "v3",
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3%3Dv3%26k4=v4", "k3"));
  // Test "=" inside a parameter value.
  EXPECT_EQ(
      "v3.1=v3.2",
      extractParam(
          "http://example.com/path1/p.php?k1=v1&k2=v2&k3%3Dv3.1%3Dv3.2%26k4=v4",
          "k3"));
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
  // Test a single byte invalid UTF-8 character.
  EXPECT_EQ("te%EF%BF%BDst", urlEncode("te\x88st"));
  // Test a multi-byte invalid UTF-8 character. (If the first byte is between
  // 0xe0 and 0xef, it should be a 3 byte character, but we only have 2 bytes
  // here.)
  EXPECT_EQ("te%EF%BF%BDst", urlEncode("te\xe0\xb8st"));
  // Test an overlong 3 byte UTF-8 character
  EXPECT_EQ("%EF%BF%BD%EF%BF%BD", urlEncode("\xe0\x94"));
  // Test an overlong 3 byte UTF-8 character with a continuation byte.
  EXPECT_EQ("%EF%BF%BD%EF%BF%BD%EF%BF%BD", urlEncode("\xe0\x94\x83"));
  // Test an overlong 4 byte UTF-8 character
  EXPECT_EQ("%EF%BF%BD%EF%BF%BD", urlEncode("\xf0\x84"));
  // Test an overlong 4 byte UTF-8 character with continuation bytes.
  EXPECT_EQ(
      "%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD", urlEncode("\xf0\x84\x90\x90"));
  // Test a 4 byte UTF-8 character outside the range of valid values.
  EXPECT_EQ(
      "%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD", urlEncode("\xfa\x80\x80\x80"));
  // Test the beginning of a 4 byte UTF-8 character followed by a
  // non-continuation byte.
  EXPECT_EQ("%EF%BF%BD%EF%BF%BD", urlEncode("\xf0\xe0"));
  // Test the invalid byte 0xc0.
  EXPECT_EQ("%EF%BF%BD%EF%BF%BD", urlEncode("\xc0\x83"));
  // Test the invalid byte 0xc1.
  EXPECT_EQ("%EF%BF%BD%EF%BF%BD", urlEncode("\xc1\x83"));
  // Test a 4 byte UTF-8 character that looks valid, but is actually outside the
  // range of valid values.
  EXPECT_EQ(
      "%EF%BF%BD%EF%BF%BD%EF%BF%BD%EF%BF%BD", urlEncode("\xf4\x92\x83\x83"));
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
  // Test a single byte invalid UTF-8 character.
  EXPECT_EQ("te\xef\xbf\xbdst", urlDecode("te%88st"));
  // Test a multi-byte invalid UTF-8 character. (If the first byte is between
  // 0xe0 and 0xef, it should be a 3 byte character, but we only have 2 bytes
  // here.)
  EXPECT_EQ("te\xef\xbf\xbdst", urlDecode("te%e0%b8st"));
  // Test an overlong 3 byte UTF-8 character
  EXPECT_EQ("\xef\xbf\xbd\xef\xbf\xbd", urlDecode("%e0%94"));
  // Test an overlong 3 byte UTF-8 character with a continuation byte.
  EXPECT_EQ("\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd", urlDecode("%e0%94%83"));
  // Test an overlong 4 byte UTF-8 character
  EXPECT_EQ("\xef\xbf\xbd\xef\xbf\xbd", urlDecode("%f0%84"));
  // Test an overlong 4 byte UTF-8 character with continuation bytes.
  EXPECT_EQ(
      "\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd",
      urlDecode("%f0%84%90%90"));
  // Test a 4 byte UTF-8 character outside the range of valid values.
  EXPECT_EQ(
      "\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd",
      urlDecode("%fa%80%80%80"));
  // Test the beginning of a 4 byte UTF-8 character followed by a
  // non-continuation byte.
  EXPECT_EQ("\xef\xbf\xbd\xef\xbf\xbd", urlDecode("%f0%e0"));
  // Test the invalid byte 0xc0.
  EXPECT_EQ("\xef\xbf\xbd\xef\xbf\xbd", urlDecode("%c0%83"));
  // Test the invalid byte 0xc1.
  EXPECT_EQ("\xef\xbf\xbd\xef\xbf\xbd", urlDecode("%c1%83"));
  // Test a 4 byte UTF-8 character that looks valid, but is actually outside the
  // range of valid values.
  EXPECT_EQ(
      "\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd\xef\xbf\xbd",
      urlDecode("%f4%92%83%83"));

  EXPECT_THROW(urlDecode("http%3A%2F%2"), VeloxUserError);
  EXPECT_THROW(urlDecode("http%3A%2F%"), VeloxUserError);
  EXPECT_THROW(urlDecode("http%3A%2F%2H"), VeloxUserError);
  EXPECT_THROW(urlDecode("%-1"), VeloxUserError);
  EXPECT_THROW(urlDecode("% 1"), VeloxUserError);
  EXPECT_THROW(urlDecode("%1 "), VeloxUserError);
}

} // namespace
} // namespace facebook::velox
