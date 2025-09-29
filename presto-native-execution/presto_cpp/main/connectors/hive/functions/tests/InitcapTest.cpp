/*
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
#include <gtest/gtest.h>

#include "presto_cpp/main/connectors/hive/functions/HiveFunctionRegistration.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::presto::functions::test {
class InitcapTest : public velox::functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    velox::functions::test::FunctionBaseTest::SetUpTestCase();
    facebook::presto::hive::functions::registerHiveNativeFunctions();
  }
};

TEST_F(InitcapTest, initcap) {
  const auto initcap = [&](const std::optional<std::string>& value) {
    return evaluateOnce<std::string>("\"hive.default.initcap\"(c0)", value);
  };

  // Unicode only.
  EXPECT_EQ(
      initcap("àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ"),
      "Àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ");
  EXPECT_EQ(initcap("αβγδεζηθικλμνξοπρςστυφχψ"), "Αβγδεζηθικλμνξοπρςστυφχψ");
  // Mix of ascii and unicode.
  EXPECT_EQ(initcap("αβγδεζ world"), "Αβγδεζ World");
  EXPECT_EQ(initcap("αfoo wβ"), "Αfoo Wβ");
  // Ascii only.
  EXPECT_EQ(initcap("hello world"), "Hello World");
  EXPECT_EQ(initcap("HELLO WORLD"), "Hello World");
  EXPECT_EQ(initcap("1234"), "1234");
  EXPECT_EQ(initcap("a b c d"), "A B C D");
  EXPECT_EQ(initcap("abcd"), "Abcd");
  // Numbers.
  EXPECT_EQ(initcap("123"), "123");
  EXPECT_EQ(initcap("1abc"), "1abc");
  // Edge cases.
  EXPECT_EQ(initcap(""), "");
  EXPECT_EQ(initcap(std::nullopt), std::nullopt);

  // Test with various whitespace characters
  EXPECT_EQ(initcap("YQ\tY"), "Yq\tY");
  EXPECT_EQ(initcap("YQ\nY"), "Yq\nY");
  EXPECT_EQ(initcap("YQ\rY"), "Yq\rY");
  EXPECT_EQ(initcap("hello\tworld\ntest"), "Hello\tWorld\nTest");
  EXPECT_EQ(initcap("foo\r\nbar"), "Foo\r\nBar");

  // Test with multiple consecutive whitespaces
  EXPECT_EQ(initcap("hello  world"), "Hello  World");
  EXPECT_EQ(initcap("a   b   c"), "A   B   C");
  EXPECT_EQ(initcap("test\t\tvalue"), "Test\t\tValue");
  EXPECT_EQ(initcap("line\n\n\nbreak"), "Line\n\n\nBreak");

  // Test with leading and trailing whitespaces
  EXPECT_EQ(initcap("  hello"), "  Hello");
  EXPECT_EQ(initcap("world  "), "World  ");
  EXPECT_EQ(initcap("  spaces  "), "  Spaces  ");
  EXPECT_EQ(initcap("\thello"), "\tHello");
  EXPECT_EQ(initcap("\nworld"), "\nWorld");
  EXPECT_EQ(initcap("test\n"), "Test\n");

  // Test with mixed whitespace types
  EXPECT_EQ(initcap("hello \t\nworld"), "Hello \t\nWorld");
  EXPECT_EQ(initcap("a\tb\nc\rd"), "A\tB\nC\rD");
  EXPECT_EQ(initcap(" \t\n "), " \t\n ");
}
} // namespace facebook::presto::functions::test
