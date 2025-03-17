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
#include <velox/common/base/VeloxException.h>
#include <velox/vector/SimpleVector.h>
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using facebook::velox::functions::test::FunctionBaseTest;

namespace facebook::velox {
namespace {

class ParsePrestoDataSizeTest : public FunctionBaseTest {
 protected:
  void assertVectors(
      const std::string& expression,
      const VectorPtr& arg0,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector({arg0}));
    test::assertEqualVectors(expected, result);
  }

  void assertSize(std::string input, const std::string& outputStr) {
    auto op = makeNullableFlatVector<std::string>({input});
    int128_t outputInt = facebook::velox::HugeInt::parse(outputStr);
    auto expected =
        makeNullableFlatVector<int128_t>({outputInt}, DECIMAL(38, 0));
    assertVectors("parse_presto_data_size(c0)", op, expected);
  }

  template <typename T, typename U = T, typename V = T>
  void assertError(
      const std::string& expression,
      const std::vector<T>& arg0,
      const std::vector<U>& arg1,
      const std::string& errorMessage) {
    auto vector0 = makeFlatVector(arg0);
    auto vector1 = makeFlatVector(arg1);
    try {
      evaluate<SimpleVector<V>>(expression, makeRowVector({vector0, vector1}));
      ASSERT_TRUE(false) << "Expected an error";
    } catch (const std::exception& e) {
      ASSERT_TRUE(
          std::string(e.what()).find(errorMessage) != std::string::npos);
    }
  }
};

TEST_F(ParsePrestoDataSizeTest, success) {
  assertSize("0B", "0");
  assertSize("1B", "1");
  assertSize("1.2B", "1");
  assertSize("1.9B", "1");
  assertSize("2.2kB", "2252");
  assertSize("2.23kB", "2283");
  assertSize("2.234kB", "2287");
  assertSize("3MB", "3145728");
  assertSize("4GB", "4294967296");
  assertSize("4TB", "4398046511104");
  assertSize("5PB", "5629499534213120");
  assertSize("6EB", "6917529027641081856");
  assertSize("8YB", "9671406556917033397649408");
  assertSize("7ZB", "8264141345021879123968");
  assertSize("8YB", "9671406556917033397649408");
  assertSize("6917529027641081856EB", "7975367974709495237422842361682067456");
  assertSize(
      "69175290276410818560EB", "79753679747094952374228423616820674560");
}

TEST_F(ParsePrestoDataSizeTest, failures) {
  assertError<std::string>(
      "parse_presto_data_size(c0)", {""}, {}, "Invalid data size: ''");
  assertError<std::string>(
      "parse_presto_data_size(c0)", {"0"}, {}, "Invalid data size: '0'");
  assertError<std::string>(
      "parse_presto_data_size(c0)", {"10KB"}, {}, "Invalid data size: '10KB'");
  assertError<std::string>(
      "parse_presto_data_size(c0)", {"KB"}, {}, "Invalid data size: 'KB'");
  assertError<std::string>(
      "parse_presto_data_size(c0)", {"-1B"}, {}, "Invalid data size: '-1B'");
  assertError<std::string>(
      "parse_presto_data_size(c0)",
      {"12345K"},
      {},
      "Invalid data size: '12345K'");
  assertError<std::string>(
      "parse_presto_data_size(c0)",
      {"A12345B"},
      {},
      "Invalid data size: 'A12345B'");
  assertError<std::string>(
      "parse_presto_data_size(c0)",
      {"99999999999999YB"},
      {},
      "Value out of range: '99999999999999YB' ('120892581961461708544797985370825293824B')");
  // Throw error with non-ASCII value
  assertError<std::string>(
      "parse_presto_data_size(c0)",
      {"١٢٣1MB"},
      {},
      "Invalid data size: '١٢٣1MB'");
}
} // namespace
} // namespace facebook::velox
