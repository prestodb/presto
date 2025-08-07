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
#include "velox/functions/sparksql/tests/JsonTestUtil.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class FromToJsonRoundTripTest : public SparkFunctionBaseTest {
 protected:
  // Performs a round-trip test: original -> from_json -> to_json -> final, and
  // compare original and final.
  void testFromToJsonRoundTrip(
      const VectorPtr& input,
      const std::shared_ptr<const Type>& rowType,
      const std::optional<std::string>& timezone = std::nullopt) {
    // from_json
    auto fromJsonExpr = createFromJson(rowType);
    auto fromJsonResult =
        evaluate(fromJsonExpr, makeRowVector({"c0"}, {input}));

    // to_json
    auto toJsonExpr = createToJson(rowType, timezone);
    auto finalResult =
        evaluate(toJsonExpr, makeRowVector({"c0"}, {fromJsonResult}));
    // Compare original and final results
    assertEqualVectors(input, finalResult);
  }
};

TEST_F(FromToJsonRoundTripTest, basicStruct) {
  auto input = makeFlatVector<std::string>(
      {R"({"a":1,"b":1.1})", R"({"b":2.2})", R"({"a":3})", R"({})"});
  auto rowType = ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  testFromToJsonRoundTrip(input, rowType);
}

TEST_F(FromToJsonRoundTripTest, basicArray) {
  auto input = makeNullableFlatVector<std::string>(
      {R"([1])", R"([2,null,3])", R"([])", R"([null])"});
  auto rowType = ARRAY(BIGINT());
  testFromToJsonRoundTrip(input, rowType);
}

TEST_F(FromToJsonRoundTripTest, basicMap) {
  auto input = makeFlatVector<std::string>(
      {R"({"a":1})", R"({"b":2})", R"({"c":3})", R"({"3":3})"});
  auto rowType = MAP(VARCHAR(), BIGINT());
  testFromToJsonRoundTrip(input, rowType);
}

TEST_F(FromToJsonRoundTripTest, nestedComplexType) {
  // ROW(VARCHAR, ARRAY(INTEGER), MAP(VARCHAR, INTEGER),
  //     ROW(VARCHAR, ARRAY(INTEGER)))
  auto input = makeFlatVector<std::string>(
      {R"({"a":"str1","b":[1,2,3],"c":{"key1":1,"key2":2,"key3":3},"d":{"d1":"d1_str1","d2":[1,2,3]}})",
       R"({"a":"str2","b":[],"d":{"d1":"d1_str2","d2":[4,5]}})",
       R"({"a":"str3","b":[null],"c":{"key4":1,"key5":null},"d":{"d2":[null]}})"});
  auto rowType =
      ROW({"a", "b", "c", "d"},
          {VARCHAR(),
           ARRAY(INTEGER()),
           MAP(VARCHAR(), INTEGER()),
           ROW({"d1", "d2"}, {VARCHAR(), ARRAY(INTEGER())})});
  testFromToJsonRoundTrip(input, rowType);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
