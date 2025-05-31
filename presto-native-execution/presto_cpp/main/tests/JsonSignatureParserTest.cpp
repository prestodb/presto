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

#include "presto_cpp/main/JsonSignatureParser.h"
#include <gtest/gtest.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox;

namespace facebook::presto::test {
namespace {

class JsonSignatureParserTest : public testing::Test {};

TEST_F(JsonSignatureParserTest, broken) {
  // Needs to parse and provide a top level `udfSignatureMap` object.
  VELOX_ASSERT_THROW(
      JsonSignatureParser(""), "Unable to parse function signature JSON file");
  VELOX_ASSERT_THROW(
      JsonSignatureParser("{}"),
      "Unable to find top level 'udfSignatureMap' key.");
  VELOX_ASSERT_THROW(
      JsonSignatureParser("{\"wrong_key\": 123}"),
      "Unable to find top level 'udfSignatureMap' key.");
  VELOX_ASSERT_THROW(
      JsonSignatureParser("{\"udfSignatureMap\": 123}"),
      "Input signatures should be an object.");
  VELOX_ASSERT_THROW(
      JsonSignatureParser("{\"udfSignatureMap\": []}"),
      "Input signatures should be an object");

  EXPECT_NO_THROW(JsonSignatureParser parser("{\"udfSignatureMap\": {}}"));

  // Broken signatures.
  VELOX_ASSERT_THROW(
      JsonSignatureParser("{\"udfSignatureMap\": {\"\": []}}"),
      "The key for a function item should be a non-empty string.");
  VELOX_ASSERT_THROW(
      JsonSignatureParser("{\"udfSignatureMap\": {\"func\": [123]}}"),
      "Function signature should be an object.");
  VELOX_ASSERT_THROW(
      JsonSignatureParser("{\"udfSignatureMap\": {\"func\": [{}, {}]}}"),
      "`outputType` and `paramTypes` are mandatory in a signature");
  VELOX_ASSERT_THROW(
      JsonSignatureParser(
          "{\"udfSignatureMap\": "
          "{\"func\": [{\"outputType\": 123, \"paramTypes\": []}]}}"),
      "Function type name should be a string.");
  VELOX_ASSERT_THROW(
      JsonSignatureParser(
          "{\"udfSignatureMap\": {\"func\": "
          "[{\"outputType\": \"varchar\", \"paramTypes\": [123]}]}}"),
      "Function type name should be a string.");

  EXPECT_NO_THROW(JsonSignatureParser parser(
      "{\"udfSignatureMap\": {\"func\": "
      "[{\"outputType\": \"varchar\", \"paramTypes\": [\"varchar\"]}]}}"));
}

TEST_F(JsonSignatureParserTest, simpleTypes) {
  auto input = R"(
  {
    "udfSignatureMap": {
      "my_func": [
        {
          "outputType": "varchar",
          "paramTypes": [
            "varchar",
            "varbinary",
            "boolean",
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "real",
            "double",
            "timestamp",
            "date"
          ],
          "schema": "test_schema"
        }
      ]
    }
  })";

  JsonSignatureParser parser(input);
  EXPECT_EQ(1, parser.size());

  const auto& it = parser.begin();
  EXPECT_EQ(it->first, "my_func");
  EXPECT_EQ(it->second.size(), 1);
  EXPECT_EQ(it->second.front().schema, "test_schema");

  // We can just verify the counts here. If the type doesn't not exist or can't
  // be parsed, the parsing constructor will throw.
  const auto& item = it->second.front();
  EXPECT_EQ(item.signature->argumentTypes().size(), 11);
}

TEST_F(JsonSignatureParserTest, complexTypes) {
  auto input = R"V(
  {
    "udfSignatureMap": {
      "my_func": [
        {
          "outputType": "varchar",
          "paramTypes": [
            "array(bigint)",
            "map(varchar, double)",
            "row(varbinary, double, tinyint)",
            "array(array(bigint))",
            "map(array(map(bigint, array(boolean))), row(integer, real))"
          ]
        }
      ]
    }
  })V";

  JsonSignatureParser parser(input);
  EXPECT_EQ(1, parser.size());

  const auto& it = parser.begin();
  EXPECT_EQ(it->first, "my_func");
  EXPECT_EQ(it->second.size(), 1);

  // We can just verify the counts here. If the type doesn't exist or can't
  // be parsed, the parsing constructor will throw.
  const auto& item = it->second.front();
  EXPECT_EQ(item.signature->argumentTypes().size(), 5);
}

TEST_F(JsonSignatureParserTest, multiple) {
  // Real example:
  auto input = R"(
  {
    "udfSignatureMap": {
      "fb_lower": [
        {
          "docString": "example 1",
          "outputType": "varchar",
          "paramTypes": [
            "varchar"
          ],
          "schema": "spark",
          "routineCharacteristics": {
            "language": "CPP",
            "determinism": "DETERMINISTIC",
            "nullCallClause": "CALLED_ON_NULL_INPUT"
          }
        },
        {
          "docString": "example 2",
          "outputType": "varchar",
          "paramTypes": [
            "varchar",
            "varchar"
          ],
          "schema": "spark",
          "routineCharacteristics": {
            "language": "CPP",
            "determinism": "DETERMINISTIC",
            "nullCallClause": "CALLED_ON_NULL_INPUT"
          }
        }
      ]
    }
  })";

  JsonSignatureParser parser(input);
  EXPECT_EQ(1, parser.size());

  const auto& it = parser.begin();
  EXPECT_EQ(it->first, "fb_lower");
  EXPECT_EQ(it->second.size(), 2);

  const auto& signature0 = it->second[0].signature;
  EXPECT_EQ(signature0->returnType().baseName(), "varchar");
  EXPECT_EQ(signature0->argumentTypes().size(), 1);
  EXPECT_EQ(signature0->argumentTypes()[0].baseName(), "varchar");

  const auto& signature1 = it->second[1].signature;
  EXPECT_EQ(signature1->returnType().baseName(), "varchar");
  EXPECT_EQ(signature1->argumentTypes().size(), 2);
  EXPECT_EQ(signature1->argumentTypes()[0].baseName(), "varchar");
  EXPECT_EQ(signature1->argumentTypes()[1].baseName(), "varchar");
}

TEST_F(JsonSignatureParserTest, dynamic) {
  auto input = R"(
  {
    "dynamicUdfSignatureMap": {
      "subdir_name": {
      "mock1": [
        {
          "entrypoint": "registry123",
          "fileName" : "test123",
          "outputType": "varchar",
          "paramTypes": [
            "varchar"
          ]
        }
      ],
      "mock2": [
        {
          "fileName" : "test123",
          "outputType": "boolean",
          "paramTypes": []
        }
      ]
    }
    }
  })";

  JsonSignatureParser parser(input, JsonSignatureScope::DynamiclibrariesUdf);
  EXPECT_EQ(2, parser.size());
  bool seenMock1, seenMock2 = false;
  for (auto it = parser.begin(); it != parser.end(); ++it) {
    if (it->first == "mock1") {
      if (seenMock1) {
        continue;
      }
      EXPECT_EQ(it->second.size(), 1);

      const auto& signature0 = it->second[0].signature;
      EXPECT_EQ(signature0->returnType().baseName(), "varchar");
      EXPECT_EQ(it->second[0].entrypoint, "registry123");
      EXPECT_EQ(signature0->argumentTypes().size(), 1);
      EXPECT_EQ(signature0->argumentTypes()[0].baseName(), "varchar");
      seenMock1 = true;
    } else if (it->first == "mock2") {
      if (seenMock2) {
        continue;
      }
      EXPECT_EQ(it->second.size(), 1);

      const auto& signature1 = it->second[0].signature;
      EXPECT_EQ(signature1->returnType().baseName(), "boolean");
      EXPECT_EQ(signature1->argumentTypes().size(), 0);
      seenMock2 = true;
    } else {
      FAIL() << "Unknown key found: " << it->first;
    }
  }
  EXPECT_TRUE(seenMock1);
  EXPECT_TRUE(seenMock2);
}

TEST_F(JsonSignatureParserTest, dynamicComplex) {
  auto input = R"json(
    {
      "dynamicUdfSignatureMap": {
        "subdir_name": {
          "mock1": [
            {
              "entrypoint": "registry123",
              "fileName" : "test123",
              "outputType": "array(array(varchar))",
              "paramTypes": [
                "varchar"
              ]
            }
          ],
          "mock2": [
            {
              "fileName" : "test123",
              "outputType": "array(array(date))",
              "paramTypes": []
            }
          ]
        }
      }
    }
    )json";
  JsonSignatureParser parser(input, JsonSignatureScope::DynamiclibrariesUdf);
  EXPECT_EQ(2, parser.size());
  bool seenMock1, seenMock2 = false;
  for (auto it = parser.begin(); it != parser.end(); ++it) {
    if (it->first == "mock1") {
      if (seenMock1) {
        continue;
      }
      EXPECT_EQ(it->second.size(), 1);

      const auto& signature0 = it->second[0].signature;
      EXPECT_EQ(signature0->returnType().baseName(), "array");
      EXPECT_EQ(signature0->returnType().parameters()[0].baseName(), "array");
      EXPECT_EQ(
          signature0->returnType().parameters()[0].parameters()[0].baseName(),
          "varchar");
      EXPECT_EQ(it->second[0].entrypoint, "registry123");
      EXPECT_EQ(signature0->argumentTypes().size(), 1);
      EXPECT_EQ(signature0->argumentTypes()[0].baseName(), "varchar");
      seenMock1 = true;
    } else if (it->first == "mock2") {
      if (seenMock2) {
        continue;
      }
      EXPECT_EQ(it->second.size(), 1);

      const auto& signature1 = it->second[0].signature;
      EXPECT_EQ(signature1->returnType().baseName(), "array");
      EXPECT_EQ(signature1->returnType().parameters()[0].baseName(), "array");
      EXPECT_EQ(
          signature1->returnType().parameters()[0].parameters()[0].baseName(),
          "date");
      EXPECT_EQ(signature1->argumentTypes().size(), 0);
      seenMock2 = true;
    } else {
      FAIL() << "Unknown key found: " << it->first;
    }
  }
  EXPECT_TRUE(seenMock1);
  EXPECT_TRUE(seenMock2);
}

} // namespace
} // namespace facebook::presto::test
