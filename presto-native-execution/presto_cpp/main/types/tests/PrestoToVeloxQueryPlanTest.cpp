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

#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/HyperLogLogRegistration.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/IPAddressRegistration.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixRegistration.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"
#include "velox/functions/prestosql/types/JsonRegistration.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneRegistration.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/UuidRegistration.h"
#include "velox/functions/prestosql/types/UuidType.h"

using namespace facebook::presto;
using namespace facebook::velox;

namespace {
void validateSqlFunctionHandleParsing(
    const std::shared_ptr<protocol::FunctionHandle>& functionHandle,
    const std::vector<TypePtr>& expectedRawInputTypes) {
  std::vector<TypePtr> actualRawInputTypes;
  TypeParser typeParser;
  const auto sqlFunctionHandle =
      std::static_pointer_cast<protocol::SqlFunctionHandle>(functionHandle);
  parseSqlFunctionHandle(sqlFunctionHandle, actualRawInputTypes, typeParser);
  EXPECT_EQ(expectedRawInputTypes.size(), actualRawInputTypes.size());
  for (int i = 0; i < expectedRawInputTypes.size(); i++) {
    EXPECT_EQ(*expectedRawInputTypes[i], *actualRawInputTypes[i]);
  }
}

protocol::VariableReferenceExpression makeVariable(const std::string& name) {
  protocol::VariableReferenceExpression variable;
  variable.name = name;
  variable.type = "bigint";
  return variable;
}
} // namespace

class PrestoToVeloxQueryPlanTest : public testing::Test {
 public:
  PrestoToVeloxQueryPlanTest() {
    registerHyperLogLogType();
    registerIPAddressType();
    registerIPPrefixType();
    registerJsonType();
    registerTimestampWithTimeZoneType();
    registerUuidType();
  }
};

TEST_F(PrestoToVeloxQueryPlanTest, parseSqlFunctionHandleWithZeroParam) {
  const std::string str = R"(
      {
        "@type": "sql_function_handle",
        "functionId": "json_file.test.count;",
        "version": "1"
      }
    )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::FunctionHandle> functionHandle = j;
  ASSERT_NE(functionHandle, nullptr);
  validateSqlFunctionHandleParsing(functionHandle, {});
}

TEST_F(PrestoToVeloxQueryPlanTest, parseSqlFunctionHandleWithOneParam) {
  const std::string str = R"(
          {
            "@type": "sql_function_handle",
            "functionId": "json_file.test.sum;tinyint",
            "version": "1"
          }
    )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::FunctionHandle> functionHandle = j;
  ASSERT_NE(functionHandle, nullptr);

  const std::vector<TypePtr> expectedRawInputTypes{TINYINT()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}

TEST_F(PrestoToVeloxQueryPlanTest, parseSqlFunctionHandleWithMultipleParam) {
  const std::string str = R"(
        {
          "@type": "sql_function_handle",
          "functionId": "json_file.test.avg;array(decimal(15, 2));varchar",
          "version": "1"
        }
    )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::FunctionHandle> functionHandle = j;
  ASSERT_NE(functionHandle, nullptr);

  const std::vector<TypePtr> expectedRawInputTypes{
      ARRAY(DECIMAL(15, 2)), VARCHAR()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}

TEST_F(PrestoToVeloxQueryPlanTest, parseSqlFunctionHandleAllComplexTypes) {
  const std::string str = R"(
        {
          "@type": "sql_function_handle",
          "functionId": "json_file.test.all_complex_types;row(map(hugeint, ipaddress), ipprefix);row(array(varbinary), timestamp, date, json, hyperloglog, timestamp with time zone, interval year to month, interval day to second);function(double, boolean);uuid",
          "version": "1"
        }
    )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::FunctionHandle> functionHandle = j;
  ASSERT_NE(functionHandle, nullptr);

  const std::vector<TypePtr> expectedRawInputTypes{
      ROW({MAP(HUGEINT(), IPADDRESS()), IPPREFIX()}),
      ROW(
          {ARRAY(VARBINARY()),
           TIMESTAMP(),
           DATE(),
           JSON(),
           HYPERLOGLOG(),
           TIMESTAMP_WITH_TIME_ZONE(),
           INTERVAL_YEAR_MONTH(),
           INTERVAL_DAY_TIME()}),
      FUNCTION({DOUBLE()}, BOOLEAN()),
      UUID()};
  validateSqlFunctionHandleParsing(functionHandle, expectedRawInputTypes);
}

TEST_F(PrestoToVeloxQueryPlanTest, parseIndexJoinNode) {
  const std::string str = R"(
    {
      "@type": "com.facebook.presto.sql.planner.plan.IndexJoinNode",
      "id": "2",
      "criteria": [
        {
          "left": {
            "@type": "variable",
            "name": "c1",
            "type": "bigint"
          },
          "right": {
            "@type": "variable",
            "name": "k1",
            "type": "bigint"
          }
        },
        {
          "left": {
            "@type": "variable",
            "name": "c2",
            "type": "smallint"
          },
          "right": {
            "@type": "variable",
            "name": "k2",
            "type": "smallint"
          }
        }
      ],
      "filter": {
        "@type": "special",
        "arguments": [
          {
            "@type": "special",
            "arguments": [
              {
                "@type": "variable",
                "name": "k3",
                "type": "smallint"
              },
              {
                "@type": "variable",
                "name": "c3",
                "type": "smallint"
              },
              {
                "@type": "constant",
                "type": "smallint",
                "valueBlock": "CwAAAFNIT1JUX0FSUkFZAQAAAAEABQA="
              }
            ],
            "form": "IN",
            "returnType": "boolean"
          },
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "variable",
                "name": "k4",
                "type": "bigint"
              },
              {
                "@type": "variable",
                "name": "c4",
                "type": "bigint"
              },
              {
                "@type": "variable",
                "name": "c5",
                "type": "bigint"
              }
            ],
            "displayName": "BETWEEN",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": [
                  "bigint",
                  "bigint",
                  "bigint"
                ],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.$operator$between",
                "returnType": "boolean",
                "typeVariableConstraints": [],
                "variableArity": false
              },
              "builtInFunctionKind": "ENGINE"
            },
            "returnType": "boolean"
          },
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "constant",
                "type": "bigint",
                "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAAAA"
              },
              {
                "@type": "call",
                "arguments": [
                  {
                    "@type": "variable",
                    "name": "c1",
                    "type": "bigint"
                  },
                  {
                    "@type": "constant",
                    "type": "bigint",
                    "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAGQAAAAAAAAA"
                  }
                ],
                "displayName": "MODULUS",
                "functionHandle": {
                  "@type": "$static",
                  "builtInFunctionKind": "ENGINE",
                  "signature": {
                    "argumentTypes": [
                      "bigint",
                      "bigint"
                    ],
                    "kind": "SCALAR",
                    "longVariableConstraints": [],
                    "name": "presto.default.$operator$modulus",
                    "returnType": "bigint",
                    "typeVariableConstraints": [],
                    "variableArity": false
                  }
                },
                "returnType": "bigint"
              }
            ],
            "displayName": "$operator$equal",
            "functionHandle": {
              "@type": "$static",
              "builtInFunctionKind": "ENGINE",
              "signature": {
                "argumentTypes": [
                  "bigint",
                  "bigint"
                ],
                "kind": "SCALAR",
                "longVariableConstraints": [],
                "name": "presto.default.$operator$equal",
                "returnType": "boolean",
                "typeVariableConstraints": [],
                "variableArity": false
              }
            },
            "returnType": "boolean"
          }
        ],
        "form": "AND",
        "returnType": "boolean"
      },
      "type": "INNER",
      "lookupVariables": []
    }
  )";

  const json j = json::parse(str);
  const std::shared_ptr<protocol::IndexJoinNode> indexJoinNode = j;
  ASSERT_NE(indexJoinNode, nullptr);
  ASSERT_EQ(indexJoinNode->type, protocol::JoinType::INNER);
  ASSERT_EQ(indexJoinNode->criteria.size(), 2);
  ASSERT_NE(indexJoinNode->filter, nullptr);
  ASSERT_EQ(indexJoinNode->probeSource, nullptr);
  ASSERT_EQ(indexJoinNode->indexSource, nullptr);
}

TEST_F(PrestoToVeloxQueryPlanTest, notNullColumnNamesEmpty) {
  EXPECT_TRUE(
      toNotNullColumnNames({}, {makeVariable("expr_1")}, {"c1"}).empty());
}

TEST_F(PrestoToVeloxQueryPlanTest, notNullColumnNamesTranslatesToTargetNames) {
  // Source variable order need not match the target column order.
  const protocol::List<protocol::VariableReferenceExpression> columns{
      makeVariable("expr_1"), makeVariable("expr_2"), makeVariable("expr_3")};
  const protocol::List<protocol::String> columnNames{"c3", "c1", "c2"};

  EXPECT_EQ(
      toNotNullColumnNames(
          {makeVariable("expr_3"), makeVariable("expr_1")},
          columns,
          columnNames),
      (folly::F14FastSet<std::string>{"c2", "c3"}));
}

TEST_F(PrestoToVeloxQueryPlanTest, notNullColumnNamesUnknownVariable) {
  VELOX_ASSERT_THROW(
      toNotNullColumnNames(
          {makeVariable("expr_2")}, {makeVariable("expr_1")}, {"c1"}),
      "NOT NULL column variable is not among the TableWriter columns: expr_2");
}

TEST_F(PrestoToVeloxQueryPlanTest, notNullColumnNamesSizeMismatch) {
  VELOX_ASSERT_THROW(
      toNotNullColumnNames(
          {makeVariable("expr_1")}, {makeVariable("expr_1")}, {"c1", "c2"}),
      "TableWriter columns and columnNames must have the same size");
}
