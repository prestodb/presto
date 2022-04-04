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

#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"

using namespace facebook::presto::protocol;

TEST(CallExpressionTest, basic) {
  std::string str = R"(
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "variable",
                "name": "event_based_revenue",
                "type": "real"
              }
            ],
            "displayName": "sum",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": [
                  "real"
                ],
                "kind": "AGGREGATE",
                "longVariableConstraints": [],
                "name": "presto.default.sum",
                "returnType": "real",
                "typeVariableConstraints": [],
                "variableArity": false
              }
            },
            "returnType": "double"
          }
    )";

  json j = json::parse(str);
  CallExpression p = j;

  // Check some values ...
  ASSERT_EQ(p._type, "call");
  ;
  ASSERT_EQ(p.displayName, "sum");
  ASSERT_EQ(p.returnType, "double");

  ASSERT_NE(p.arguments[0], nullptr);
  {
    ASSERT_EQ(p.arguments[0]->_type, "variable");
    std::shared_ptr<VariableReferenceExpression> k =
        std::static_pointer_cast<VariableReferenceExpression>(p.arguments[0]);
    ASSERT_EQ(k->name, "event_based_revenue");
    ASSERT_EQ(k->type, "real");
  }

  ASSERT_NE(p.functionHandle, nullptr);
  {
    ASSERT_EQ(p.functionHandle->_type, "$static");
    std::shared_ptr<BuiltInFunctionHandle> k =
        std::static_pointer_cast<BuiltInFunctionHandle>(p.functionHandle);
    ASSERT_EQ(k->signature.argumentTypes[0], "real");
    ASSERT_EQ(k->signature.kind, FunctionKind::AGGREGATE);
    ASSERT_EQ(k->signature.returnType, "real");
  }

  testJsonRoundtrip(j, p);
}
