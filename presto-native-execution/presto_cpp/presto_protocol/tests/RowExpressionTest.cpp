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

using namespace facebook::presto::protocol;

namespace {
const std::string strJson = R"(
        {
             "@type":"special",
             "form":"COALESCE",
             "returnType":"bigint",
             "arguments":[
                {
                   "@type":"call",
                   "displayName":"$operator$hash_code",
                   "functionHandle":{
                      "@type":"$static",
                      "signature":{
                         "name":"presto.default.$operator$hash_code",
                         "kind":"SCALAR",
                         "typeVariableConstraints":[
                         ],
                         "longVariableConstraints":[
                         ],
                         "returnType":"bigint",
                         "argumentTypes":[
                            "integer"
                         ],
                         "variableArity":false
                      },
                     "builtInFunctionKind": "ENGINE"
                   },
                   "returnType":"bigint",
                   "arguments":[
                      {
                         "@type":"variable",
                         "name":"segment",
                         "type":"integer"
                      }
                   ]
                },
                {
                   "@type":"constant",
                   "valueBlock":"CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAAAA",
                   "type":"bigint"
                }
             ]
        }
    )";
};

class RowExpressionTest : public ::testing::Test {};

TEST_F(RowExpressionTest, constant) {
  std::string str = R"(
        {
            "@type": "constant",   
            "valueBlock":"CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAAAA",
            "type":"bigint"
        }
    )";

  json j = json::parse(str);
  std::shared_ptr<RowExpression> p = j;

  // Check some values ...
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->_type, "constant");

  std::shared_ptr<ConstantExpression> k =
      std::static_pointer_cast<ConstantExpression>(p);
  ASSERT_EQ(k->type, "bigint");
  ASSERT_EQ("CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAAAA", k->valueBlock.data);

  testJsonRoundtrip(j, p);
}

TEST_F(RowExpressionTest, call) {
  std::string str = R"(
        {
           "@type":"call",
           "displayName":"$operator$hash_code",
           "functionHandle":{
              "@type":"$static",
              "signature":{
                 "name":"presto.default.$operator$hash_code",
                 "kind":"SCALAR",
                 "typeVariableConstraints":[

                 ],
                 "longVariableConstraints":[

                 ],
                 "returnType":"bigint",
                 "argumentTypes":[
                    "integer"
                 ],
                 "variableArity":false
              },
              "builtInFunctionKind": "ENGINE"
           },
           "returnType":"bigint",
           "arguments":[
              {
                 "@type":"variable",
                 "name":"segment",
                 "type":"integer"
              }
           ]
        }
    )";

  json j = json::parse(str);
  std::shared_ptr<RowExpression> p = j;

  // Check some values ...
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->_type, "call");

  std::shared_ptr<CallExpression> k =
      std::static_pointer_cast<CallExpression>(p);
  ASSERT_EQ(k->displayName, "$operator$hash_code");

  testJsonRoundtrip(j, p);
}

TEST_F(RowExpressionTest, special) {
  json j = json::parse(strJson);
  std::shared_ptr<RowExpression> p = j;

  // Check some values ...
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->_type, "special");

  std::shared_ptr<SpecialFormExpression> k =
      std::static_pointer_cast<SpecialFormExpression>(p);
  ASSERT_EQ(k->form, Form::COALESCE);
  ASSERT_EQ(k->returnType, "bigint");

  testJsonRoundtrip(j, p);
}

TEST_F(RowExpressionTest, variableReference) {
  json j = json::parse(strJson);
  std::shared_ptr<RowExpression> p = j;

  // Check some values ...
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(p->_type, "special");

  std::shared_ptr<SpecialFormExpression> k =
      std::static_pointer_cast<SpecialFormExpression>(p);
  ASSERT_EQ(k->form, Form::COALESCE);
  ASSERT_EQ(k->returnType, "bigint");

  testJsonRoundtrip(j, p);
}
