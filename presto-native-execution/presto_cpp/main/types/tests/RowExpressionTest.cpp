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

#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/core/Expressions.h"
#include "velox/type/Type.h"

using namespace facebook::presto;
using namespace facebook::velox;
using namespace facebook::velox::core;

class RowExpressionTest : public ::testing::Test {
 public:
  void SetUp() override {
    pool_ = memory::getDefaultScopedMemoryPool();
    converter_ = std::make_unique<VeloxExprConverter>(pool_.get());
  }

  void testConstantExpression(
      const std::string& str,
      const std::string& type,
      const std::string& value) {
    json j = json::parse(str);
    std::shared_ptr<protocol::RowExpression> p = j;

    auto cexpr = std::static_pointer_cast<const ConstantTypedExpr>(
        converter_->toVeloxExpr(p));

    ASSERT_EQ(cexpr->type()->toString(), type);
    ASSERT_EQ(cexpr->value().toJson(), value);
  }

  std::unique_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VeloxExprConverter> converter_;
};

TEST_F(RowExpressionTest, bigInt) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA",
            "type": "bigint"
        }
    )";
  testConstantExpression(str, "BIGINT", "1");
}

TEST_F(RowExpressionTest, doubleNull) {
  std::string str = R"(
        {
           "@type":"constant",
           "valueBlock":"AwAAAFJMRQEAAAAKAAAATE9OR19BUlJBWQEAAAABgA==",
           "type":"double"
        }
  )";
  testConstantExpression(str, "DOUBLE", "null");
}

TEST_F(RowExpressionTest, bigInt2) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock":"CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAAAA",
            "type":"bigint"
        }
    )";
  testConstantExpression(str, "BIGINT", "0");
}

TEST_F(RowExpressionTest, bigInt3) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAABcAAAAAAAAA",
            "type": "bigint"
        }
    )";
  testConstantExpression(str, "BIGINT", "23");
}

TEST_F(RowExpressionTest, bigInt4) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAGYAAAAAAAAA",
            "type": "bigint"
        }
    )";
  testConstantExpression(str, "BIGINT", "102");
}

TEST_F(RowExpressionTest, double1) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAADdA",
            "type": "double"
        }
    )##";
  testConstantExpression(str, "DOUBLE", "23");
}

TEST_F(RowExpressionTest, double2) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAgFlA",
            "type": "double"
        }
    )##";
  testConstantExpression(str, "DOUBLE", "102");
}

TEST_F(RowExpressionTest, double3) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAAAAAAAAPA/",
            "type": "double"
        }
    )##";
  testConstantExpression(str, "DOUBLE", "1");
}

TEST_F(RowExpressionTest, real1) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAADMQg==",
            "type": "real"
        }
    )##";
  testConstantExpression(str, "REAL", "102");
}

TEST_F(RowExpressionTest, real2) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAACAPw==",
            "type": "real"
        }
    )##";
  testConstantExpression(str, "REAL", "1");
}

TEST_F(RowExpressionTest, real3) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAAC4QQ==",
            "type": "real"
        }
    )##";
  testConstantExpression(str, "REAL", "23");
}

TEST_F(RowExpressionTest, integer1) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
            "type": "integer"
        }
    )";
  testConstantExpression(str, "INTEGER", "1");
}

TEST_F(RowExpressionTest, integer2) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAFwAAAA==",
            "type": "integer"
        }
    )";
  testConstantExpression(str, "INTEGER", "23");
}

TEST_F(RowExpressionTest, integer3) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAZgAAAA==",
            "type": "integer"
        }
    )";
  testConstantExpression(str, "INTEGER", "102");
}

TEST_F(RowExpressionTest, tinyInt1) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAEJZVEVfQVJSQVkBAAAAAAE=",
            "type": "tinyint"
        }
    )";
  testConstantExpression(str, "TINYINT", "1");
}

TEST_F(RowExpressionTest, tinyInt2) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAEJZVEVfQVJSQVkBAAAAABc=",
            "type": "tinyint"
        }
    )";
  testConstantExpression(str, "TINYINT", "23");
}

TEST_F(RowExpressionTest, tinyInt3) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAEJZVEVfQVJSQVkBAAAAAGY=",
            "type": "tinyint"
        }
    )";
  testConstantExpression(str, "TINYINT", "102");
}

TEST_F(RowExpressionTest, booleanTrue) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAEJZVEVfQVJSQVkBAAAAAAE=",
            "type": "boolean"
        }
    )";
  testConstantExpression(str, "BOOLEAN", "true");
}

TEST_F(RowExpressionTest, varchar1) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAIAAAAAAgAAADIz",
            "type": "varchar(25)"
        }
    )##";
  testConstantExpression(str, "VARCHAR", "\"23\"");
}

TEST_F(RowExpressionTest, varchar2) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAAAAQAAADE=",
            "type": "varchar"
        }
    )##";
  testConstantExpression(str, "VARCHAR", "\"1\"");
}

TEST_F(RowExpressionTest, varchar3) {
  std::string str = R"##(
        {
            "@type": "constant",
            "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAMAAAAAAwAAADEwMg==",
            "type": "varchar"
        }
    )##";
  testConstantExpression(str, "VARCHAR", "\"102\"");
}

TEST_F(RowExpressionTest, timestamp) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAMAvTLU6AQAA",
            "type": "timestamp"
        }
    )";
  testConstantExpression(str, "TIMESTAMP", "\"2012-10-31T05:30:00.000000000\"");
}

TEST_F(RowExpressionTest, timestamp2) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAEDcFCNuAQAA",
            "type": "timestamp"
        }
    )";
  testConstantExpression(str, "TIMESTAMP", "\"2019-10-31T18:30:00.000000000\"");
}

TEST_F(RowExpressionTest, date) {
  std::string str = R"(
        {
            "@type": "constant",
            "valueBlock": "CQAAAElOVF9BUlJBWQEAAAAAGEcAAA==",
            "type": "date"
        }
    )";
  testConstantExpression(str, "DATE", "\"2019-10-31\"");
}

TEST_F(RowExpressionTest, call) {
  std::string str = R"##(
      {
        "@type": "call",
        "arguments": [
          {
            "@type": "variable",
            "name": "name",
            "type": "varchar(25)"
          },
          {
            "@type": "constant",
            "type": "varchar(25)",
            "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAMAAAAAAwAAAGZvbw=="
          }
        ],
        "displayName": "EQUAL",
        "functionHandle": {
          "@type": "$static",
          "signature": {
            "argumentTypes": [
              "varchar(25)",
              "varchar(25)"
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
  )##";

  json j = json::parse(str);
  std::shared_ptr<protocol::RowExpression> p = j;

  InputTypedExpr rowExpr(BIGINT());

  auto callexpr =
      std::static_pointer_cast<const CallTypedExpr>(converter_->toVeloxExpr(p));

  // Check some values ...
  ASSERT_EQ(callexpr->name(), "eq");

  auto iexpr = callexpr->inputs();

  ASSERT_EQ(iexpr.size(), 2);

  {
    auto cexpr = std::static_pointer_cast<const FieldAccessTypedExpr>(iexpr[0]);
    ASSERT_EQ(cexpr->type()->toString(), "VARCHAR");
    ASSERT_EQ(cexpr->name(), "name");
  }
  {
    auto cexpr = std::static_pointer_cast<const ConstantTypedExpr>(iexpr[1]);
    ASSERT_EQ(cexpr->type()->toString(), "VARCHAR");
    ASSERT_EQ(cexpr->value().toJson(), "\"foo\"");
  }
}

TEST_F(RowExpressionTest, special) {
  std::string str = R"##(
      {
        "@type": "special",
        "arguments": [
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "variable",
                "name": "custkey",
                "type": "bigint"
              },
              {
                "@type": "constant",
                "type": "bigint",
                "valueBlock": "CgAAAExPTkdfQVJSQVkBAAAAAAoAAAAAAAAA"
              }
            ],
            "displayName": "EQUAL",
            "functionHandle": {
              "@type": "$static",
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
          },
          {
            "@type": "call",
            "arguments": [
              {
                "@type": "variable",
                "name": "name",
                "type": "varchar(25)"
              },
              {
                "@type": "constant",
                "type": "varchar(25)",
                "valueBlock": "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAMAAAAAAwAAAGZvbw=="
              }
            ],
            "displayName": "EQUAL",
            "functionHandle": {
              "@type": "$static",
              "signature": {
                "argumentTypes": [
                  "varchar(25)",
                  "varchar(25)"
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
      }
  )##";

  json j = json::parse(str);
  std::shared_ptr<protocol::RowExpression> p = j;

  auto callexpr =
      std::static_pointer_cast<const CallTypedExpr>(converter_->toVeloxExpr(p));

  // Check some values ...
  ASSERT_EQ(callexpr->type()->toString(), "BOOLEAN");
  ASSERT_EQ(callexpr->name(), "and");

  {
    auto arg0expr =
        std::static_pointer_cast<const CallTypedExpr>(callexpr->inputs()[0]);

    ASSERT_EQ(arg0expr->type()->toString(), "BOOLEAN");
    ASSERT_EQ(arg0expr->name(), "eq");
    {
      auto cexpr = std::static_pointer_cast<const FieldAccessTypedExpr>(
          arg0expr->inputs()[0]);
      ASSERT_EQ(cexpr->type()->toString(), "BIGINT");
      ASSERT_EQ(cexpr->name(), "custkey");
    }
    {
      auto cexpr = std::static_pointer_cast<const ConstantTypedExpr>(
          arg0expr->inputs()[1]);
      ASSERT_EQ(cexpr->type()->toString(), "BIGINT");
      ASSERT_EQ(cexpr->value().toJson(), "10");
    }
  }

  {
    auto arg1expr =
        std::static_pointer_cast<const CallTypedExpr>(callexpr->inputs()[1]);

    ASSERT_EQ(arg1expr->type()->toString(), "BOOLEAN");
    ASSERT_EQ(arg1expr->name(), "eq");
    {
      auto cexpr = std::static_pointer_cast<const FieldAccessTypedExpr>(
          arg1expr->inputs()[0]);
      ASSERT_EQ(cexpr->type()->toString(), "VARCHAR");
      ASSERT_EQ(cexpr->name(), "name");
    }
    {
      auto cexpr = std::static_pointer_cast<const ConstantTypedExpr>(
          arg1expr->inputs()[1]);
      ASSERT_EQ(cexpr->type()->toString(), "VARCHAR");
      ASSERT_EQ(cexpr->value().toJson(), "\"foo\"");
    }
  }
}

TEST_F(RowExpressionTest, bind) {
  std::string str = R"##(
      {
         "@type":"special",
         "form":"BIND",
         "returnType":"function(integer,bigint)",
         "arguments":[
            {
               "@type":"variable",
               "name":"nationkey",
               "type":"bigint"
            },
            {
               "@type":"lambda",
               "argumentTypes":[
                  "bigint",
                  "integer"
               ],
               "arguments":[
                  "nationkey_3",
                  "expr"
               ],
               "body":{
                  "@type":"call",
                  "displayName":"ADD",
                  "functionHandle":{
                     "@type":"$static",
                     "signature":{
                        "name":"presto.default.$operator$add",
                        "kind":"SCALAR",
                        "typeVariableConstraints":[

                        ],
                        "longVariableConstraints":[

                        ],
                        "returnType":"bigint",
                        "argumentTypes":[
                           "bigint",
                           "bigint"
                        ],
                        "variableArity":false
                     }
                  },
                  "returnType":"bigint",
                  "arguments":[
                     {
                        "@type":"call",
                        "displayName":"CAST",
                        "functionHandle":{
                           "@type":"$static",
                           "signature":{
                              "name":"presto.default.$operator$cast",
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
                           }
                        },
                        "returnType":"bigint",
                        "arguments":[
                           {
                              "@type":"variable",
                              "name":"expr",
                              "type":"integer"
                           }
                        ]
                     },
                     {
                        "@type":"variable",
                        "name":"nationkey_3",
                        "type":"bigint"
                     }
                  ]
               }
            }
         ]
      }
  )##";

  json j = json::parse(str);
  std::shared_ptr<protocol::RowExpression> p = j;

  auto expr = converter_->toVeloxExpr(p);

  auto lambda = std::dynamic_pointer_cast<const LambdaTypedExpr>(expr);
  ASSERT_NE(lambda, nullptr);

  auto expectedSignature = ROW({"expr"}, {INTEGER()});
  ASSERT_EQ(*lambda->signature(), *expectedSignature);
}

TEST_F(RowExpressionTest, likeSimple) {
  std::string str = R"##(
      {
         "@type" : "call",
            "displayName" : "LIKE",
            "functionHandle" : {
              "@type" : "$static",
              "signature" : {
                "name" : "presto.default.like",
                "kind" : "SCALAR",
                "typeVariableConstraints" : [ ],
                "longVariableConstraints" : [ ],
                "returnType" : "boolean",
                "argumentTypes" : [ "varchar", "LikePattern" ],
                "variableArity" : false
              }
            },
            "returnType" : "boolean",
            "arguments" : [ {
              "@type" : "variable",
              "name" : "type",
              "type" : "varchar(25)"
            }, {
              "@type" : "call",
              "displayName" : "CAST",
              "functionHandle" : {
                "@type" : "$static",
                "signature" : {
                  "name" : "presto.default.$operator$cast",
                  "kind" : "SCALAR",
                  "typeVariableConstraints" : [ ],
                  "longVariableConstraints" : [ ],
                  "returnType" : "LikePattern",
                  "argumentTypes" : [ "varchar" ],
                  "variableArity" : false
                }
              },
              "returnType" : "LikePattern",
              "arguments" : [ {
                "@type" : "constant",
                "valueBlock" : "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAYAAAAABgAAACVCUkFTUw==",
                "type" : "varchar(6)"
              } ]
            } ]
      }
    )##";

  json j = json::parse(str);
  std::shared_ptr<protocol::RowExpression> p = j;

  auto expr = converter_->toVeloxExpr(p);

  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_NE(callExpr, nullptr);

  auto callExprToString = callExpr->toString();
  ASSERT_EQ(callExprToString, "like(\"type\",\"%BRASS\")");
}

TEST_F(RowExpressionTest, likeWithEscape) {
  std::string str = R"##(
      {
         "@type" : "call",
            "displayName" : "LIKE",
            "functionHandle" : {
              "@type" : "$static",
              "signature" : {
                "name" : "presto.default.like",
                "kind" : "SCALAR",
                "typeVariableConstraints" : [ ],
                "longVariableConstraints" : [ ],
                "returnType" : "boolean",
                "argumentTypes" : [ "varchar", "LikePattern" ],
                "variableArity" : false
              }
            },
            "returnType" : "boolean",
            "arguments" : [ {
              "@type" : "variable",
              "name" : "type",
              "type" : "varchar(25)"
            }, {
              "@type" : "call",
              "displayName" : "LIKE_PATTERN",
              "functionHandle" : {
                "@type" : "$static",
                "signature" : {
                  "name" : "presto.default.like_pattern",
                  "kind" : "SCALAR",
                  "typeVariableConstraints" : [ ],
                  "longVariableConstraints" : [ ],
                  "returnType" : "LikePattern",
                  "argumentTypes" : [ "varchar", "varchar" ],
                  "variableArity" : false
                }
              },
              "returnType" : "LikePattern",
              "arguments" : [ {
                "@type" : "constant",
                "valueBlock" : "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAYAAAAABgAAACVCUkFTUw==",
                "type" : "varchar(6)"
              }, {
                "@type" : "constant",
                "valueBlock" : "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAEAAAAAAQAAACM=",
                "type" : "varchar(1)"
              } ]
            } ]
      }
    )##";

  json j = json::parse(str);
  std::shared_ptr<protocol::RowExpression> p = j;

  auto expr = converter_->toVeloxExpr(p);

  auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(expr);
  ASSERT_NE(callExpr, nullptr);

  auto callExprToString = callExpr->toString();
  ASSERT_EQ(callExpr->toString(), "like(\"type\",\"%BRASS\",\"#\")");
}

TEST_F(RowExpressionTest, dereference) {
  std::string str = R"##(
    {
       "@type":"special",
       "form":"DEREFERENCE",
       "returnType":"integer",
       "arguments":[
          {
             "@type":"call",
             "displayName":"SUBSCRIPT",
             "functionHandle":{
                "@type":"$static",
                "signature":{
                   "name":"presto.default.$operator$subscript",
                   "kind":"SCALAR",
                   "typeVariableConstraints":[

                   ],
                   "longVariableConstraints":[

                   ],
                   "returnType":"row(partkey integer,suppkey integer,quantity integer,extendedprice double,discount double,tax double,returnflag varchar(1),linestatus varchar(1),shipdate varchar,commitdate varchar,receiptdate varchar,shipinstruct varchar(25),shipmode varchar(10))",
                   "argumentTypes":[
                      "array(row(partkey integer,suppkey integer,quantity integer,extendedprice double,discount double,tax double,returnflag varchar(1),linestatus varchar(1),shipdate varchar,commitdate varchar,receiptdate varchar,shipinstruct varchar(25),shipmode varchar(10)))",
                      "bigint"
                   ],
                   "variableArity":false
                }
             },
             "returnType":"row(partkey integer,suppkey integer,quantity integer,extendedprice double,discount double,tax double,returnflag varchar(1),linestatus varchar(1),shipdate varchar,commitdate varchar,receiptdate varchar,shipinstruct varchar(25),shipmode varchar(10))",
             "arguments":[
                {
                   "@type":"call",
                   "displayName":"SUBSCRIPT",
                   "functionHandle":{
                      "@type":"$static",
                      "signature":{
                         "name":"presto.default.$operator$subscript",
                         "kind":"SCALAR",
                         "typeVariableConstraints":[

                         ],
                         "longVariableConstraints":[

                         ],
                         "returnType":"array(row(partkey integer,suppkey integer,quantity integer,extendedprice double,discount double,tax double,returnflag varchar(1),linestatus varchar(1),shipdate varchar,commitdate varchar,receiptdate varchar,shipinstruct varchar(25),shipmode varchar(10)))",
                         "argumentTypes":[
                            "map(integer,array(row(partkey integer,suppkey integer,quantity integer,extendedprice double,discount double,tax double,returnflag varchar(1),linestatus varchar(1),shipdate varchar,commitdate varchar,receiptdate varchar,shipinstruct varchar(25),shipmode varchar(10))))",
                            "integer"
                         ],
                         "variableArity":false
                      }
                   },
                   "returnType":"array(row(partkey integer,suppkey integer,quantity integer,extendedprice double,discount double,tax double,returnflag varchar(1),linestatus varchar(1),shipdate varchar,commitdate varchar,receiptdate varchar,shipinstruct varchar(25),shipmode varchar(10)))",
                   "arguments":[
                      {
                         "@type":"variable",
                         "name":"lineitems_map",
                         "type":"map(integer,array(row(partkey integer,suppkey integer,quantity integer,extendedprice double,discount double,tax double,returnflag varchar(1),linestatus varchar(1),shipdate varchar,commitdate varchar,receiptdate varchar,shipinstruct varchar(25),shipmode varchar(10))))"
                      },
                      {
                         "@type":"constant",
                         "valueBlock":"CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
                         "type":"integer"
                      }
                   ]
                },
                {
                   "@type":"constant",
                   "valueBlock":"CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA",
                   "type":"bigint"
                }
             ]
          },
          {
             "@type":"constant",
             "valueBlock":"CQAAAElOVF9BUlJBWQEAAAAAAAAAAA==",
             "type":"integer"
          }
       ]
    }
  )##";

  json j = json::parse(str);
  std::shared_ptr<protocol::RowExpression> p = j;

  auto expr = converter_->toVeloxExpr(p);

  auto fieldAccess =
      std::dynamic_pointer_cast<const FieldAccessTypedExpr>(expr);
  ASSERT_NE(fieldAccess, nullptr);

  ASSERT_EQ(fieldAccess->name(), "partkey");
}
