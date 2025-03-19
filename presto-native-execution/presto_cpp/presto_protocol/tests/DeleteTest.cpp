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

using namespace ::testing;

class DeleteTest : public ::testing::Test {};

TEST_F(DeleteTest, jsonRoundtrip) {
  std::string str = R"(
      {
        "@type" : ".DeleteNode",
        "id" : "0",
        "source" : {
          "@type" : ".ValuesNode",
          "id" : "1",
          "outputVariables" : [ {
            "@type" : "variable",
            "name" : "$row_group_id",
            "type" : "varchar"
          }, {
            "@type" : "variable",
            "name" : "$row_number",
            "type" : "bigint"
          } ],
          "rows" : [ [ {
              "@type" : "constant",
              "valueBlock" : "DgAAAFZBUklBQkxFX1dJRFRIAQAAAAUAAAAABQAAAGFiY2Rl",
              "type" : "varchar"
            }, {
              "@type" : "constant",
              "valueBlock" : "CgAAAExPTkdfQVJSQVkBAAAAAAEAAAAAAAAA",
              "type" : "bigint"
          } ] ]
        },
        "rowId" : {
          "@type" : "variable",
          "name" : "$rowid",
          "type" : "varbinary"
        },
        "outputVariables" : [ {
            "@type" : "variable",
            "name" : "$row_group_id",
            "type" : "varchar"
          }, {
            "@type" : "variable",
            "name" : "$row_number",
            "type" : "bigint"
          } ],
        "inputDistribution" : {
          "@type" : ".BaseInputDistribution",
          "partitionBy" : [ {
            "@type" : "variable",
            "name" : "part_key",
            "type" : "varchar"
          } ],
          "inputVariables" : [ {
              "@type" : "variable",
              "name" : "filter_key",
              "type" : "bigint"
          }, {
              "@type" : "variable",
              "name" : "$row_group_id",
              "type" : "varchar"
          }, {
              "@type" : "variable",
              "name" : "$row_number",
              "type" : "bigint"
          } ]
        }
      }
  )";

  json j = json::parse(str);
  DeleteNode d = j;

  // Check some values ...
  ASSERT_NE(d.inputDistribution, nullptr);
  ASSERT_EQ(d.inputDistribution->_type, ".BaseInputDistribution");

  auto inputDistribution =
      std::static_pointer_cast<BaseInputDistribution>(d.inputDistribution);
  ASSERT_EQ(inputDistribution->partitionBy[0].name, "part_key");
  ASSERT_EQ(inputDistribution->inputVariables.size(), 3);
  ASSERT_EQ(inputDistribution->inputVariables[0].name, "filter_key");
  ASSERT_EQ(inputDistribution->inputVariables[1].name, "$row_group_id");
  ASSERT_EQ(inputDistribution->inputVariables[2].name, "$row_number");

  ASSERT_EQ(d.outputVariables.size(), 2);
  ASSERT_EQ(d.outputVariables[0].name, "$row_group_id");
  ASSERT_EQ(d.outputVariables[1].name, "$row_number");

  ASSERT_EQ(d.rowId.name, "$rowid");

  testJsonRoundtrip(j, d);
}
