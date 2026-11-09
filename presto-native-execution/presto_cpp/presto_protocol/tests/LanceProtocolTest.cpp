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

#include "presto_cpp/presto_protocol/connector/lance/presto_protocol_lance.h"

using namespace facebook::presto::protocol;

// The Java coordinator serializes LanceTableHandle with airlift's
// NON_ABSENT ObjectMapper: empty Optionals are omitted, so the two cases to
// cover are "field present" and "field absent" (never "field null").
TEST(LanceProtocolTest, tableHandleWithVectorSearch) {
  std::string str = R"(
      {
        "@type": "lance",
        "schemaName": "default",
        "tableName": "vectors",
        "tablePath": "/data/vectors.lance",
        "tableId": ["vectors"],
        "datasetVersion": 7,
        "updatedColumns": [],
        "storageOptions": {},
        "vectorSearch": {
          "column": "vec",
          "queryVector": [0.3, 0.0, 0.0, 0.0],
          "k": 3,
          "metric": "l2",
          "nprobes": 10
        }
      }
  )";

  json j = json::parse(str);
  lance::LanceTableHandle handle = j;

  ASSERT_EQ(handle.schemaName, "default");
  ASSERT_EQ(handle.tableName, "vectors");
  ASSERT_NE(handle.datasetVersion, nullptr);
  ASSERT_EQ(*handle.datasetVersion, 7);
  ASSERT_NE(handle.vectorSearch, nullptr);
  ASSERT_EQ(handle.vectorSearch->column, "vec");
  ASSERT_EQ(handle.vectorSearch->queryVector.size(), 4);
  ASSERT_DOUBLE_EQ(handle.vectorSearch->queryVector[0], 0.3);
  ASSERT_EQ(handle.vectorSearch->k, 3);
  ASSERT_EQ(handle.vectorSearch->metric, "l2");
  ASSERT_NE(handle.vectorSearch->nprobes, nullptr);
  ASSERT_EQ(*handle.vectorSearch->nprobes, 10);
  ASSERT_EQ(handle.vectorSearch->ef, nullptr);
  ASSERT_EQ(handle.vectorSearch->refineFactor, nullptr);

  // Round-trip back to JSON: present fields survive, absent stay absent.
  json out = handle;
  ASSERT_EQ(out["datasetVersion"], 7);
  ASSERT_EQ(out["vectorSearch"]["k"], 3);
  ASSERT_EQ(out["vectorSearch"]["metric"], "l2");
  ASSERT_EQ(out["vectorSearch"]["nprobes"], 10);
  ASSERT_FALSE(out["vectorSearch"].contains("ef"));
}

TEST(LanceProtocolTest, tableHandleWithoutVectorSearch) {
  std::string str = R"(
      {
        "@type": "lance",
        "schemaName": "default",
        "tableName": "t1",
        "tablePath": "/data/t1.lance",
        "tableId": ["t1"],
        "updatedColumns": [],
        "storageOptions": {}
      }
  )";

  json j = json::parse(str);
  lance::LanceTableHandle handle = j;

  ASSERT_EQ(handle.tableName, "t1");
  ASSERT_EQ(handle.datasetVersion, nullptr);
  ASSERT_EQ(handle.vectorSearch, nullptr);

  json out = handle;
  ASSERT_FALSE(out.contains("datasetVersion"));
  ASSERT_FALSE(out.contains("vectorSearch"));
}
