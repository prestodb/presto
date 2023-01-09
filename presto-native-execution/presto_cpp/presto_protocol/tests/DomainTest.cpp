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

class DomainTest : public ::testing::Test {};

TEST_F(DomainTest, basic) {
  std::string str = R"(
        {
           "values":{
            "@type":"sortable",
              "type":"integer",
              "ranges":[
                 {
                    "low":{
                       "type":"integer",
                       "valueBlock":"CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
                       "bound":"EXACTLY"
                    },
                    "high":{
                       "type":"integer",
                       "valueBlock":"CQAAAElOVF9BUlJBWQEAAAAAAQAAAA==",
                       "bound":"EXACTLY"
                    }
                 }
              ]
           },
           "nullAllowed":false
        }
    )";

  json j = json::parse(str);
  Domain p = j;

  // Check some values ...
  ASSERT_NE(p.values, nullptr);
  ASSERT_EQ(p.values->_type, "sortable");

  auto values = std::static_pointer_cast<SortedRangeSet>(p.values);

  ASSERT_EQ(values->type, "integer");
  ASSERT_EQ(values->ranges[0].low.type, "integer");
  ASSERT_EQ(values->ranges[0].high.type, "integer");

  testJsonRoundtrip(j, p);
}
