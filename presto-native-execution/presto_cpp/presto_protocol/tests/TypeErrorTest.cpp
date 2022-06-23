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

TEST(TestPrestoPrococol, TestUnknownFromJsonSubclass) {
  std::string str = R"(
        {
           "values":{
            "@type":"unknown",
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
  Domain p;

  ASSERT_THROW(p = j;, TypeError) << "unknown from_json subclass key";
}

// TODO(spershin) Disabling this test now due to asan failures (memory leak).
// When we move Presto protocl from json to thrift serialization, we won't
// need this test anymore.
TEST(TestPrestoPrococol, DISABLED_TestUnknownToJsonSubclass) {
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
  p.values->_type = "Unknown";

  ASSERT_THROW(j = p;, TypeError) << "unknown from_json key";
}

TEST(TestPrestoPrococol, TestUnknownKey) {
  std::string str = R"( { "strange_key": "aaa" } )";

  json j = json::parse(str);
  Column p;

  ASSERT_THROW(p = j;, OutOfRange) << "unknown member key";
}
