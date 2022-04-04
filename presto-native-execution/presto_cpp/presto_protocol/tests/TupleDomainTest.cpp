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

TEST(TestPrestoPrococol, TestTupleDomainSubfield) {
  std::string str = R"(
        { "columnDomains":[ 
                {
                    "column":"valid_experiment_reading",
                    "domain":{
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
                } 
            ]
        }
    )";

  json j = json::parse(str);
  TupleDomain<Subfield> tup = j;

  ASSERT_NE(tup.domains, nullptr);
  ASSERT_NE((*tup.domains)["valid_experiment_reading"].values, nullptr);
  ASSERT_EQ(
      (*tup.domains)["valid_experiment_reading"].values->_type, "sortable");

  testJsonRoundtrip(j, tup);
}
