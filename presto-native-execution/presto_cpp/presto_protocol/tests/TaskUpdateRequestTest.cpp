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

#include <fstream>
#include <ios>
#include <iosfwd>

#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/encode/Base64.h"

using namespace facebook;
using namespace facebook::presto::protocol;

// TODO data/TaskUpdateRequest.1 is out of date. Needs to be re-generated.
TEST(TestPrestoPrococol, DISABLED_TestAggregationNode) {
  std::string str = slurp("data/TaskUpdateRequest.1");

  json j = json::parse(str);
  TaskUpdateRequest p = j;

  // Check some values ...
  ASSERT_NE(p.fragment, nullptr);

  PlanFragment f = json::parse(velox::encoding::Base64::decode(*p.fragment));

  ASSERT_EQ(f.root->_type, ".AggregationNode");

  std::shared_ptr<AggregationNode> root =
      std::static_pointer_cast<AggregationNode>(f.root);
  ASSERT_EQ(root->id, "211");
  ASSERT_NE(root->source, nullptr);
  ASSERT_EQ(root->source->_type, ".ProjectNode");

  std::shared_ptr<ProjectNode> proj =
      std::static_pointer_cast<ProjectNode>(root->source);
  ASSERT_EQ(proj->id, "233");
  ASSERT_NE(proj->source, nullptr);
  ASSERT_EQ(proj->source->_type, ".TableScanNode");

  std::shared_ptr<TableScanNode> scan =
      std::static_pointer_cast<TableScanNode>(proj->source);
  ASSERT_EQ(scan->id, "0");

  testJsonRoundtrip(j, p);
}

// TODO data/TaskUpdateRequest.2 is out of date. Needs to be re-generated.
TEST(TestPrestoPrococol, DISABLED_TestLimitNode) {
  std::string str = slurp("data/TaskUpdateRequest.2");

  json j = json::parse(str);
  TaskUpdateRequest p = j;

  // Check some values ...
  ASSERT_NE(p.fragment, nullptr);

  PlanFragment f = json::parse(velox::encoding::Base64::decode(*p.fragment));

  ASSERT_EQ(f.root->_type, ".LimitNode");

  std::shared_ptr<LimitNode> root = std::static_pointer_cast<LimitNode>(f.root);
  ASSERT_EQ(root->id, "106");
  ASSERT_NE(root->source, nullptr);
  ASSERT_EQ(root->source->_type, ".TableScanNode");

  std::shared_ptr<TableScanNode> scan =
      std::static_pointer_cast<TableScanNode>(root->source);
  ASSERT_EQ(scan->id, "0");

  testJsonRoundtrip(j, p);
}
