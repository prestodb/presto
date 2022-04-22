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

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <ios>
#include <iosfwd>

#include "presto_cpp/main/common/tests/test_json.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/exec/Operator.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace fs = boost::filesystem;

using namespace facebook::presto;
using namespace facebook::velox;

namespace {
std::string getDataPath(const std::string& fileName) {
  std::string current_path = fs::current_path().c_str();
  if (boost::algorithm::ends_with(current_path, "fbcode")) {
    return current_path + "/presto_cpp/main/types/tests/data/" + fileName;
  }
  return current_path + "/data/" + fileName;
}
} // namespace

TEST(TestValues, valuesRowVector) {
  std::string str = slurp(getDataPath("ValuesNode.json"));

  json j = json::parse(str);
  std::shared_ptr<protocol::ValuesNode> p = j;

  testJsonRoundtrip(j, p);

  auto scopedPool = memory::getDefaultScopedMemoryPool();
  VeloxQueryPlanConverter converter(scopedPool.get());
  auto values = std::dynamic_pointer_cast<const core::ValuesNode>(
      converter.toVeloxQueryPlan(
          std::dynamic_pointer_cast<protocol::PlanNode>(p),
          nullptr,
          "20201107_130540_00011_wrpkw.1.2.3"));

  ASSERT_NE(values, nullptr);
  ASSERT_EQ(values->values().size(), 1);
  ASSERT_EQ(values->values()[0]->children().size(), 2);
  ASSERT_EQ(values->values()[0]->size(), 3);

  {
    auto v = values->values()[0]->childAt(0)->asFlatVector<int32_t>();
    ASSERT_EQ(v->valueAt(0), 1);
    ASSERT_EQ(v->valueAt(1), 2);
    ASSERT_EQ(v->valueAt(2), 3);
  }

  {
    auto v = values->values()[0]->childAt(1)->asFlatVector<StringView>();
    ASSERT_EQ(v->valueAt(0), StringView("a"));
    ASSERT_EQ(v->valueAt(1), StringView("b"));
    ASSERT_EQ(v->valueAt(2), StringView("c"));
  }
}

TEST(TestValues, valuesPlan) {
  // select a, b from (VALUES (1, 'a'), (2, 'b'), (3, 'c')) as t (a, b) where a
  // = 1;
  //
  std::string str = slurp(getDataPath("ValuesPipeTest.json"));

  json j = json::parse(str);
  std::shared_ptr<protocol::PlanFragment> p = j;

  testJsonRoundtrip(j, p);

  auto scopedPool = memory::getDefaultScopedMemoryPool();
  VeloxQueryPlanConverter converter(scopedPool.get());
  auto values = converter.toVeloxQueryPlan(
      std::dynamic_pointer_cast<protocol::OutputNode>(p->root)->source,
      nullptr,
      "20201107_130540_00011_wrpkw.1.2.3");

  ASSERT_EQ(values->name(), "Filter");
  ASSERT_EQ(values->sources()[0]->name(), "LocalPartition");
  ASSERT_EQ(values->sources()[0]->sources()[0]->name(), "Values");

  ASSERT_EQ(values->id(), "4");
  ASSERT_EQ(values->sources()[0]->sources()[0]->id(), "0");
}
