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

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

using namespace facebook::presto::protocol;

class AggregationNodeTest : public ::testing::Test {};

namespace {
VariableReferenceExpression variable(std::string name, std::string type) {
  VariableReferenceExpression v;
  v._type = "variable";
  v.name = std::move(name);
  v.type = std::move(type);
  return v;
}
} // namespace

// Verifies that protocol::AggregationNode::aggregationOutputs survives a JSON
// round-trip and preserves the explicit list order. Java sends this field in
// LinkedHashMap insertion order so native workers can build the
// AggregationNode output schema in the same order as the Java planner.
// Without it, iterating node->aggregations (std::map<VRE>, sorted by name)
// can shift channel positions and cause type mismatches at exchange
// operators (see prestodb/presto#27902).
TEST_F(AggregationNodeTest, aggregationOutputsRoundTripPreservesOrder) {
  // Names chosen so sorted order [count_a, sum_b] differs from insertion
  // order [sum_b, count_a] -- mirroring the production scenario where
  // `approx_distinct_*` sorted before `sum_*` while Java inserted sums first.
  AggregationNode original;
  original.id = "1";
  original.aggregationOutputs =
      std::make_shared<List<VariableReferenceExpression>>(
          List<VariableReferenceExpression>{
              variable("sum_b", "double"),
              variable("count_a", "bigint"),
          });
  original.groupingSets.groupingKeys = {};
  original.groupingSets.groupingSetCount = 1;
  original.groupingSets.globalGroupingSets = {0};
  original.step = AggregationNodeStep::PARTIAL;

  json j = original;
  AggregationNode parsed = j;

  ASSERT_NE(parsed.aggregationOutputs, nullptr);
  ASSERT_EQ(parsed.aggregationOutputs->size(), 2);
  EXPECT_EQ((*parsed.aggregationOutputs)[0].name, "sum_b");
  EXPECT_EQ((*parsed.aggregationOutputs)[0].type, "double");
  EXPECT_EQ((*parsed.aggregationOutputs)[1].name, "count_a");
  EXPECT_EQ((*parsed.aggregationOutputs)[1].type, "bigint");
}

// Verifies backward compatibility: when JSON omits aggregationOutputs (older
// coordinators), deserialization succeeds and the field is left empty.
// PrestoToVeloxQueryPlan.cpp falls back to iterating `aggregations` in that
// case.
TEST_F(AggregationNodeTest, missingAggregationOutputsIsBackwardCompatible) {
  std::string str = R"({
        "@type": ".AggregationNode",
        "id": "1",
        "aggregations": {},
        "groupingSets": {
          "groupingKeys": [],
          "groupingSetCount": 1,
          "globalGroupingSets": [0]
        },
        "preGroupedVariables": [],
        "step": "PARTIAL"
      })";

  json j = json::parse(str);
  AggregationNode parsed = j;

  EXPECT_EQ(parsed.aggregationOutputs, nullptr);
  EXPECT_TRUE(parsed.aggregations.empty());
}
