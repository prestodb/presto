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

#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto::operators;

namespace facebook::velox::exec::test {
class PlanNodeSerdeTest : public testing::Test,
                          public velox::test::VectorTestBase {
 protected:
  PlanNodeSerdeTest() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    presto::registerPrestoPlanNodeSerDe();

    Type::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    exec::registerPartitionFunctionSerDe();

    data_ = {makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3}),
        makeFlatVector<int32_t>({10, 20, 30}),
        makeConstant(true, 3),
    })};
    type_ = asRowType(data_[0]->type());
  }

  void testSerde(const core::PlanNodePtr& plan) {
    auto serialized = plan->serialize();

    auto copy =
        velox::ISerializable::deserialize<core::PlanNode>(serialized, pool());
    ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
  }

  static std::vector<std::string> reverseColumns(const RowTypePtr& rowType) {
    auto names = rowType->names();
    std::reverse(names.begin(), names.end());
    return names;
  }

  std::vector<RowVectorPtr> data_;
  RowTypePtr type_;
};

TEST_F(PlanNodeSerdeTest, partitionAndSerializeNode) {
  auto plan = exec::test::PlanBuilder()
                  .values(data_, true)
                  .addNode(addPartitionAndSerializeNode(
                      4, false, reverseColumns(asRowType(data_[0]->type()))))
                  .localPartition({})
                  .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, shuffleReadNode) {
  auto plan = exec::test::PlanBuilder()
                  .addNode(addShuffleReadNode(type_))
                  .project(type_->names())
                  .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, shuffleWriteNode) {
  const int numPartitions = 4;
  const std::string shuffleName("shuffleWriteNodeSerdeTest");
  static constexpr std::string_view kTestShuffleInfoFormat =
      "{{\n"
      "  \"numPartitions\": {},\n"
      "  \"maxBytesPerPartition\": {}\n"
      "}}";
  const std::string shuffleInfo =
      fmt::format(kTestShuffleInfoFormat, numPartitions, 1 << 20);
  auto plan = exec::test::PlanBuilder()
                  .values(data_, true)
                  .addNode(addPartitionAndSerializeNode(numPartitions, false))
                  .localPartition({})
                  .addNode(addShuffleWriteNode(shuffleName, shuffleInfo))
                  .planNode();
  testSerde(plan);
}

TEST_F(PlanNodeSerdeTest, broadcastWriteNode) {
  const std::string basePath("/tmp/query-20130101-0000-001");
  auto broadcastWritePlan = exec::test::PlanBuilder()
                                .values(data_, true)
                                .addNode(addBroadcastWriteNode(basePath))
                                .partitionedOutputBroadcast()
                                .planNode();
  testSerde(broadcastWritePlan);
}
} // namespace facebook::velox::exec::test
