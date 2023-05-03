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

#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::presto::operators;

namespace facebook::velox::exec::test {

namespace {
class HashPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  HashPartitionFunctionSpec(
      RowTypePtr inputType,
      const std::vector<column_index_t>& keys,
      const std::vector<VectorPtr>& constValues = {})
      : inputType_{inputType}, keys_{keys}, constValues_{constValues} {}

  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions) const override {
    return std::make_unique<exec::HashPartitionFunction>(
        numPartitions, inputType_, keys_, constValues_);
  }

  std::string toString() const override {
    return fmt::format("HASH({})", folly::join(", ", keys_));
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "HashPartitionFunctionSpec";
    obj["inputType"] = inputType_->serialize();
    obj["keys"] = ISerializable::serialize(keys_);
    std::vector<velox::core::ConstantTypedExpr> constValues;
    constValues.reserve(constValues_.size());
    for (const auto& value : constValues_) {
      constValues.emplace_back(value);
    }
    obj["constants"] = ISerializable::serialize(constValues);
    return obj;
  }

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& obj,
      void* context) {
    const auto keys =
        ISerializable::deserialize<std::vector<column_index_t>>(obj["keys"]);
    const auto constTypeExprs =
        ISerializable::deserialize<std::vector<velox::core::ConstantTypedExpr>>(
            obj["constants"]);
    VELOX_CHECK_EQ(keys.size(), constTypeExprs.size());

    auto* pool = static_cast<memory::MemoryPool*>(context);
    std::vector<VectorPtr> constValues;
    constValues.reserve(constTypeExprs.size());
    for (const auto& value : constTypeExprs) {
      constValues.emplace_back(value->toConstantVector(pool));
    }
    return std::make_shared<HashPartitionFunctionSpec>(
        ISerializable::deserialize<RowType>(obj["inputType"]),
        keys,
        constValues);
  }

 private:
  const RowTypePtr inputType_;
  const std::vector<column_index_t> keys_;
  const std::vector<VectorPtr> constValues_;
};
} // namespace

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

  auto addPartitionAndSerializeNode(uint32_t numPartitions) {
    return [numPartitions](
               core::PlanNodeId nodeId,
               core::PlanNodePtr source) -> core::PlanNodePtr {
      const auto outputType = source->outputType();
      const std::vector<velox::core::TypedExprPtr> keys = {
          std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0")};
      return std::make_shared<PartitionAndSerializeNode>(
          nodeId,
          keys,
          numPartitions,
          ROW({"p", "d"}, {INTEGER(), VARBINARY()}),
          std::move(source),
          std::make_shared<HashPartitionFunctionSpec>(
              outputType, std::vector<column_index_t>{0}));
    };
  }

  auto addShuffleReadNode(velox::RowTypePtr& outputType) {
    return [&outputType](
               core::PlanNodeId nodeId,
               core::PlanNodePtr /* source */) -> core::PlanNodePtr {
      return std::make_shared<ShuffleReadNode>(nodeId, outputType);
    };
  }

  auto addShuffleWriteNode(
      const std::string& shuffleName,
      const std::string& serializedWriteInfo) {
    return [&shuffleName, &serializedWriteInfo](
               core::PlanNodeId nodeId,
               core::PlanNodePtr source) -> core::PlanNodePtr {
      return std::make_shared<ShuffleWriteNode>(
          nodeId, shuffleName, serializedWriteInfo, std::move(source));
    };
  }

  std::vector<RowVectorPtr> data_;
  RowTypePtr type_;
};

TEST_F(PlanNodeSerdeTest, partitionAndSerializeNode) {
  auto plan = exec::test::PlanBuilder()
                  .values(data_, true)
                  .addNode(addPartitionAndSerializeNode(4))
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
                  .addNode(addPartitionAndSerializeNode(numPartitions))
                  .localPartition({})
                  .addNode(addShuffleWriteNode(shuffleName, shuffleInfo))
                  .planNode();
  testSerde(plan);
}
} // namespace facebook::velox::exec::test
