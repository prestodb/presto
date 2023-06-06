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
#include "presto_cpp/main/operators/ShuffleWrite.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

#define CALL_SHUFFLE(call, methodName)                                \
  try {                                                               \
    call;                                                             \
  } catch (const VeloxException& e) {                                 \
    throw;                                                            \
  } catch (const std::exception& e) {                                 \
    VELOX_FAIL("ShuffleWriter::{} failed: {}", methodName, e.what()); \
  }

class ShuffleWriteOperator : public Operator {
 public:
  ShuffleWriteOperator(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const ShuffleWriteNode>& planNode)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "ShuffleWrite") {
    const auto& shuffleName = planNode->shuffleName();
    auto shuffleFactory = ShuffleInterfaceFactory::factory(shuffleName);
    VELOX_CHECK(
        shuffleFactory != nullptr,
        fmt::format(
            "Failed to create shuffle write interface: Shuffle factory "
            "with name '{}' is not registered.",
            shuffleName));
    shuffle_ = shuffleFactory->createWriter(
        planNode->serializedShuffleWriteInfo(), operatorCtx_->pool());
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    auto partitions = input->childAt(0)->as<SimpleVector<int32_t>>();
    auto serializedRows = input->childAt(1)->as<SimpleVector<StringView>>();
    for (auto i = 0; i < input->size(); ++i) {
      auto partition = partitions->valueAt(i);
      auto data = serializedRows->valueAt(i);
      CALL_SHUFFLE(
          shuffle_->collect(
              partition, std::string_view(data.data(), data.size())),
          "collect");
    }
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    CALL_SHUFFLE(shuffle_->noMoreData(true), "noMoreData");

    {
      auto lockedStats = stats_.wlock();
      for (const auto& [name, value] : shuffle_->stats()) {
        lockedStats->runtimeStats[name] = RuntimeMetric(value);
      }
    }
  }

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  std::shared_ptr<ShuffleWriter> shuffle_;
};

#undef CALL_SHUFFLE
} // namespace

folly::dynamic ShuffleWriteNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["shuffleName"] = ISerializable::serialize<std::string>(shuffleName_);
  obj["shuffleWriteInfo"] =
      ISerializable::serialize<std::string>(serializedShuffleWriteInfo_);
  obj["sources"] = ISerializable::serialize(sources_);
  return obj;
}

velox::core::PlanNodePtr ShuffleWriteNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<ShuffleWriteNode>(
      deserializePlanNodeId(obj),
      ISerializable::deserialize<std::string>(obj["shuffleName"], context),
      ISerializable::deserialize<std::string>(obj["shuffleWriteInfo"], context),
      ISerializable::deserialize<std::vector<velox::core::PlanNode>>(
          obj["sources"], context)[0]);
}

std::unique_ptr<Operator> ShuffleWriteTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto shuffleWriteNode =
          std::dynamic_pointer_cast<const ShuffleWriteNode>(node)) {
    return std::make_unique<ShuffleWriteOperator>(id, ctx, shuffleWriteNode);
  }
  return nullptr;
}
} // namespace facebook::presto::operators
