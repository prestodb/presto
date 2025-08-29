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
#include "velox/exec/ExchangeClient.h"

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
            "ShuffleWrite"),
        numPartitions_{planNode->numPartitions()},
        serializedShuffleWriteInfo_{planNode->serializedShuffleWriteInfo()} {
    const auto& shuffleName = planNode->shuffleName();
    shuffleFactory_ = ShuffleInterfaceFactory::factory(shuffleName);
    VELOX_CHECK_NOT_NULL(
        shuffleFactory_,
        "Failed to create shuffle write interface: Shuffle factory "
        "with name '{}' is not registered.",
        shuffleName);
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  /// The input of this operator has 3 columns:
  /// (1) partition number (INTEGER);
  /// (2) serialized key (VARBINARY)
  /// (3) serialized data (VARBINARY)
  ///
  /// When replicateNullsAndAny is true, there is an extra boolean column that
  /// indicated whether a row should be replicated to all partitions.
  void addInput(RowVectorPtr input) override {
    constexpr int kPartition = 0;
    constexpr int kKey = 1;
    constexpr int kData = 2;
    constexpr int kReplicateNullsAndAny = 3;

    checkCreateShuffleWriter();
    auto partitions = input->childAt(kPartition)->as<SimpleVector<int32_t>>();
    auto serializedKeys = input->childAt(kKey)->as<SimpleVector<StringView>>();
    auto serializedData = input->childAt(kData)->as<SimpleVector<StringView>>();
    SimpleVector<bool>* replicate = nullptr;
    if (input->type()->size() == 4) {
      replicate =
          input->childAt(kReplicateNullsAndAny)->as<SimpleVector<bool>>();
    }

    for (auto i = 0; i < input->size(); ++i) {
      auto data = serializedData->valueAt(i);
      auto key = serializedKeys->valueAt(i);
      if (replicate && replicate->valueAt(i)) {
        for (auto partition = 0; partition < numPartitions_; ++partition) {
          CALL_SHUFFLE(
              shuffle_->collect(
                  partition,
                  std::string_view(key.data(), key.size()),
                  std::string_view(data.data(), data.size())),
              "collect");
        }
      } else {
        auto partition = partitions->valueAt(i);
        CALL_SHUFFLE(
            shuffle_->collect(
                partition,
                std::string_view(key.data(), key.size()),
                std::string_view(data.data(), data.size())),
            "collect");
      }
    }
    auto lockedStats = stats_.wlock();
    lockedStats->addOutputVector(input->estimateFlatSize(), input->size());
  }

  void noMoreInput() override {
    Operator::noMoreInput();

    checkCreateShuffleWriter();
    CALL_SHUFFLE(shuffle_->noMoreData(true), "noMoreData");

    recordShuffleWriteClientStats();
  }

  void recordShuffleWriteClientStats() {
    auto lockedStats = stats_.wlock();
    const auto shuffleStats = shuffle_->stats();
    for (const auto& [name, value] : shuffleStats) {
      lockedStats->runtimeStats[name] = RuntimeMetric(value);
    }

    auto backgroundCpuTimeMs =
        shuffleStats.find(ExchangeClient::kBackgroundCpuTimeMs);
    if (backgroundCpuTimeMs != shuffleStats.end()) {
      const CpuWallTiming backgroundTiming{
          static_cast<uint64_t>(1),
          0,
          static_cast<uint64_t>(backgroundCpuTimeMs->second) *
              Timestamp::kNanosecondsInMillisecond};
      lockedStats->backgroundTiming.clear();
      lockedStats->backgroundTiming.add(backgroundTiming);
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
  void checkCreateShuffleWriter() {
    if (shuffle_ == nullptr) {
      shuffle_ = shuffleFactory_->createWriter(
          serializedShuffleWriteInfo_, operatorCtx_->pool());
    }
  }

  const uint32_t numPartitions_;
  const std::string serializedShuffleWriteInfo_;
  ShuffleInterfaceFactory* shuffleFactory_;
  std::shared_ptr<ShuffleWriter> shuffle_;
};

#undef CALL_SHUFFLE
} // namespace

folly::dynamic ShuffleWriteNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["numPartitions"] = numPartitions_;
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
      obj["numPartitions"].asInt(),
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
