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
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/BroadcastFactory.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

// TODO: This is a copy from Exchange.cpp. We should refactor
// such that this method is globally accessible from a single location. This is
// to prevent diverges of serde options during write and read.
std::unique_ptr<VectorSerde::Options> getVectorSerdeOptions(
    const core::QueryConfig& queryConfig,
    VectorSerde::Kind kind) {
  std::unique_ptr<VectorSerde::Options> options =
      kind == VectorSerde::Kind::kPresto
      ? std::make_unique<serializer::presto::PrestoVectorSerde::PrestoOptions>()
      : std::make_unique<VectorSerde::Options>();
  options->compressionKind =
      common::stringToCompressionKind(queryConfig.shuffleCompressionKind());
  return options;
}

// BroadcastWriteOperator writes input RowVectors to specified file.
class BroadcastWriteOperator : public Operator {
 public:
  BroadcastWriteOperator(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const BroadcastWriteNode>& planNode)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "BroadcastWrite"),
        serdeRowType_{planNode->serdeRowType()},
        serdeChannels_(calculateOutputChannels(
            planNode->inputType(),
            planNode->serdeRowType(),
            planNode->serdeRowType())) {
    auto fileBroadcast = BroadcastFactory(planNode->basePath());
    fileBroadcastWriter_ = fileBroadcast.createWriter(
        8 << 20,
        operatorCtx_->pool(),
        getVectorSerdeOptions(ctx->queryConfig(), VectorSerde::Kind::kPresto));
  }

  bool needsInput() const override {
    return true;
  }

  void addInput(RowVectorPtr input) override {
    RowVectorPtr reorderedInput = nullptr;
    if (serdeRowType_->size() > 0 && serdeChannels_.empty()) {
      reorderedInput = std::move(input);
    } else {
      std::vector<VectorPtr> outputColumns;
      outputColumns.reserve(serdeChannels_.size());
      for (auto i : serdeChannels_) {
        outputColumns.push_back(input->childAt(i));
      }

      reorderedInput = std::make_shared<RowVector>(
          input->pool(),
          serdeRowType_,
          nullptr /*nulls*/,
          input->size(),
          outputColumns);
    }

    fileBroadcastWriter_->write(reorderedInput);
    auto lockedStats = stats_.wlock();
    lockedStats->addOutputVector(
        reorderedInput->estimateFlatSize(), reorderedInput->size());
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    fileBroadcastWriter_->noMoreData();
  }

  RowVectorPtr getOutput() override {
    if (!noMoreInput_ || finished_) {
      return nullptr;
    }

    finished_ = true;
    return fileBroadcastWriter_->fileStats();
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  // May be empty.
  const RowTypePtr serdeRowType_;
  // Empty if column order in the serdeRowType_ is exactly the same as in input
  // or serdeRowType_ has no columns.
  const std::vector<column_index_t> serdeChannels_;
  std::unique_ptr<BroadcastFileWriter> fileBroadcastWriter_;
  bool finished_{false};
};
} // namespace

folly::dynamic BroadcastWriteNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["broadcastWriteBasePath"] =
      ISerializable::serialize<std::string>(basePath_);
  obj["rowType"] = serdeRowType_->serialize();
  obj["sources"] = ISerializable::serialize(sources_);
  return obj;
}

velox::core::PlanNodePtr BroadcastWriteNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<BroadcastWriteNode>(
      deserializePlanNodeId(obj),
      ISerializable::deserialize<std::string>(
          obj["broadcastWriteBasePath"], context),
      ISerializable::deserialize<RowType>(obj["rowType"]),
      ISerializable::deserialize<std::vector<velox::core::PlanNode>>(
          obj["sources"], context)[0]);
}

std::unique_ptr<Operator> BroadcastWriteTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto broadcastWriteNode =
          std::dynamic_pointer_cast<const BroadcastWriteNode>(node)) {
    return std::make_unique<BroadcastWriteOperator>(
        id, ctx, broadcastWriteNode);
  }
  return nullptr;
}
} // namespace facebook::presto::operators
