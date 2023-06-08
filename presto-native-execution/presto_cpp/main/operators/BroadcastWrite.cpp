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
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "presto_cpp/main/operators/FileBroadcast.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

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
            "BroadcastWrite") {
    const auto& basePath = planNode->broadcastWriteBasePath();
    // fmt::format("{}/{}", planNode->broadcastWriteBasePath(), makeUuid());
    auto fileBroadcast = FileBroadcast(basePath);
    fileBroadcastWriter_ = fileBroadcast.createWriter(
        operatorCtx_->pool(), planNode->sources().back()->outputType());
  }

  static std::string makeUuid() {
    return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    // write pages to buffer/file
    input_ = std::move(input);
    fileBroadcastWriter_->collect(input_);
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    fileBroadcastWriter_->noMoreData();
  }

  RowVectorPtr getOutput() override {
    if (!noMoreInput_ || !input_) {
      return nullptr;
    }
    input_.reset();
    return fileBroadcastWriter_->fileStats();
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  std::shared_ptr<BroadcastFileWriter> fileBroadcastWriter_;
};
} // namespace

folly::dynamic BroadcastWriteNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["broadcastWriteBasePath"] =
      ISerializable::serialize<std::string>(basePath_);
  obj["sources"] = ISerializable::serialize(sources_);
  return obj;
}

velox::core::PlanNodePtr BroadcastWriteNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<BroadcastWriteNode>(
      deserializePlanNodeId(obj),
      "/tmp",
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
