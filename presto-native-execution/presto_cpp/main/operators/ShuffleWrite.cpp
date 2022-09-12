/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
        shuffle_{planNode->shuffle()} {}

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    auto partitions = input->childAt(0)->as<SimpleVector<int32_t>>();
    auto serializedRows = input->childAt(1)->as<SimpleVector<StringView>>();
    for (auto i = 0; i < input->size(); ++i) {
      auto partition = partitions->valueAt(i);
      auto data = serializedRows->valueAt(i);

      shuffle_->collect(partition, std::string_view(data.data(), data.size()));
    }
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    shuffle_->noMoreData(true);
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
  ShuffleInterface* shuffle_;
};
} // namespace

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