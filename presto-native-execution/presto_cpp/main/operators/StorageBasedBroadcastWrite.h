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
#pragma once

#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

class StorageBasedBroadcastWriteNode : public velox::core::PlanNode {
 public:
  StorageBasedBroadcastWriteNode(
      const velox::core::PlanNodeId& id,
      velox::core::PlanNodePtr source,
      ShuffleInterface* shuffle)
      : velox::core::PlanNode(id),
        sources_{std::move(source)},
        shuffle_(shuffle) {}

  const velox::RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "StorageBasedBroadcastWrite";
  }

  ShuffleInterface* shuffle() const {
    return shuffle_;
  }

 private:
  void addDetails(std::stringstream& stream) const override {
  }

  const std::vector<velox::core::PlanNodePtr> sources_;

  ShuffleInterface* shuffle_;
};

class StorageBasedBroadcastWriteTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) override;
};
} // namespace facebook::presto::operators