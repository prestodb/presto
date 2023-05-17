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

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

/// BroadcastWriteNode represents node which broadcasts using file system.
class BroadcastWriteNode : public velox::core::PlanNode {
 public:
  BroadcastWriteNode(
      const velox::core::PlanNodeId& id,
      const std::string& basePath,
      velox::core::PlanNodePtr source)
      : velox::core::PlanNode(id),
        basePath_{basePath},
        sources_{std::move(source)},
        outputType_{velox::ROW({velox::VARCHAR()})} {}

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const std::string& basePath() const {
    return basePath_;
  }

  std::string_view name() const override {
    return "BroadcastWrite";
  }

 private:
  void addDetails(std::stringstream& stream) const override {}

  const std::string basePath_;
  const std::vector<velox::core::PlanNodePtr> sources_;
  const velox::RowTypePtr outputType_;
};

class BroadcastWriteTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) override;
};
} // namespace facebook::presto::operators
