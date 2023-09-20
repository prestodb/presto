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
  /// @param serdeRowType Type of the serialized data. This can be different
  /// from the input type. Input columns may appear in different order, some
  /// columns may be missing, some columns may appear multiple types. May
  /// contain no columns at all if only row count needs to be broadcasted.
  BroadcastWriteNode(
      const velox::core::PlanNodeId& id,
      const std::string& basePath,
      velox::RowTypePtr serdeRowType,
      velox::core::PlanNodePtr source)
      : velox::core::PlanNode(id),
        basePath_{basePath},
        serdeRowType_{serdeRowType},
        sources_{std::move(source)} {}

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const velox::RowTypePtr& outputType() const override {
    static const auto outputType = velox::ROW({velox::VARCHAR()});
    return outputType;
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const velox::RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const std::string& basePath() const {
    return basePath_;
  }

  /// The desired schema of the serialized data. May include a subset of input
  /// columns, some columns may be duplicated, some columns may be missing,
  /// columns may appear in different order.
  const velox::RowTypePtr& serdeRowType() const {
    return serdeRowType_;
  }

  std::string_view name() const override {
    return "BroadcastWrite";
  }

 private:
  void addDetails(std::stringstream& stream) const override {}

  const std::string basePath_;
  const velox::RowTypePtr serdeRowType_;
  const std::vector<velox::core::PlanNodePtr> sources_;
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
