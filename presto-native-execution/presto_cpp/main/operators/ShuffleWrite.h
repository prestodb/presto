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
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

class ShuffleWriteNode : public velox::core::PlanNode {
 public:
  ShuffleWriteNode(
      const velox::core::PlanNodeId& id,
      uint32_t numPartitions,
      const std::string& shuffleName,
      const std::string& serializedShuffleWriteInfo,
      velox::core::PlanNodePtr source)
      : velox::core::PlanNode(id),
        numPartitions_{numPartitions},
        shuffleName_{shuffleName},
        serializedShuffleWriteInfo_(serializedShuffleWriteInfo),
        sources_{std::move(source)} {}

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const velox::RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  uint32_t numPartitions() const {
    return numPartitions_;
  }

  const std::string& shuffleName() const {
    return shuffleName_;
  }

  const std::string& serializedShuffleWriteInfo() const {
    return serializedShuffleWriteInfo_;
  }

  std::string_view name() const override {
    return "ShuffleWrite";
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << numPartitions_ << ", " << shuffleName_;
  }

  const uint32_t numPartitions_;
  const std::string shuffleName_;
  const std::string serializedShuffleWriteInfo_;
  const std::vector<velox::core::PlanNodePtr> sources_;
};

class ShuffleWriteTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) override;
};
} // namespace facebook::presto::operators
