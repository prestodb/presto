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
class ShuffleReadNode : public velox::core::PlanNode {
 public:
  ShuffleReadNode(const velox::core::PlanNodeId& id, velox::RowTypePtr type)
      : PlanNode(id), outputType_(type) {}

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const ShuffleReadNode& other) {
      id_ = other.id();
      outputType_ = other.outputType();
    }

    Builder& id(velox::core::PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& outputType(velox::RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    std::shared_ptr<ShuffleReadNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ShuffleReadNode id is not set");
      VELOX_USER_CHECK(
          outputType_.has_value(), "ShuffleReadNode outputType is not set");

      return std::make_shared<ShuffleReadNode>(
          id_.value(), outputType_.value());
    }

   private:
    std::optional<velox::core::PlanNodeId> id_;
    std::optional<velox::RowTypePtr> outputType_;
  };

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    static const std::vector<velox::core::PlanNodePtr> kEmptySources;
    return kEmptySources;
  }

  bool requiresExchangeClient() const override {
    return true;
  }

  bool requiresSplits() const override {
    return true;
  }

  std::string_view name() const override {
    return "ShuffleRead";
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    // Nothing to add
  }

  const velox::RowTypePtr outputType_;
};

class ShuffleReadTranslator : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node,
      std::shared_ptr<velox::exec::ExchangeClient> exchangeClient) override;
};
} // namespace facebook::presto::operators
