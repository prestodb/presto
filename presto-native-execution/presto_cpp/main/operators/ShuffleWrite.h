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

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const ShuffleWriteNode& other) {
      id_ = other.id();
      numPartitions_ = other.numPartitions();
      shuffleName_ = other.shuffleName();
      serializedShuffleWriteInfo_ = other.serializedShuffleWriteInfo();
      source_ = other.sources()[0];
    }

    Builder& id(velox::core::PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& numPartitions(uint32_t numPartitions) {
      numPartitions_ = numPartitions;
      return *this;
    }

    Builder& shuffleName(std::string shuffleName) {
      shuffleName_ = std::move(shuffleName);
      return *this;
    }

    Builder& serializedShuffleWriteInfo(
        std::string serializedShuffleWriteInfo) {
      serializedShuffleWriteInfo_ = std::move(serializedShuffleWriteInfo);
      return *this;
    }

    Builder& source(velox::core::PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<ShuffleWriteNode> build() const {
      VELOX_USER_CHECK(id_.has_value(), "ShuffleWriteNode id is not set");
      VELOX_USER_CHECK(
          numPartitions_.has_value(),
          "ShuffleWriteNode numPartitions_ is not set");
      VELOX_USER_CHECK(
          shuffleName_.has_value(), "ShuffleWriteNode shuffleName is not set");
      VELOX_USER_CHECK(
          serializedShuffleWriteInfo_.has_value(),
          "ShuffleWriteNode serializedShuffleWriteInfo is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "ShuffleWriteNode source is not set");

      return std::make_shared<ShuffleWriteNode>(
          id_.value(),
          numPartitions_.value(),
          shuffleName_.value(),
          serializedShuffleWriteInfo_.value(),
          source_.value());
    }

   private:
    std::optional<velox::core::PlanNodeId> id_;
    std::optional<uint32_t> numPartitions_;
    std::optional<std::string> shuffleName_;
    std::optional<std::string> serializedShuffleWriteInfo_;
    std::optional<velox::core::PlanNodePtr> source_;
  };

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
