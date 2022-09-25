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
#pragma once

#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

class ShuffleWriteNode : public velox::core::PlanNode {
 public:
  ShuffleWriteNode(
      const velox::core::PlanNodeId& id,
      ShuffleInterface* shuffle,
      velox::core::PlanNodePtr source)
      : velox::core::PlanNode(id),
        shuffle_{shuffle},
        sources_{std::move(source)} {}

  const velox::RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  ShuffleInterface* shuffle() const {
    return shuffle_;
  }

  std::string_view name() const override {
    return "ShuffleWrite";
  }

 private:
  void addDetails(std::stringstream& stream) const override {}

  ShuffleInterface* shuffle_;

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