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

/// Partitions the input row based on partition function and serializes the
/// entire row.
class PartitionAndSerializeNode : public velox::core::PlanNode {
 public:
  static constexpr std::string_view kPartitionColumnNameDefault = "partition";
  static constexpr std::string_view kDataColumnNameDefault = "data";

  PartitionAndSerializeNode(
      const velox::core::PlanNodeId& id,
      std::vector<velox::core::TypedExprPtr> keys,
      uint32_t numPartitions,
      velox::RowTypePtr outputType,
      velox::core::PlanNodePtr source,
      velox::core::PartitionFunctionFactory partitionFunctionFactory)
      : velox::core::PlanNode(id),
        keys_(std::move(keys)),
        numPartitions_(numPartitions),
        outputType_{std::move(outputType)},
        sources_({std::move(source)}),
        partitionFunctionFactory_(std::move(partitionFunctionFactory)) {
    // Only verify output types are correct. Note column names are not enforced
    // in the following check.
    VELOX_USER_CHECK(
        velox::ROW(
            {"partition", "data"}, {velox::INTEGER(), velox::VARBINARY()})
            ->equivalent(*outputType_));
    VELOX_USER_CHECK_NOT_NULL(
        partitionFunctionFactory_,
        "Partition function factory cannot be null.");
  }

  const velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const std::vector<velox::core::TypedExprPtr>& keys() const {
    return keys_;
  }

  uint32_t numPartitions() const {
    return numPartitions_;
  }

  const velox::core::PartitionFunctionFactory& partitionFunctionFactory()
      const {
    return partitionFunctionFactory_;
  }

  std::string_view name() const override {
    return "PartitionAndSerialize";
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<velox::core::TypedExprPtr> keys_;
  const uint32_t numPartitions_;
  const velox::RowTypePtr outputType_;
  const std::vector<velox::core::PlanNodePtr> sources_;
  const velox::core::PartitionFunctionFactory partitionFunctionFactory_;
};

class PartitionAndSerializeTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) override;
};
} // namespace facebook::presto::operators