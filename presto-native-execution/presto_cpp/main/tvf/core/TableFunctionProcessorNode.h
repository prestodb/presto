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

#include "presto_cpp/main/tvf/spi/TableFunction.h"

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"

namespace facebook::presto::tvf {

class TableFunctionProcessorNode : public velox::core::PlanNode {
 public:
  struct PassThroughColumnSpecification {
   public:
    PassThroughColumnSpecification(
        bool isPartitioningColumn,
        velox::column_index_t inputChannel,
        velox::column_index_t indexChannel)
        : isPartitioningColumn_(isPartitioningColumn),
          inputChannel_(inputChannel),
          indexChannel_(indexChannel) {}

    velox::column_index_t inputChannel() const {
      return inputChannel_;
    }

    velox::column_index_t indexChannel() const {
      return indexChannel_;
    }

    bool isPartitioningColumn() const {
      return isPartitioningColumn_;
    }

    folly::dynamic serialize() const {
      folly::dynamic obj = folly::dynamic::object;
      obj["isPartitioningColumn"] = isPartitioningColumn_;
      obj["inputChannel"] = inputChannel_;
      obj["indexChannel"] = indexChannel_;
      return obj;
    }

    static PassThroughColumnSpecification deserialize(
        const folly::dynamic& obj) {
      return {
          obj["isPartitioningColumn"].asBool(),
          static_cast<velox::column_index_t>(obj["inputChannel"].asInt()),
          static_cast<velox::column_index_t>(obj["indexChannel"].asInt())};
    }

   private:
    const bool isPartitioningColumn_;
    const velox::column_index_t inputChannel_;
    const velox::column_index_t indexChannel_;
  };

  TableFunctionProcessorNode(
      velox::core::PlanNodeId id,
      std::string name,
      TableFunctionHandlePtr handle,
      std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys,
      std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys,
      std::vector<velox::core::SortOrder> sortingOrders,
      bool pruneWhenEmpty,
      velox::RowTypePtr outputType,
      std::vector<std::vector<velox::column_index_t>> requiredColumns,
      std::unordered_map<velox::column_index_t, velox::column_index_t>
          markerChannels,
      std::vector<PassThroughColumnSpecification> passThroughColumns,
      std::vector<velox::core::PlanNodePtr> sources);

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  bool canSpill(const velox::core::QueryConfig& queryConfig) const override {
    return false;
  }

  bool pruneWhenEmpty() const {
    return pruneWhenEmpty_;
  }

  const velox::RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  const velox::RowTypePtr& outputType() const override {
    return outputType_;
  };

  std::string_view name() const override {
    return "TableFunctionProcessor";
  }

  const std::string functionName() const {
    return functionName_;
  }

  const std::shared_ptr<const TableFunctionHandle> handle() const {
    return handle_;
  }

  const std::vector<velox::core::FieldAccessTypedExprPtr>& partitionKeys()
      const {
    return partitionKeys_;
  }

  const std::vector<velox::core::FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  const std::vector<velox::core::SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  const std::vector<std::vector<velox::column_index_t>>& requiredColumns()
      const {
    return requiredColumns_;
  }

  const std::unordered_map<velox::column_index_t, velox::column_index_t>&
  markerChannels() const {
    return markerChannels_;
  }

  const std::vector<PassThroughColumnSpecification>& passThroughColumns()
      const {
    return passThroughColumns_;
  }

  bool requiresSplits() const override {
    if (sources_.empty()) {
      // This is a leaf operator that needs splits then.
      return true;
    }

    return false;
  }

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::string functionName_;

  TableFunctionHandlePtr handle_;

  std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys_;
  std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys_;
  std::vector<velox::core::SortOrder> sortingOrders_;

  bool pruneWhenEmpty_;

  const velox::RowTypePtr outputType_;

  const std::vector<std::vector<velox::column_index_t>> requiredColumns_;

  const std::unordered_map<velox::column_index_t, velox::column_index_t>
      markerChannels_;

  const std::vector<PassThroughColumnSpecification> passThroughColumns_;

  const std::vector<velox::core::PlanNodePtr> sources_;
};

using TableFunctionProcessorNodePtr =
    std::shared_ptr<const TableFunctionProcessorNode>;

} // namespace facebook::presto::tvf
