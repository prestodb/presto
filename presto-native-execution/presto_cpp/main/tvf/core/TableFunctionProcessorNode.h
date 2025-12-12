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
  TableFunctionProcessorNode(
      velox::core::PlanNodeId id,
      std::string name,
      TableFunctionHandlePtr handle,
      std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys,
      std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys,
      std::vector<velox::core::SortOrder> sortingOrders,
      velox::RowTypePtr outputType,
      std::vector<velox::column_index_t> requiredColumns,
      std::vector<velox::core::PlanNodePtr> sources);

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  bool canSpill(const velox::core::QueryConfig& queryConfig) const override {
    return false;
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

  const std::vector<velox::column_index_t>& requiredColumns() const {
    return requiredColumns_;
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

  const velox::RowTypePtr outputType_;

  const std::vector<velox::column_index_t> requiredColumns_;

  const std::vector<velox::core::PlanNodePtr> sources_;
};

using TableFunctionProcessorNodePtr =
    std::shared_ptr<const TableFunctionProcessorNode>;

} // namespace facebook::presto::tvf
