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
#include "velox/core/PlanNode.h"

namespace facebook::presto::tvf {

class TableFunctionNode : public velox::core::PlanNode {
 public:
  /// @param windowColumnNames specifies the output column
  /// names for each window function column. SoWi
  /// windowColumnNames.length() = windowFunctions.length().
  TableFunctionNode(
      velox::core::PlanNodeId id,
      const std::string& name,
      TableFunctionHandlePtr handle,
      velox::RowTypePtr outputType,
      RequiredColumnsMap requiredColumns,
      velox::core::PlanNodePtr source);

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
    return "TableFunction";
  }

  const std::string functionName() const {
    return functionName_;
  }

  const std::shared_ptr<const TableFunctionHandle> handle() const {
    return handle_;
  }

  const RequiredColumnsMap requiredColumns() const {
    return requiredColumns_;
  }

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::string functionName_;

  TableFunctionHandlePtr handle_;

  const velox::RowTypePtr outputType_;

  const RequiredColumnsMap requiredColumns_;

  const std::vector<velox::core::PlanNodePtr> sources_;
};

using TableFunctionNodePtr = std::shared_ptr<const TableFunctionNode>;

} // namespace facebook::presto::tvf
