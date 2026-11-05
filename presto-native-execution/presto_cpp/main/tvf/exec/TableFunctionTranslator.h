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

#include "presto_cpp/main/tvf/core/TableFunctionProcessorNode.h"
#include "presto_cpp/main/tvf/exec/LeafTableFunctionOperator.h"
#include "presto_cpp/main/tvf/exec/TableFunctionOperator.h"

#include "velox/exec/Operator.h"

namespace facebook::presto::tvf {

// Custom translation logic to hook into Velox Driver.
class TableFunctionTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) {
    if (auto tableFunctionProcessorNode =
            std::dynamic_pointer_cast<const TableFunctionProcessorNode>(node)) {
      if (tableFunctionProcessorNode->sources().empty()) {
        return std::make_unique<LeafTableFunctionOperator>(
            id, ctx, tableFunctionProcessorNode);
      }
      return std::make_unique<TableFunctionOperator>(
          id, ctx, tableFunctionProcessorNode);
    }
    return nullptr;
  }
};

} // namespace facebook::presto::tvf
