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
#include <unordered_map>

#include "velox/core/PlanNode.h"
#include "velox/exec/fuzzer/PrestoSql.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"

namespace facebook::velox::exec::test {
bool ReferenceQueryRunner::isSupportedDwrfType(const TypePtr& type) {
  if (type->isDate() || type->isIntervalDayTime() || type->isUnKnown()) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isSupportedDwrfType(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

std::unordered_map<std::string, std::vector<velox::RowVectorPtr>>
ReferenceQueryRunner::getAllTables(const core::PlanNodePtr& plan) {
  std::unordered_map<std::string, std::vector<velox::RowVectorPtr>> result;
  if (const auto valuesNode =
          std::dynamic_pointer_cast<const core::ValuesNode>(plan)) {
    result.insert({getTableName(*valuesNode), valuesNode->values()});
  } else {
    for (const auto& source : plan->sources()) {
      auto tablesAndNames = getAllTables(source);
      result.insert(tablesAndNames.begin(), tablesAndNames.end());
    }
  }
  return result;
}
} // namespace facebook::velox::exec::test
