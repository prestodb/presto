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

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"

namespace facebook::presto {

class VeloxPlanChecker {
 public:
  VeloxPlanChecker();

  static protocol::PlanConversionResponse checkPlanFragment(
      const std::string& planFragmentJson,
      velox::memory::MemoryPool* pool);

  /// Add overloads of this method for different plan node types as needed.
  /// By default, all plan nodes are considered valid.
  template <typename T>
  bool isValidPlanNode(const T* /*node*/) const {
    return true;
  }

  bool isValidPlanNode(const velox::core::NestedLoopJoinNode* node) const;
  bool isValidPlanNode(const velox::core::ProjectNode* node) const;
  bool isValidPlanNode(const velox::core::FilterNode* node) const;
  bool isValidPlanNode(const velox::core::HashJoinNode* node) const;
  bool isValidPlanNode(const velox::core::AggregationNode* node) const;
  bool isValidPlanNode(const velox::core::TableScanNode* node) const;

 private:
  bool failOnNestedLoopJoin_;
};

} // namespace facebook::presto
