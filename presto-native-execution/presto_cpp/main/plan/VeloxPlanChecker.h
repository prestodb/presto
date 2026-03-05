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

/// Specifies the purpose of plan conversion.
enum class PlanConversionPurpose { kExecution, kValidation };

class VeloxPlanChecker {
 public:
  explicit VeloxPlanChecker(velox::memory::MemoryPool* pool) : pool_(pool) {}

  protocol::PlanConversionResponse checkPlanFragment(
      const std::string& planFragmentJson) const;

  static void validatePlanFragment(const velox::core::PlanFragment& fragment);

  /// Add overloads of this method for different plan node types as needed.
  /// By default, all plan nodes are considered valid.
  template <typename T>
  static bool isValidPlanNode(const T* /*node*/) {
    return true;
  }

 private:
  static bool planHasNestedJoinLoop(const velox::core::PlanNodePtr& planNode);

  velox::memory::MemoryPool* const pool_;
};

} // namespace facebook::presto
