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

#include "presto_cpp/main/types/VeloxPlanValidator.h"
#include "presto_cpp/main/common/Configs.h"

namespace facebook::presto {
bool planHasNestedJoinLoop(const velox::core::PlanNodePtr planNode) {
  if (auto joinNode =
          std::dynamic_pointer_cast<const velox::core::NestedLoopJoinNode>(
              planNode)) {
    return true;
  }

  for (auto plan : planNode->sources()) {
    if (planHasNestedJoinLoop(plan)) {
      return true;
    }
  }

  return false;
}

void VeloxPlanValidator::validatePlanFragment(
    const velox::core::PlanFragment& fragment) const {
  const auto failOnNestedLoopJoin =
      SystemConfig::instance()
          ->optionalProperty<bool>(
              SystemConfig::kPlanValidatorFailOnNestedLoopJoin)
          .value_or(false);
  if (failOnNestedLoopJoin) {
    VELOX_CHECK(
        !planHasNestedJoinLoop(fragment.planNode),
        "Velox plan uses nested join loop which isn't supported.");
  }
}

} // namespace facebook::presto
