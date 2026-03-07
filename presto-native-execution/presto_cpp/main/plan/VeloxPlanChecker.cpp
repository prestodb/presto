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
#include "presto_cpp/main/plan/VeloxPlanChecker.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/plan/PrestoToVeloxQueryPlan.h"
#include "velox/core/QueryCtx.h"

using namespace facebook::velox;

namespace facebook::presto {

bool VeloxPlanChecker::planHasNestedJoinLoop(
    const velox::core::PlanNodePtr& planNode) {
  if (std::dynamic_pointer_cast<const velox::core::NestedLoopJoinNode>(
          planNode)) {
    return true;
  }

  for (const auto& source : planNode->sources()) {
    if (planHasNestedJoinLoop(source)) {
      return true;
    }
  }

  return false;
}

void VeloxPlanChecker::validatePlanFragment(
    const velox::core::PlanFragment& fragment) {
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

protocol::PlanConversionResponse VeloxPlanChecker::checkPlanFragment(
    const std::string& planFragmentJson) const {
  protocol::PlanConversionResponse response;

  try {
    protocol::PlanFragment planFragment = json::parse(planFragmentJson);

    auto queryCtx = core::QueryCtx::create();
    // Attempt to convert the plan fragment to a Velox plan with Velox plan
    // node validation.
    VeloxInteractiveQueryPlanConverter converter(
        queryCtx.get(), pool_, PlanConversionPurpose::kValidation);

    // Create a taskId and empty TableWriteInfo needed for plan conversion.
    protocol::TaskId taskId = "velox-plan-conversion.0.0.0.0";
    auto tableWriteInfo = std::make_shared<protocol::TableWriteInfo>();

    auto veloxPlan =
        converter.toVeloxQueryPlan(planFragment, tableWriteInfo, taskId);
    validatePlanFragment(veloxPlan);
  } catch (const VeloxException& e) {
    response.failures.emplace_back(
        toNativeSidecarFailureInfo(translateToPrestoException(e)));
  } catch (const std::exception& e) {
    response.failures.emplace_back(
        toNativeSidecarFailureInfo(translateToPrestoException(e)));
  }

  return response;
}

} // namespace facebook::presto
