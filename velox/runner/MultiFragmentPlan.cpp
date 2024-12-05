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

#include "velox/runner/MultiFragmentPlan.h"

namespace facebook::velox::runner {

namespace {

std::string toFragmentsString(
    const std::vector<ExecutableFragment>& fragments,
    const std::function<std::string(const core::PlanNode&)>& planNodeToString) {
  std::stringstream out;
  for (auto i = 0; i < fragments.size(); ++i) {
    const auto& fragment = fragments[i];
    out << fmt::format(
               "Fragment {}: {} numWorkers={}:",
               i,
               fragment.taskPrefix,
               fragment.width)
        << std::endl;
    out << planNodeToString(*fragment.fragment.planNode) << std::endl;
    if (!fragment.inputStages.empty()) {
      out << "Inputs: ";
      for (auto& input : fragment.inputStages) {
        out << fmt::format(
            " {} <- {} ", input.consumerNodeId, input.producerTaskPrefix);
      }
      out << std::endl;
    }
  }
  return out.str();
}

} // namespace

std::string MultiFragmentPlan::toString(bool detailed) const {
  return toFragmentsString(fragments_, [&](const auto& planNode) {
    return planNode.toString(detailed, true);
  });
}

std::string MultiFragmentPlan::toSummaryString(
    core::PlanSummaryOptions options) const {
  return toFragmentsString(fragments_, [&](const auto& planNode) {
    return planNode.toSummaryString(options);
  });
}

} // namespace facebook::velox::runner
