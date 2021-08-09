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
#include "velox/experimental/codegen/CodegenLogger.h"
#include <glog/logging.h>
#include "velox/experimental/codegen/Codegen.h"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"

namespace facebook::velox::codegen {

void CodegenTaskLoggerBase::onCompileEnd(
    DefaultScopedTimer::EventSequence& eventSequence,
    const core::PlanNode& planNode) const {
  LOG(INFO) << "Finished codegen planNode transformation for planNode "
            << planNode.toString() << ", taskId " << taskId_;
  LOG(INFO) << std::endl
            << DefaultScopedTimer::printCounters(eventSequence) << std::endl;
}

} // namespace facebook::velox::codegen
