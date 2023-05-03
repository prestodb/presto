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
#include "velox/exec/ProbeOperatorState.h"

namespace facebook::velox::exec {

std::string probeOperatorStateName(ProbeOperatorState state) {
  switch (state) {
    case ProbeOperatorState::kWaitForBuild:
      return "WAIT_FOR_BUILD";
    case ProbeOperatorState::kRunning:
      return "RUNNING";
    case ProbeOperatorState::kWaitForPeers:
      return "WAIT_FOR_PEERS";
    case ProbeOperatorState::kFinish:
      return "FINISH";
    default:
      return fmt::format("UNKNOWN: {}", static_cast<int>(state));
  }
}

} // namespace facebook::velox::exec
