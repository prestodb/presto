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

#include "velox/dwio/common/Writer.h"

namespace facebook::velox::dwio::common {

void Writer::checkStateTransition(State oldState, State newState) {
  switch (oldState) {
    case State::kInit:
      if (newState == State::kRunning) {
        return;
      }
      break;
    case State::kRunning:
      // NOTE: some physical file writer might switch from kRunning to kClosed
      // directly as there is nothing to do with finish() call.
      if (newState == State::kAborted || newState == State::kClosed ||
          newState == State::kFinishing) {
        return;
      }
      break;
    case State::kFinishing:
      if (newState == State::kAborted || newState == State::kClosed ||
          // NOTE: the finishing state is reentry state as we could yield in the
          // middle of long running finish processing.
          newState == State::kFinishing) {
        return;
      }
      break;
    case State::kAborted:
      [[fallthrough]];
    case State::kClosed:
      [[fallthrough]];
    default:
      break;
  }
  VELOX_FAIL(
      "Unexpected state transition from {} to {}",
      Writer::stateString(oldState),
      Writer::stateString(newState));
}

std::string Writer::stateString(State state) {
  switch (state) {
    case State::kInit:
      return "INIT";
    case State::kRunning:
      return "RUNNING";
    case State::kFinishing:
      return "FINISHING";
    case State::kClosed:
      return "CLOSED";
    case State::kAborted:
      return "ABORTED";
    default:
      VELOX_UNREACHABLE("BAD STATE: {}", static_cast<int>(state));
  }
}

bool Writer::isRunning() const {
  return state_ == State::kRunning;
}

bool Writer::isFinishing() const {
  return state_ == State::kFinishing;
}

void Writer::checkRunning() const {
  VELOX_CHECK_EQ(
      state_,
      State::kRunning,
      "Writer is not running: {}",
      Writer::stateString(state_));
}

void Writer::setState(State state) {
  checkStateTransition(state_, state);
  state_ = state;
}

} // namespace facebook::velox::dwio::common
