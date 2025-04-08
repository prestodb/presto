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

#include "velox/experimental/wave/exec/Project.h"
#include "velox/common/process/TraceContext.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/Wave.h"
#include "velox/experimental/wave/exec/WaveDriver.h"

#include <iostream>

namespace facebook::velox::wave {

exec::BlockingReason Project::isBlocked(
    WaveStream& stream,
    ContinueFuture* future) {
  for (int32_t i = levels_.size() - 1; i >= 0; --i) {
    auto& level = levels_[i];
    for (auto j = 0; j < level.size(); ++j) {
      auto* program = level[j].get();
      auto result = program->isBlocked(stream, future);
      if (result != exec::BlockingReason::kNotBlocked) {
        return result;
      }
    }
  }
  return exec::BlockingReason::kNotBlocked;
}

std::vector<AdvanceResult> Project::canAdvance(WaveStream& stream) {
  auto& controls = stream.launchControls(id_);
  if (controls.empty()) {
    /// No previous execution on the stream. If the first program starts with a
    /// source we can continue that.
    if (!isSource()) {
      return {};
    }
    auto* program = levels_[0][0].get();
    auto advance = program->canAdvance(stream, nullptr, 0);
    if (!advance.empty()) {
      advance.programIdx = 0;
      return {advance};
    }
    return {};
  }
  std::vector<AdvanceResult> result;
  for (int32_t i = levels_.size() - 1; i >= 0; --i) {
    auto& level = levels_[i];
    VELOX_CHECK_EQ(controls[i]->programInfo.size(), level.size());
    for (auto j = 0; j < level.size(); ++j) {
      auto* program = level[j].get();
      auto advance = program->canAdvance(stream, controls[i].get(), j);
      if (!advance.empty()) {
        advance.nthLaunch = i;
        result.push_back(advance);
        controls[i]->programInfo[j].advance = advance;
      } else {
        controls[i]->programInfo[j].advance = {};
      }
    }
    if (!result.empty()) {
      return result;
    }
  }

  return {};
}

void Project::callUpdateStatus(
    WaveStream& stream,
    const std::vector<WaveStream*>& otherStreams,
    AdvanceResult& advance) {
  if (advance.updateStatus) {
    levels_[advance.nthLaunch][advance.programIdx]->callUpdateStatus(
        stream, otherStreams, advance);
  }
}

namespace {
bool anyContinuable(LaunchControl& control) {
  for (auto& info : control.programInfo) {
    if (!info.advance.empty()) {
      return true;
    }
  }
  return false;
}
} // namespace

void Project::schedule(WaveStream& stream, int32_t maxRows) {
  int32_t firstLevel = 0;
  auto& controls = stream.launchControls(id_);
  bool isContinue = false;
  // firstLevel is 0 if no previous activity, otherwise it is the last level
  // with continuable activity. If continuable activity,'isContinue' is true.
  if (!controls.empty()) {
    for (int32_t i = levels_.size() - 1; i >= 0; --i) {
      if (anyContinuable(*controls[i])) {
        firstLevel = i;
        isContinue = true;
        break;
      }
    }
  }
  for (auto levelIdx = firstLevel; levelIdx < levels_.size(); ++levelIdx) {
    auto& level = levels_[levelIdx];
    std::vector<std::unique_ptr<Executable>> exes(level.size());
    for (auto i = 0; i < level.size(); ++i) {
      auto& program = level[i];
      exes[i] = stream.recycleExecutable(program.get(), maxRows);
      if (!exes[i]) {
        exes[i] = program->getExecutable(maxRows, driver_->operands());
      }
    }
    auto blocksPerExe = bits::roundUp(maxRows, kBlockSize) / kBlockSize;
    auto* data = exes.data();
    auto range = folly::Range(data, data + exes.size());
    stream.installExecutables(
        range, [&](Stream* out, folly::Range<Executable**> exes) {
          LaunchControl* inputControl = nullptr;
          if (!isSource()) {
            inputControl = driver_->inputControl(stream, id_);
          }
          auto control = stream.prepareProgramLaunch(
              id_,
              levelIdx,
              isContinue ? -1 : maxRows,
              exes,
              blocksPerExe,
              inputControl,
              out);
          out->prefetch(
              getDevice(),
              control->deviceData->as<char>(),
              control->deviceData->size());
          stream.setState(WaveStream::State::kParallel);
          stream.checkExecutables();
          {
            PrintTime c("expr");
            auto* kernel = exes[0]->programShared->kernel();
            VELOX_CHECK_NOT_NULL(kernel);
            auto numBranches = exes[0]->programShared->numBranches();
            void* params = &control->params;
            // The count of TBs is the BlockStatus count ceil
            // numRowsPerThread, i.e. 11 blocks with 4 rows per thread
            // is 3. The TBs in the launch is this times the number of
            // program branches.
            auto numTBs = numBranches *
                bits::roundUp(control->params.numBlocks,
                              control->params.numRowsPerThread) /
                control->params.numRowsPerThread;
            kernel->launch(
                0, numTBs, kBlockSize, control->sharedMemorySize, out, &params);
          }
          stream.checkExecutables();
        });
    isContinue = false;
  }
}

void Project::pipelineFinished(WaveStream& stream) {
  for (auto& level : levels_) {
    for (auto& program : level) {
      program->pipelineFinished(stream);
    }
  }
}

void Project::finalize(CompileState& state) {
  for (auto& level : levels_) {
    for (auto& program : level) {
      for (auto& pair : program->output()) {
        if (true /*isProjected(id)*/) {
          computedSet_.add(pair.first->id);
        }
      }
    }
  }
}

} // namespace facebook::velox::wave
