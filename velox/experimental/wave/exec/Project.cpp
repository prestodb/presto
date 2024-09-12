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

namespace facebook::velox::wave {

AbstractWrap* Project::findWrap() const {
  return filterWrap_;
}

AdvanceResult Project::canAdvance(WaveStream& stream) {
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
    }
    return advance;
  }
  for (int32_t i = levels_.size() - 1; i >= 0; --i) {
    auto& level = levels_[i];
    AdvanceResult first;
    VELOX_CHECK_EQ(controls[i]->programInfo.size(), level.size());
    for (auto j = 0; j < level.size(); ++j) {
      auto* program = level[i].get();
      auto advance = program->canAdvance(stream, controls[i].get(), j);
      if (!advance.empty()) {
        if (first.empty()) {
          first = advance;
        }
        controls[i]->programInfo[j].advance = advance;
      } else {
        controls[i]->programInfo[j].advance = {};
      }
      if (!first.empty()) {
        return first;
      }
    }
  }

  return {};
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
          if (!isContinue && !isSource()) {
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
          {
            PrintTime c("expr");
            reinterpret_cast<WaveKernelStream*>(out)->call(
                out,
                exes.size() * blocksPerExe,
                control->sharedMemorySize,
                control->params);
          }
        });
    isContinue = false;
  }
}

void Project::finalize(CompileState& state) {
  for (auto& level : levels_) {
    for (auto& program : level) {
      program->prepareForDevice(state.arena());
      for (auto& pair : program->output()) {
        if (true /*isProjected(id)*/) {
          computedSet_.add(pair.first->id);
        }
      }
    }
  }
}

} // namespace facebook::velox::wave
