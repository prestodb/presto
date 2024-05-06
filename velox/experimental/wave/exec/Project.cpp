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
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/Wave.h"
#include "velox/experimental/wave/exec/WaveDriver.h"

namespace facebook::velox::wave {

AbstractWrap* Project::findWrap() const {
  return filterWrap_;
}

void Project::schedule(WaveStream& stream, int32_t maxRows) {
  for (auto& level : levels_) {
    std::vector<std::unique_ptr<Executable>> exes(level.size());
    for (auto i = 0; i < level.size(); ++i) {
      auto& program = level[i];
      exes[i] = program->getExecutable(maxRows, driver_->operands());
    }
    auto blocksPerExe = bits::roundUp(maxRows, kBlockSize) / kBlockSize;
    auto* data = exes.data();
    auto range = folly::Range(data, data + exes.size());
    stream.installExecutables(
        range, [&](Stream* out, folly::Range<Executable**> exes) {
          auto inputControl = driver_->inputControl(stream, id_);
          auto control = stream.prepareProgramLaunch(
              id_, maxRows, exes, blocksPerExe, inputControl, out);
          reinterpret_cast<WaveKernelStream*>(out)->call(
              out,
              exes.size() * blocksPerExe,
              control->blockBase,
              control->programIdx,
              control->programs,
              control->operands,
              inputControl->status,
              control->sharedMemorySize);
        });
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

vector_size_t Project::outputSize(WaveStream& stream) const {
  auto& control = stream.launchControls(id_);
  VELOX_CHECK(!control.empty());
  return control[0]->inputRows;
}

} // namespace facebook::velox::wave
