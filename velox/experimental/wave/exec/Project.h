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

#include "velox/experimental/wave/exec/WaveOperator.h"

namespace facebook::velox::wave {

class Project : public WaveOperator {
 public:
  Project(
      CompileState& state,
      RowTypePtr outputType,
      std::vector<AbstractOperand*> operands,
      std::vector<std::vector<ProgramPtr>> levels)
      : WaveOperator(state, outputType, ""), levels_(std::move(levels)) {}

  bool isStreaming() const override {
    return true;
  }

  void schedule(WaveStream& stream, int32_t maxRows = 0) override;

  vector_size_t outputSize(WaveStream& stream) const override;

  void finalize(CompileState& state) override;

  std::string toString() const override {
    return "Project";
  }

  const OperandSet& syncSet() const override {
    return computedSet_;
  }

 private:
  std::vector<std::vector<ProgramPtr>> levels_;
  OperandSet computedSet_;
};

} // namespace facebook::velox::wave
