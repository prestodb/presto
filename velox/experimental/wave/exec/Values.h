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

#pragma once
#include "velox/core/PlanNode.h"
#include "velox/experimental/wave/exec/WaveOperator.h"

namespace facebook::velox::wave {

class Values : public WaveSourceOperator {
 public:
  Values(CompileState& state, const core::ValuesNode& values);

  AdvanceResult canAdvance(WaveStream& stream) override;

  bool isStreaming() const override {
    return true;
  }

  void schedule(WaveStream& stream, int32_t maxRows = 0) override;

  bool isFinished() const override {
    return roundsLeft_ == (current_ == values_.size());
  }

  vector_size_t outputSize(WaveStream& stream) const override {
    // Must not be called before schedule().
    VELOX_CHECK_LT(0, current_);
    return values_[current_ - 1]->size();
  }

  std::string toString() const override;

 private:
  std::vector<RowVectorPtr> values_;
  int32_t current_ = 0;
  size_t roundsLeft_ = 1;
};

} // namespace facebook::velox::wave
