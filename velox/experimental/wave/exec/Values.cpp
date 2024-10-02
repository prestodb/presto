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

#include "velox/experimental/wave/exec/Values.h"
#include "velox/experimental/wave/exec/Vectors.h"
#include "velox/experimental/wave/exec/WaveDriver.h"

namespace facebook::velox::wave {

Values::Values(CompileState& state, const core::ValuesNode& values)
    : WaveSourceOperator(state, values.outputType(), values.id()),
      values_(values.values()),
      roundsLeft_(values.repeatTimes()) {}

std::vector<AdvanceResult> Values::canAdvance(WaveStream& stream) {
  if (current_ < values_.size()) {
    return {{.numRows = values_[current_]->size()}};
  }
  if (roundsLeft_ > 1) {
    return {{.numRows = values_[0]->size()}};
  }
  return {};
}

void Values::schedule(WaveStream& stream, int32_t maxRows) {
  RowVectorPtr data;
  if (current_ == values_.size()) {
    VELOX_CHECK_GE(roundsLeft_, 1);
    current_ = 1;
    data = values_[0];
    --roundsLeft_;

  } else {
    data = values_[current_++];
  }
  VELOX_CHECK_LE(data->size(), maxRows);

  std::vector<const BaseVector*> sources;
  for (auto i = 0; i < subfields_.size(); ++i) {
    sources.push_back(data->childAt(i).get());
  }
  int32_t counter = 0;
  outputIds_.forEach([&](auto id) {
    stream.setNullable(*stream.operandAt(id), sources[counter]->mayHaveNulls());
    ++counter;
  });
  folly::Range<Executable**> empty(nullptr, nullptr);
  auto numBlocks = bits::roundUp(data->size(), kBlockSize) / kBlockSize;
  stream.setNumRows(data->size());
  stream.prepareProgramLaunch(
      id_, 0, data->size(), empty, numBlocks, nullptr, nullptr);
  vectorsToDevice(
      folly::Range(sources.data(), sources.size()), outputIds_, stream);
}

std::string Values::toString() const {
  return "Values";
}

} // namespace facebook::velox::wave
