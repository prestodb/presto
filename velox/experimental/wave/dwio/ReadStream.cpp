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

#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/StructColumnReader.h"

namespace facebook::velox::wave {
void allOperands(const ColumnReader* reader, OperandSet& operands) {
  auto op = reader->operand();
  if (op != kNoOperand) {
    operands.add(op);
  }
  for (auto& child : reader->children()) {
    allOperands(child, operands);
  }
}

ReadStream::ReadStream(
    StructColumnReader* columnReader,
    vector_size_t offset,
    RowSet rows,
    WaveStream& _waveStream,
    const OperandSet* firstColumns)
    : Executable(), offset_(offset), rows_(rows) {
  waveStream = &_waveStream;
  allOperands(columnReader, outputOperands);
  output.resize(outputOperands.size());
  reader_ = columnReader;
  staging_.push_back(std::make_unique<SplitStaging>());
  currentStaging_ = staging_[0].get();
  makeOps();
}

void ReadStream::makeOps() {
  auto& children = reader_->children();
  for (auto i = 0; i < children.size(); ++i) {
    ops_.emplace_back();
    auto& op = ops_.back();
    auto* child = reader_->children()[i];
    child->makeOp(this, ColumnAction::kValues, offset_, rows_, op);
  }
}

bool ReadStream::makePrograms(bool& needSync) {
  bool allDone = true;
  needSync = false;
  programs_.clear();
  for (auto i = 0; i < ops_.size(); ++i) {
    auto& op = ops_[i];
    if (op.isFinal) {
      continue;
    }
    if (op.prerequisite == ColumnOp::kNoPrerequisite ||
        ops_[op.prerequisite].isFinal) {
      op.reader->formatData()->startOp(
          op,
          nullptr,
          deviceStaging_,
          resultStaging_,
          *currentStaging_,
          programs_,
          *this);
      if (!op.isFinal) {
        allDone = false;
      }
      if (op.needsResult) {
        needSync = true;
      }
    } else {
      allDone = false;
    }
  }
  resultStaging_.setReturnBuffer(waveStream->arena(), programs_);
  return allDone;
}

// static
void ReadStream::launch(std::unique_ptr<ReadStream>&& readStream) {
  using UniqueExe = std::unique_ptr<Executable>;
  readStream->waveStream->installExecutables(
      folly::Range<UniqueExe*>(reinterpret_cast<UniqueExe*>(&readStream), 1),
      [&](Stream* stream, folly::Range<Executable**> exes) {
        auto* readStream = reinterpret_cast<ReadStream*>(exes[0]);
        bool needSync = false;
        for (;;) {
          bool done = readStream->makePrograms(needSync);
          readStream->currentStaging_->transfer(
              *readStream->waveStream, *stream);
          if (done) {
            break;
          }
          WaveBufferPtr extra;
          launchDecode(
              readStream->programs(),
              &readStream->waveStream->arena(),
              extra,
              stream);
          readStream->staging_.push_back(std::make_unique<SplitStaging>());
          readStream->currentStaging_ = readStream->staging_.back().get();
          if (needSync) {
            stream->wait();
          }
        }
        WaveBufferPtr extra;
        launchDecode(
            readStream->programs(),
            &readStream->waveStream->arena(),
            extra,
            stream);
        readStream->waveStream->markLaunch(*stream, *readStream);
      });
}

} // namespace facebook::velox::wave
