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

DEFINE_int32(
    wave_reader_rows_per_tb,
    1024,
    "Number of items per thread block in Wave reader");

DEFINE_int32(
    wave_max_reader_batch_rows,
    80 * 1024,
    "Max batch for Wave table scan");

namespace facebook::velox::wave {

void allOperands(
    const ColumnReader* reader,
    OperandSet& operands,
    std::vector<AbstractOperand*>* abstractOperands) {
  auto op = reader->operand();
  if (op != nullptr) {
    operands.add(op->id);
    if (abstractOperands) {
      abstractOperands->push_back(op);
    }
  }

  for (auto& child : reader->children()) {
    allOperands(child, operands, abstractOperands);
  }
}

ReadStream::ReadStream(
    StructColumnReader* columnReader,
    WaveStream& _waveStream,
    const OperandSet* firstColumns)
    : Executable() {
  waveStream = &_waveStream;
  allOperands(columnReader, outputOperands, &abstractOperands_);
  output.resize(outputOperands.size());
  reader_ = columnReader;
  reader_->splitStaging().push_back(std::make_unique<SplitStaging>());
  currentStaging_ = reader_->splitStaging().back().get();
}

void ReadStream::setBlockStatusAndTemp() {
  auto* status = control_->deviceData->as<BlockStatus>();
  auto tempSize = programs_.programs[0][0]->tempSize();
  auto size = programs_.programs.size() * tempSize;
  auto id = deviceStaging_.reserve(size);
  for (auto blockIdx = 0; blockIdx < programs_.programs.size(); ++blockIdx) {
    auto& program = programs_.programs[blockIdx];
    for (auto& op : program) {
      op->temp = reinterpret_cast<int32_t*>(blockIdx * tempSize);
      deviceStaging_.registerPointer(id, &op->temp, false);
      op->blockStatus = status + op->numRowsPerThread * op->nthBlock;
    }
  }
}

void ReadStream::makeGrid(Stream* stream) {
  programs_.clear();
  auto total = reader_->formatData()->totalRows();
  auto blockSize = FLAGS_wave_reader_rows_per_tb;
  if (total < blockSize) {
    return;
  }
  auto numBlocks = bits::roundUp(total, blockSize) / blockSize;
  auto& children = reader_->children();
  for (auto i = 0; i < children.size(); ++i) {
    auto* child = reader_->children()[i];
    // TODO:  Must  propagate the incoming nulls from outer to inner structs.
    // griddize must decode nulls if present.
    child->formatData()->griddize(
        blockSize,
        numBlocks,
        deviceStaging_,
        resultStaging_,
        *currentStaging_,
        programs_,
        *this);
  }
  if (!programs_.programs.empty()) {
    WaveStats& stats = waveStream->stats();
    stats.bytesToDevice += currentStaging_->bytesToDevice();
    ++stats.numKernels;
    stats.numPrograms += programs_.programs.size();
    stats.numThreads +=
        programs_.programs.size() * std::min<int32_t>(rows_.size(), kBlockSize);
    setBlockStatusAndTemp();
    deviceStaging_.makeDeviceBuffer(waveStream->arena());
    currentStaging_->transfer(*waveStream, *stream);
    WaveBufferPtr extra;
    launchDecode(programs_, &waveStream->arena(), extra, stream);
    reader_->recordGriddize(*stream);
    if (extra) {
      commands_.push_back(std::move(extra));
    }
    reader_->splitStaging().push_back(std::make_unique<SplitStaging>());
    currentStaging_ = reader_->splitStaging().back().get();
  }
}

void ReadStream::makeCompact(bool isSerial) {
  auto rowsPerBlock = FLAGS_wave_reader_rows_per_tb;
  auto numRowsPerThread = FLAGS_wave_reader_rows_per_tb / kBlockSize;
  for (int32_t i = 0; i < static_cast<int32_t>(filters_.size()) - 1; ++i) {
    if (filters_[i].waveVector) {
      int32_t numTBs =
          bits::roundUp(numBlocks_, numRowsPerThread) / numRowsPerThread;
      for (auto blockIdx = 0; blockIdx < numTBs; ++blockIdx) {
        auto step = std::make_unique<GpuDecode>();
        step->step = DecodeStep::kCompact64;
        step->nthBlock = blockIdx;
        step->numRowsPerThread = numRowsPerThread;
        if (filters_.back().deviceResult) {
          step->data.compact.finalRows =
              filters_.back().deviceResult + blockIdx * rowsPerBlock;
          step->data.compact.sourceNumRows =
              filters_[i].extraRowCount + blockIdx * numRowsPerThread;
        } else {
          step->data.compact.finalRows = reinterpret_cast<int32_t*>(
              blockIdx * rowsPerBlock * sizeof(int32_t));
          deviceStaging_.registerPointer(
              filters_.back().deviceResultId,
              &step->data.compact.finalRows,
              false);
          step->data.compact.sourceNumRows = reinterpret_cast<int32_t*>(
              blockIdx * numRowsPerThread * sizeof(int32_t));
          deviceStaging_.registerPointer(
              filters_[i].extraRowCountId,
              &step->data.compact.sourceNumRows,
              false);
        }
        if (filters_[i].deviceResult) {
          step->data.compact.sourceRows =
              filters_[i].deviceResult + blockIdx * rowsPerBlock;
        } else {
          step->data.compact.sourceRows =
              reinterpret_cast<int32_t*>(blockIdx * rowsPerBlock);
          deviceStaging_.registerPointer(
              filters_[i].deviceResultId,
              &step->data.compact.sourceRows,
              false);
        }
        auto& vector = filters_[i].waveVector;
        step->dataType = static_cast<WaveTypeKind>(vector->type()->kind());
        step->data.compact.source = vector->values<char>() +
            waveTypeKindSize(step->dataType) * blockIdx * rowsPerBlock;
        if (vector->nulls()) {
          step->data.compact.sourceNull =
              vector->nulls() + blockIdx * rowsPerBlock;
        }
        if (isSerial) {
          programs_.programs[blockIdx].push_back(std::move(step));
        } else {
          programs_.programs.emplace_back();
          programs_.programs.back().push_back(std::move(step));
        }
      }
    }
  }
}

void ReadStream::makeOps() {
  auto& children = reader_->children();
  for (auto i = 0; i < children.size(); ++i) {
    auto* child = reader_->children()[i];
    if (child->scanSpec().filter()) {
      hasFilters_ = true;
      filters_.emplace_back();
      bool filterOnly = !child->scanSpec().keepValues();
      child->makeOp(
          this,
          filterOnly ? ColumnAction::kFilter : ColumnAction::kValues,
          filters_.back());
    }
  }
  for (auto i = 0; i < children.size(); ++i) {
    auto* child = reader_->children()[i];
    if (child->scanSpec().filter()) {
      continue;
    }
    ops_.emplace_back();
    auto& op = ops_.back();
    child->makeOp(this, ColumnAction::kValues, op);
  }
}

bool ReadStream::decodenonFiltersInFiltersKernel() {
  return ops_.size() == 1;
}

void ReadStream::prepareRead() {
  filtersDone_ = false;
  for (auto& op : filters_) {
    op.reader->formatData()->newBatch(row_);
    op.isFinal = false;
    op.rows = rows_;
  }
  for (auto& op : ops_) {
    op.reader->formatData()->newBatch(row_);
    op.isFinal = false;
    op.rows = rows_;
  }
}

bool ReadStream::makePrograms(bool& needSync) {
  bool allDone = true;
  needSync = false;
  programs_.clear();
  ColumnOp* previousFilter = nullptr;
  if (!filtersDone_ && !filters_.empty()) {
    // Filters are done consecutively, each TB does all the filters for its
    // range.
    for (auto& filter : filters_) {
      filter.reader->formatData()->startOp(
          filter,
          previousFilter,
          deviceStaging_,
          resultStaging_,
          *currentStaging_,
          programs_,
          *this);
      previousFilter = &filter;
    }
    if (!decodenonFiltersInFiltersKernel()) {
      filtersDone_ = true;
      return false;
    }
  }
  makeCompact(!filtersDone_);
  previousFilter = filters_.empty() ? nullptr : &filters_.back();
  for (auto i = 0; i < ops_.size(); ++i) {
    auto& op = ops_[i];
    if (op.isFinal) {
      continue;
    }
    if (op.prerequisite == ColumnOp::kNoPrerequisite ||
        ops_[op.prerequisite].isFinal) {
      op.reader->formatData()->startOp(
          op,
          previousFilter,
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
  filtersDone_ = true;
  if (filters_.empty() && allDone) {
    auto setCount = std::make_unique<GpuDecode>();
    setCount->step = DecodeStep::kRowCountNoFilter;
    setCount->data.rowCountNoFilter.numRows = rows_.size();
    setCount->data.rowCountNoFilter.status =
        control_->deviceData->as<BlockStatus>();
    programs_.programs.emplace_back();
    programs_.programs.back().push_back(std::move(setCount));
  }
  ++nthWave_;
  resultStaging_.setReturnBuffer(waveStream->arena(), programs_.result);
  return allDone;
}

void ReadStream::launch(
    std::unique_ptr<ReadStream> readStream,
    int32_t row,
    RowSet rows) {
  using UniqueExe = std::unique_ptr<Executable>;
  readStream->row_ = row;
  readStream->rows_ = rows;

  // The function of control here is to have a status and row count for each
  // kBlockSize top level rows of output and to have Operand structs for the
  // produced column.
  readStream->makeControl();
  auto numRows = readStream->rows_.size();
  auto waveStream = readStream->waveStream;
  WaveStats& stats = waveStream->stats();
  bool firstLaunch = true;
  waveStream->installExecutables(
      folly::Range<UniqueExe*>(reinterpret_cast<UniqueExe*>(&readStream), 1),
      [&](Stream* stream, folly::Range<Executable**> exes) {
        auto* readStream = reinterpret_cast<ReadStream*>(exes[0]);
        bool needSync = false;
        bool griddizedHere = false;
        if (!readStream->inited_) {
          readStream->makeGrid(stream);
          griddizedHere = true;
          readStream->makeOps();
          readStream->inited_ = true;
        }
        readStream->prepareRead();
        for (;;) {
          bool done = readStream->makePrograms(needSync);
          stats.bytesToDevice += readStream->currentStaging_->bytesToDevice();
          ++stats.numKernels;
          stats.numPrograms += readStream->programs_.programs.size();
          stats.numThreads += readStream->programs_.programs.size() *
              std::min<int32_t>(readStream->rows_.size(), kBlockSize);
          readStream->currentStaging_->transfer(*waveStream, *stream);
          if (done) {
            break;
          }
          readStream->setBlockStatusAndTemp();
          readStream->deviceStaging_.makeDeviceBuffer(waveStream->arena());
          WaveBufferPtr extra;
          if (!griddizedHere && firstLaunch) {
            // If the same split is read on multiple streams for
            // different row ranges, the non-first will have to sync
            // on the griddize of the first.
            if (auto* event = readStream->reader_->griddizeEvent()) {
              event->wait(*stream);
            }
          }
          firstLaunch = false;
          launchDecode(
              readStream->programs(), &waveStream->arena(), extra, stream);
          if (extra) {
            readStream->commands_.push_back(std::move(extra));
          }
          readStream->reader_->splitStaging().push_back(
              std::make_unique<SplitStaging>());
          readStream->currentStaging_ =
              readStream->reader_->splitStaging().back().get();
          if (needSync) {
            waveStream->setState(WaveStream::State::kWait);
            stream->wait();
            readStream->waveStream->setState(WaveStream::State::kHost);
          } else {
            readStream->waveStream->setState(WaveStream::State::kParallel);
          }
        }

        readStream->setBlockStatusAndTemp();
        readStream->deviceStaging_.makeDeviceBuffer(waveStream->arena());
        WaveBufferPtr extra;
        launchDecode(
            readStream->programs(),
            &readStream->waveStream->arena(),
            extra,
            stream);
        if (extra) {
          readStream->commands_.push_back(std::move(extra));
        }
        readStream->waveStream->setState(WaveStream::State::kParallel);
        readStream->waveStream->markLaunch(*stream, *readStream);
      });
}

void ReadStream::makeControl() {
  auto numRows = rows_.size();
  numBlocks_ = bits::roundUp(numRows, kBlockSize) / kBlockSize;
  waveStream->setNumRows(numRows);
  WaveStream::ExeLaunchInfo info;
  waveStream->exeLaunchInfo(*this, numBlocks_, info);
  auto statusBytes = bits::roundUp(sizeof(BlockStatus) * numBlocks_, 8);
  auto deviceBytes = statusBytes + info.totalBytes;
  auto control = std::make_unique<LaunchControl>(0, numRows);
  control->deviceData = waveStream->arena().allocate<char>(deviceBytes);
  // Zero initialization is expected, for example for operands and arrays in
  // Operand::indices.
  memset(control->deviceData->as<char>(), 0, deviceBytes);
  control->params.status = control->deviceData->as<BlockStatus>();
  for (auto& reader : reader_->children()) {
    if (!reader->formatData()->hasNulls() || reader->hasNonNullFilter()) {
      auto* operand = reader->operand();
      if (operand) {
        waveStream->operandNullable()[operand->id] = false;
      }
    }
  }
  operands = waveStream->fillOperands(
      *this, control->deviceData->as<char>() + statusBytes, info)[0];
  control_ = control.get();
  waveStream->setLaunchControl(0, 0, std::move(control));
}

} // namespace facebook::velox::wave
