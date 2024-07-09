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

#include "velox/experimental/wave/exec/tests/utils/WaveTestSplitReader.h"
#include "velox/experimental/wave/exec/tests/utils/TestFormatReader.h"

DECLARE_int32(wave_max_reader_batch_rows);

namespace facebook::velox::wave::test {

using common::Subfield;

WaveTestSplitReader::WaveTestSplitReader(
    const std::shared_ptr<connector::ConnectorSplit>& split,
    const SplitReaderParams& params,
    const DefinesMap* defines) {
  auto hiveSplit =
      dynamic_cast<connector::hive::HiveConnectorSplit*>(split.get());
  VELOX_CHECK_NOT_NULL(hiveSplit);
  stripe_ = test::Table::getStripe(hiveSplit->filePath);
  VELOX_CHECK_NOT_NULL(stripe_);
  TestFormatParams formatParams(
      *params.connectorQueryCtx->memoryPool(), readerStats_, stripe_);
  std::vector<std::unique_ptr<Subfield::PathElement>> empty;
  columnReader_ = TestFormatReader::build(
      params.readerOutputType,
      stripe_->typeWithId,
      formatParams,
      *params.scanSpec,
      empty,
      *defines,
      true);
}

int32_t WaveTestSplitReader::canAdvance(WaveStream& stream) {
  if (!stripe_) {
    return 0;
  }
  return std::min<int32_t>(FLAGS_wave_max_reader_batch_rows, available());
}

void WaveTestSplitReader::schedule(WaveStream& waveStream, int32_t maxRows) {
  auto numRows = std::min<int32_t>(maxRows, available());
  scheduledRows_ = numRows;
  auto rowSet = folly::Range<const int32_t*>(iota(numRows, rows_), numRows);
  std::unique_ptr<ReadStream> exe(reinterpret_cast<ReadStream*>(
      waveStream.recycleExecutable(nullptr, 0).release()));
  if (exe) {
    VELOX_DCHECK_NOT_NULL(
        dynamic_cast<ReadStream*>(reinterpret_cast<Executable*>(exe.get())));
    if (reinterpret_cast<ColumnReader*>(exe->reader()) != columnReader_.get()) {
      // The previous read exe on the WaveStream is a different split. Make new.
      exe.reset();
    }
  }
  if (!exe) {
    exe = std::make_unique<ReadStream>(
        reinterpret_cast<StructColumnReader*>(columnReader_.get()), waveStream);
  }
  ReadStream::launch(std::move(exe), nextRow_, rowSet);
  nextRow_ += scheduledRows_;
}

vector_size_t WaveTestSplitReader::outputSize(WaveStream& stream) const {
  return scheduledRows_;
}

bool WaveTestSplitReader::isFinished() const {
  return nextRow_ >= available();
}

namespace {
class WaveTestSplitReaderFactory : public WaveSplitReaderFactory {
 public:
  std::shared_ptr<WaveSplitReader> create(
      const std::shared_ptr<connector::ConnectorSplit>& split,
      const SplitReaderParams& params,
      const DefinesMap* defines) override {
    auto hiveSplit =
        dynamic_cast<connector::hive::HiveConnectorSplit*>(split.get());
    if (!hiveSplit) {
      return nullptr;
    }
    if (hiveSplit->filePath.size() > 11 &&
        memcmp(hiveSplit->filePath.data(), "wavemock://", 11) == 0) {
      return std::make_shared<WaveTestSplitReader>(split, params, defines);
    }
    return nullptr;
  }
};
} // namespace

//  static
void WaveTestSplitReader::registerTestSplitReader() {
  WaveSplitReader::registerFactory(
      std::make_unique<WaveTestSplitReaderFactory>());
}

} // namespace facebook::velox::wave::test
