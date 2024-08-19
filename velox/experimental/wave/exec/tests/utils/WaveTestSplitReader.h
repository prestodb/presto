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

#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/exec/WaveSplitReader.h"
#include "velox/experimental/wave/exec/tests/utils/FileFormat.h"

namespace facebook::velox::wave::test {

/// A WaveSplitReader that decodes mock Wave tables.
class WaveTestSplitReader : public WaveSplitReader {
 public:
  WaveTestSplitReader(
      const std::shared_ptr<connector::ConnectorSplit>& split,
      const SplitReaderParams& params,
      const DefinesMap* defines);

  bool emptySplit() override {
    return !stripe_ || stripe_->columns[0]->numValues == 0 || emptySplit_;
  }

  int32_t canAdvance(WaveStream& stream) override;

  void schedule(WaveStream& stream, int32_t maxRows = 0) override;

  vector_size_t outputSize(WaveStream& stream) const;

  bool isFinished() const override;

  uint64_t getCompletedBytes() override {
    return params_.ioStats->rawBytesRead();
  }

  uint64_t getCompletedRows() override {
    return nextRow_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    return {};
  }

  static void registerTestSplitReader();

 private:
  int32_t available() const {
    return stripe_->columns[0]->numValues - nextRow_;
  }

  std::shared_ptr<connector::ConnectorSplit> split_;
  SplitReaderParams params_;
  FileHandleCachedPtr fileHandleCachePtr;
  cache::AsyncDataCache* cache_{nullptr};
  test::Stripe* stripe_{nullptr};
  std::unique_ptr<ColumnReader> columnReader_;
  // First unscheduled row.
  int32_t nextRow_{0};
  int32_t scheduledRows_{0};
  dwio::common::ColumnReaderStatistics readerStats_;
  raw_vector<int32_t> rows_;
  FileHandleCachedPtr fileHandle_;
  FileInfo fileInfo_;
  bool emptySplit_{false};
};

} // namespace facebook::velox::wave::test
