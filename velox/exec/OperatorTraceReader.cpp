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

#include <utility>

#include "velox/exec/OperatorTraceReader.h"

#include "velox/exec/TraceUtil.h"

namespace facebook::velox::exec::trace {

OperatorTraceInputReader::OperatorTraceInputReader(
    std::string traceDir,
    RowTypePtr dataType,
    memory::MemoryPool* pool)
    : traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      dataType_(std::move(dataType)),
      pool_(pool),
      inputStream_(getInputStream()) {
  VELOX_CHECK_NOT_NULL(dataType_);
  VELOX_CHECK_NOT_NULL(inputStream_);
}

bool OperatorTraceInputReader::read(RowVectorPtr& batch) const {
  if (inputStream_->atEnd()) {
    batch = nullptr;
    return false;
  }

  VectorStreamGroup::read(
      inputStream_.get(), pool_, dataType_, &batch, &readOptions_);
  return true;
}

std::unique_ptr<common::FileInputStream>
OperatorTraceInputReader::getInputStream() const {
  auto traceFile = fs_->openFileForRead(getOpTraceInputFilePath(traceDir_));
  // TODO: Make the buffer size configurable.
  return std::make_unique<common::FileInputStream>(
      std::move(traceFile), 1 << 20, pool_);
}

OperatorTraceSummaryReader::OperatorTraceSummaryReader(
    std::string traceDir,
    memory::MemoryPool* pool)
    : traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      pool_(pool),
      summaryFile_(fs_->openFileForRead(getOpTraceSummaryFilePath(traceDir_))) {
}

OperatorTraceSummary OperatorTraceSummaryReader::read() const {
  VELOX_CHECK_NOT_NULL(summaryFile_);
  const auto summaryStr = summaryFile_->pread(0, summaryFile_->size());
  VELOX_CHECK(!summaryStr.empty());

  folly::dynamic summaryObj = folly::parseJson(summaryStr);
  OperatorTraceSummary summary;
  summary.opType = summaryObj[OperatorTraceTraits::kOpTypeKey].asString();
  summary.peakMemory = summaryObj[OperatorTraceTraits::kPeakMemoryKey].asInt();
  summary.inputRows = summaryObj[OperatorTraceTraits::kInputRowsKey].asInt();
  return summary;
}
} // namespace facebook::velox::exec::trace
