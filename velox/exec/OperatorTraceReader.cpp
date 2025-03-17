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

#include <folly/hash/Checksum.h>
#include "velox/common/file/FileInputStream.h"
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
      serde_(getNamedVectorSerde(VectorSerde::Kind::kPresto)),
      inputStream_(getInputStream()) {
  VELOX_CHECK_NOT_NULL(dataType_);
}

bool OperatorTraceInputReader::read(RowVectorPtr& batch) const {
  if (inputStream_ == nullptr) {
    return false;
  }

  if (inputStream_->atEnd()) {
    batch = nullptr;
    return false;
  }

  VectorStreamGroup::read(
      inputStream_.get(), pool_, dataType_, serde_, &batch, &readOptions_);
  return true;
}

std::unique_ptr<common::FileInputStream>
OperatorTraceInputReader::getInputStream() const {
  auto traceFile = fs_->openFileForRead(getOpTraceInputFilePath(traceDir_));
  if (traceFile->size() == 0) {
    LOG(WARNING) << "Operator trace input file is empty: "
                 << getOpTraceInputFilePath(traceDir_);
    return nullptr;
  }
  // TODO: Make the buffer size configurable.
  return std::make_unique<common::FileInputStream>(
      std::move(traceFile), 1 << 20, pool_);
}

OperatorTraceSummaryReader::OperatorTraceSummaryReader(
    std::string traceDir,
    memory::MemoryPool* /* pool */)
    : traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      summaryFile_(fs_->openFileForRead(getOpTraceSummaryFilePath(traceDir_))) {
}

OperatorTraceSummary OperatorTraceSummaryReader::read() const {
  VELOX_CHECK_NOT_NULL(summaryFile_);
  const auto summaryStr = summaryFile_->pread(0, summaryFile_->size());
  VELOX_CHECK(!summaryStr.empty());

  folly::dynamic summaryObj = folly::parseJson(summaryStr);
  OperatorTraceSummary summary;
  summary.opType = summaryObj[OperatorTraceTraits::kOpTypeKey].asString();
  if (summary.opType == "TableScan") {
    summary.numSplits = summaryObj[OperatorTraceTraits::kNumSplitsKey].asInt();
  }
  summary.peakMemory = summaryObj[OperatorTraceTraits::kPeakMemoryKey].asInt();
  summary.inputRows = summaryObj[OperatorTraceTraits::kInputRowsKey].asInt();
  summary.inputBytes = summaryObj[OperatorTraceTraits::kInputBytesKey].asInt();
  summary.rawInputRows =
      summaryObj[OperatorTraceTraits::kRawInputRowsKey].asInt();
  summary.rawInputBytes =
      summaryObj[OperatorTraceTraits::kRawInputBytesKey].asInt();
  return summary;
}

OperatorTraceSplitReader::OperatorTraceSplitReader(
    std::vector<std::string> traceDirs,
    memory::MemoryPool* pool)
    : traceDirs_(std::move(traceDirs)),
      fs_(filesystems::getFileSystem(traceDirs_[0], nullptr)),
      pool_(pool) {
  VELOX_CHECK_NOT_NULL(fs_);
}

std::vector<std::string> OperatorTraceSplitReader::read() const {
  std::vector<std::string> splits;
  for (const auto& traceDir : traceDirs_) {
    auto stream = getSplitInputStream(traceDir);
    if (stream == nullptr) {
      continue;
    }
    auto curSplits = deserialize(stream.get());
    splits.insert(
        splits.end(),
        std::make_move_iterator(curSplits.begin()),
        std::make_move_iterator(curSplits.end()));
  }
  return splits;
}

std::unique_ptr<common::FileInputStream>
OperatorTraceSplitReader::getSplitInputStream(
    const std::string& traceDir) const {
  auto splitInfoFile = fs_->openFileForRead(getOpTraceSplitFilePath(traceDir));
  if (splitInfoFile->size() == 0) {
    LOG(WARNING) << "Split info is empty in " << traceDir;
    return nullptr;
  }
  // TODO: Make the buffer size configurable.
  return std::make_unique<common::FileInputStream>(
      std::move(splitInfoFile), 1 << 20, pool_);
}

// static
std::vector<std::string> OperatorTraceSplitReader::deserialize(
    common::FileInputStream* stream) {
  std::vector<std::string> splits;
  try {
    while (!stream->atEnd()) {
      const auto length = stream->read<uint32_t>();
      std::string splitStr(length, '\0');
      stream->readBytes(reinterpret_cast<uint8_t*>(splitStr.data()), length);
      const auto crc32 = stream->read<uint32_t>();
      const auto actualCrc32 = folly::crc32(
          reinterpret_cast<const uint8_t*>(splitStr.data()), splitStr.size());
      if (crc32 != actualCrc32) {
        LOG(ERROR) << "Failed to verify the split checksum " << crc32
                   << " which does not equal to the actual computed checksum "
                   << actualCrc32;
        break;
      }
      splits.push_back(std::move(splitStr));
    }
  } catch (const VeloxException& e) {
    LOG(ERROR) << "Failed to deserialize split: " << e.message();
  }
  return splits;
}
} // namespace facebook::velox::exec::trace
