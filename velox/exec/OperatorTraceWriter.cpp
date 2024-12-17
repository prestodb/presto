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

#include "velox/exec/OperatorTraceWriter.h"

#include <folly/hash/Checksum.h>
#include <folly/io/Cursor.h>
#include <utility>

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Trace.h"
#include "velox/exec/TraceUtil.h"

namespace facebook::velox::exec::trace {
namespace {
void recordOperatorSummary(Operator* op, folly::dynamic& obj) {
  obj[OperatorTraceTraits::kOpTypeKey] = op->operatorType();
  const auto stats = op->stats(/*clear=*/false);
  if (op->operatorType() == "TableScan") {
    obj[OperatorTraceTraits::kNumSplitsKey] = stats.numSplits;
  }
  obj[OperatorTraceTraits::kPeakMemoryKey] =
      stats.memoryStats.peakTotalMemoryReservation;
  obj[OperatorTraceTraits::kInputRowsKey] = stats.inputPositions;
  obj[OperatorTraceTraits::kInputBytesKey] = stats.inputBytes;
  obj[OperatorTraceTraits::kRawInputRowsKey] = stats.rawInputPositions;
  obj[OperatorTraceTraits::kRawInputBytesKey] = stats.rawInputBytes;
}
} // namespace

OperatorTraceInputWriter::OperatorTraceInputWriter(
    Operator* traceOp,
    std::string traceDir,
    memory::MemoryPool* pool,
    UpdateAndCheckTraceLimitCB updateAndCheckTraceLimitCB)
    : traceOp_(traceOp),
      traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      pool_(pool),
      serde_(getNamedVectorSerde(VectorSerde::Kind::kPresto)),
      updateAndCheckTraceLimitCB_(std::move(updateAndCheckTraceLimitCB)) {
  traceFile_ = fs_->openFileForWrite(getOpTraceInputFilePath(traceDir_));
  VELOX_CHECK_NOT_NULL(traceFile_);
}

void OperatorTraceInputWriter::write(const RowVectorPtr& rows) {
  if (FOLLY_UNLIKELY(finished_)) {
    return;
  }

  if (batch_ == nullptr) {
    batch_ = std::make_unique<VectorStreamGroup>(pool_, serde_);
    batch_->createStreamTree(
        std::static_pointer_cast<const RowType>(rows->type()),
        1'000,
        &options_);
  }
  batch_->append(rows);

  // Serialize and write out each batch.
  IOBufOutputStream out(
      *pool_, nullptr, std::max<int64_t>(64 * 1024, batch_->size()));
  batch_->flush(&out);
  batch_->clear();
  auto iobuf = out.getIOBuf();
  updateAndCheckTraceLimitCB_(iobuf->computeChainDataLength());
  traceFile_->append(std::move(iobuf));
}

void OperatorTraceInputWriter::finish() {
  if (finished_) {
    return;
  }

  VELOX_CHECK_NOT_NULL(
      traceFile_, "The query data writer has already been finished");
  traceFile_->close();
  traceFile_.reset();
  batch_.reset();
  writeSummary();
  finished_ = true;
}

void OperatorTraceInputWriter::writeSummary() const {
  const auto summaryFilePath = getOpTraceSummaryFilePath(traceDir_);
  const auto file = fs_->openFileForWrite(summaryFilePath);
  folly::dynamic obj = folly::dynamic::object;
  recordOperatorSummary(traceOp_, obj);
  file->append(folly::toJson(obj));
  file->close();
}

OperatorTraceSplitWriter::OperatorTraceSplitWriter(
    Operator* traceOp,
    std::string traceDir)
    : traceOp_(traceOp),
      traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      splitFile_(fs_->openFileForWrite(getOpTraceSplitFilePath(traceDir_))) {
  VELOX_CHECK_NOT_NULL(splitFile_);
}

void OperatorTraceSplitWriter::write(const exec::Split& split) const {
  // TODO: Supports group later once we have driver id mapping in trace node.
  VELOX_CHECK(!split.hasGroup(), "Do not support grouped execution");
  VELOX_CHECK(split.hasConnectorSplit());
  const auto splitObj = split.connectorSplit->serialize();
  const auto splitJson = folly::toJson(splitObj);
  auto ioBuf = serialize(splitJson);
  splitFile_->append(std::move(ioBuf));
}

void OperatorTraceSplitWriter::finish() {
  if (finished_) {
    return;
  }

  VELOX_CHECK_NOT_NULL(
      splitFile_, "The query data writer has already been finished");
  splitFile_->close();
  writeSummary();
  finished_ = true;
}

// static
std::unique_ptr<folly::IOBuf> OperatorTraceSplitWriter::serialize(
    const std::string& split) {
  const uint32_t length = split.length();
  const uint32_t crc32 = folly::crc32(
      reinterpret_cast<const uint8_t*>(split.data()), split.size());
  auto ioBuf =
      folly::IOBuf::create(sizeof(length) + split.size() + sizeof(crc32));
  folly::io::Appender appender(ioBuf.get(), 0);
  appender.writeLE(length);
  appender.push(reinterpret_cast<const uint8_t*>(split.data()), length);
  appender.writeLE(crc32);
  return ioBuf;
}

void OperatorTraceSplitWriter::writeSummary() const {
  const auto summaryFilePath = getOpTraceSummaryFilePath(traceDir_);
  const auto file = fs_->openFileForWrite(summaryFilePath);
  folly::dynamic obj = folly::dynamic::object;
  recordOperatorSummary(traceOp_, obj);
  file->append(folly::toJson(obj));
  file->close();
}
} // namespace facebook::velox::exec::trace
