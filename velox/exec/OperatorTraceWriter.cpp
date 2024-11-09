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

#include <utility>
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Trace.h"
#include "velox/exec/TraceUtil.h"

namespace facebook::velox::exec::trace {

OperatorTraceWriter::OperatorTraceWriter(
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

void OperatorTraceWriter::write(const RowVectorPtr& rows) {
  if (FOLLY_UNLIKELY(finished_)) {
    return;
  }
  if (FOLLY_UNLIKELY(dataType_ == nullptr)) {
    dataType_ = rows->type();
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

void OperatorTraceWriter::finish() {
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

void OperatorTraceWriter::writeSummary() const {
  const auto summaryFilePath = getOpTraceSummaryFilePath(traceDir_);
  const auto file = fs_->openFileForWrite(summaryFilePath);
  folly::dynamic obj = folly::dynamic::object;
  if (dataType_ != nullptr) {
    obj[TraceTraits::kDataTypeKey] = dataType_->serialize();
  }
  obj[OperatorTraceTraits::kOpTypeKey] = traceOp_->operatorType();
  const auto stats = traceOp_->stats(/*clear=*/false);
  obj[OperatorTraceTraits::kPeakMemoryKey] =
      stats.memoryStats.peakTotalMemoryReservation;
  obj[OperatorTraceTraits::kInputRowsKey] = stats.inputPositions;
  file->append(folly::toJson(obj));
  file->close();
}

} // namespace facebook::velox::exec::trace
