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

#include "velox/exec/QueryDataWriter.h"

#include <utility>
#include "velox/common/base/SpillStats.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/QueryTraceTraits.h"
#include "velox/serializers/PrestoSerializer.h"

namespace facebook::velox::exec::trace {

QueryDataWriter::QueryDataWriter(
    std::string path,
    memory::MemoryPool* pool,
    UpdateAndCheckTraceLimitCB updateAndCheckTraceLimitCB)
    : dirPath_(std::move(path)),
      fs_(filesystems::getFileSystem(dirPath_, nullptr)),
      pool_(pool),
      updateAndCheckTraceLimitCB_(std::move(updateAndCheckTraceLimitCB)) {
  dataFile_ = fs_->openFileForWrite(
      fmt::format("{}/{}", dirPath_, QueryTraceTraits::kDataFileName));
  VELOX_CHECK_NOT_NULL(dataFile_);
}

void QueryDataWriter::write(const RowVectorPtr& rows) {
  if (FOLLY_UNLIKELY(finished_)) {
    return;
  }
  if (FOLLY_UNLIKELY(dataType_ == nullptr)) {
    dataType_ = rows->type();
  }

  if (batch_ == nullptr) {
    batch_ = std::make_unique<VectorStreamGroup>(pool_);
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
  if (FOLLY_UNLIKELY(
          updateAndCheckTraceLimitCB_(iobuf->computeChainDataLength()))) {
    finish(true);
    return;
  }
  dataFile_->append(std::move(iobuf));
}

void QueryDataWriter::finish(bool limitExceeded) {
  if (finished_) {
    return;
  }

  VELOX_CHECK_NOT_NULL(
      dataFile_, "The query data writer has already been finished");
  dataFile_->close();
  dataFile_.reset();
  batch_.reset();
  writeSummary(limitExceeded);
  finished_ = true;
}

void QueryDataWriter::writeSummary(bool limitExceeded) const {
  const auto summaryFilePath =
      fmt::format("{}/{}", dirPath_, QueryTraceTraits::kDataSummaryFileName);
  const auto file = fs_->openFileForWrite(summaryFilePath);
  folly::dynamic obj = folly::dynamic::object;
  if (dataType_ != nullptr) {
    obj[QueryTraceTraits::kDataTypeKey] = dataType_->serialize();
  }
  obj[QueryTraceTraits::kTraceLimitExceededKey] = limitExceeded;
  file->append(folly::toJson(obj));
  file->close();
}

} // namespace facebook::velox::exec::trace
