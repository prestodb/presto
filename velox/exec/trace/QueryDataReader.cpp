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

#include "velox/exec/trace/QueryDataReader.h"

#include "velox/common/file/File.h"
#include "velox/exec/trace/QueryTraceTraits.h"

namespace facebook::velox::exec::trace {

QueryDataReader::QueryDataReader(std::string path, memory::MemoryPool* pool)
    : path_(std::move(path)),
      fs_(filesystems::getFileSystem(path_, nullptr)),
      pool_(pool),
      dataType_(getTraceDataType()),
      dataStream_(getDataInputStream()) {
  VELOX_CHECK_NOT_NULL(dataType_);
  VELOX_CHECK_NOT_NULL(dataStream_);
}

bool QueryDataReader::read(RowVectorPtr& batch) const {
  if (dataStream_->atEnd()) {
    batch = nullptr;
    return false;
  }

  VectorStreamGroup::read(
      dataStream_.get(), pool_, dataType_, &batch, &readOptions_);
  return true;
}

RowTypePtr QueryDataReader::getTraceDataType() const {
  const auto summaryFile = fs_->openFileForRead(
      fmt::format("{}/{}", path_, QueryTraceTraits::kDataSummaryFileName));
  const auto summary = summaryFile->pread(0, summaryFile->size());
  VELOX_USER_CHECK(!summary.empty());
  folly::dynamic obj = folly::parseJson(summary);
  return ISerializable::deserialize<RowType>(obj["rowType"]);
}

std::unique_ptr<common::FileInputStream> QueryDataReader::getDataInputStream()
    const {
  auto dataFile = fs_->openFileForRead(
      fmt::format("{}/{}", path_, QueryTraceTraits::kDataFileName));
  // TODO: Make the buffer size configurable.
  return std::make_unique<common::FileInputStream>(
      std::move(dataFile), 1 << 20, pool_);
}

} // namespace facebook::velox::exec::trace
