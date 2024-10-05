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

#include "velox/exec/QueryDataReader.h"

#include "velox/common/file/File.h"
#include "velox/exec/QueryTraceTraits.h"

namespace facebook::velox::exec::trace {

QueryDataReader::QueryDataReader(
    std::string traceDir,
    RowTypePtr dataType,
    memory::MemoryPool* pool)
    : traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      dataType_(std::move(dataType)),
      pool_(pool),
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

std::unique_ptr<common::FileInputStream> QueryDataReader::getDataInputStream()
    const {
  auto dataFile = fs_->openFileForRead(
      fmt::format("{}/{}", traceDir_, QueryTraceTraits::kDataFileName));
  // TODO: Make the buffer size configurable.
  return std::make_unique<common::FileInputStream>(
      std::move(dataFile), 1 << 20, pool_);
}

} // namespace facebook::velox::exec::trace
