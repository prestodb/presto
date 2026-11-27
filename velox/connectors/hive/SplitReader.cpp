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

#include "velox/connectors/hive/SplitReader.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/ReaderFactory.h"

#include <limits>

namespace facebook::velox::connector::hive {

namespace {

bool isOpenEndedSplit(uint64_t length) {
  return length == std::numeric_limits<uint64_t>::max();
}

} // namespace

void SplitReader::createReader() {
  auto fileHandle = fileHandleFactory_->generate(
      hiveSplit_->filePath,
      {.noCacheRetention = readerConfig_.noCacheRetention()});

  const uint64_t fileSize = fileHandle->file->size();
  // Full-file splits (start == 0) must carry the exact HDFS size. Mismatches
  // from metastore/split generation break footer and stream reads. Open-ended
  // splits (length == max) and partial splits (start > 0) are skipped.
  if (hiveSplit_->start == 0 && !isOpenEndedSplit(hiveSplit_->length) &&
      hiveSplit_->length != fileSize) {
    VELOX_USER_FAIL(
        "Hive split length {} does not match file size {} for {}",
        hiveSplit_->length,
        fileSize,
        hiveSplit_->filePath);
  }
  // Any split that extends past EOF is invalid.
  if (!isOpenEndedSplit(hiveSplit_->length) &&
      hiveSplit_->start + hiveSplit_->length > fileSize) {
    VELOX_USER_FAIL(
        "Hive split [start={}, length={}] exceeds file size {} for {}",
        hiveSplit_->start,
        hiveSplit_->length,
        fileSize,
        hiveSplit_->filePath);
  }

  createReaderFromFileHandle(std::move(fileHandle));
}

} // namespace facebook::velox::connector::hive
