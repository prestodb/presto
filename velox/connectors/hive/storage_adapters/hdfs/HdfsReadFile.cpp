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

#include "HdfsReadFile.h"
#include <folly/synchronization/CallOnce.h>
#include <hdfs/hdfs.h>

namespace facebook::velox {

HdfsReadFile::HdfsReadFile(hdfsFS hdfs, const std::string_view path)
    : hdfsClient_(hdfs), filePath_(path) {
  fileInfo_ = hdfsGetPathInfo(hdfsClient_, filePath_.data());
  if (fileInfo_ == nullptr) {
    auto error = hdfsGetLastError();
    auto errMsg = fmt::format(
        "Unable to get file path info for file: {}. got error: {}",
        filePath_,
        error);
    if (std::strstr(error, "FileNotFoundException") != nullptr) {
      VELOX_FILE_NOT_FOUND_ERROR(errMsg);
    }
    VELOX_FAIL(errMsg);
  }
}

HdfsReadFile::~HdfsReadFile() {
  // should call hdfsFreeFileInfo to avoid memory leak
  hdfsFreeFileInfo(fileInfo_, 1);
}

void HdfsReadFile::preadInternal(uint64_t offset, uint64_t length, char* pos)
    const {
  checkFileReadParameters(offset, length);
  if (!file_->handle_) {
    file_->open(hdfsClient_, filePath_);
  }
  file_->seek(offset);
  uint64_t totalBytesRead = 0;
  while (totalBytesRead < length) {
    auto bytesRead = file_->read(pos, length - totalBytesRead);
    totalBytesRead += bytesRead;
    pos += bytesRead;
  }
}

std::string_view
HdfsReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  preadInternal(offset, length, static_cast<char*>(buf));
  return {static_cast<char*>(buf), length};
}

std::string HdfsReadFile::pread(uint64_t offset, uint64_t length) const {
  std::string result(length, 0);
  char* pos = result.data();
  preadInternal(offset, length, pos);
  return result;
}

uint64_t HdfsReadFile::size() const {
  return fileInfo_->mSize;
}

uint64_t HdfsReadFile::memoryUsage() const {
  return fileInfo_->mBlockSize;
}

bool HdfsReadFile::shouldCoalesce() const {
  return false;
}

void HdfsReadFile::checkFileReadParameters(uint64_t offset, uint64_t length)
    const {
  auto fileSize = size();
  auto endPoint = offset + length;
  VELOX_CHECK_GE(
      fileSize,
      endPoint,
      "Cannot read HDFS file beyond its size: {}, offset: {}, end point: {}",
      fileSize,
      offset,
      endPoint)
}
} // namespace facebook::velox
