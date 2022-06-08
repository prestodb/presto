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
  VELOX_CHECK_NOT_NULL(
      fileInfo_,
      "Unable to get file path info for file: {}. got error: {}",
      filePath_,
      hdfsGetLastError());
}

void HdfsReadFile::preadInternal(uint64_t offset, uint64_t length, char* pos)
    const {
  checkFileReadParameters(offset, length);
  auto file = hdfsOpenFile(hdfsClient_, filePath_.data(), O_RDONLY, 0, 0, 0);
  VELOX_CHECK_NOT_NULL(
      file,
      "Unable to open file {}. got error: {}",
      filePath_,
      hdfsGetLastError());
  seekToPosition(file, offset);
  uint64_t totalBytesRead = 0;
  while (totalBytesRead < length) {
    auto bytesRead = hdfsRead(hdfsClient_, file, pos, length - totalBytesRead);
    VELOX_CHECK(bytesRead >= 0, "Read failure in HDFSReadFile::preadInternal.")
    totalBytesRead += bytesRead;
    pos += bytesRead;
  }

  if (hdfsCloseFile(hdfsClient_, file) == -1) {
    LOG(ERROR) << "Unable to close file, errno: " << errno;
  }
}

void HdfsReadFile::seekToPosition(hdfsFile file, uint64_t offset) const {
  auto seekStatus = hdfsSeek(hdfsClient_, file, offset);
  VELOX_CHECK_EQ(
      seekStatus,
      0,
      "Cannot seek through HDFS file: {}, error: {}",
      filePath_,
      std::string(hdfsGetLastError()));
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
