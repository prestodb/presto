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
#include "velox/external/hdfs/ArrowHdfsInternal.h"

namespace facebook::velox {

struct HdfsFile {
  filesystems::arrow::io::internal::LibHdfsShim* driver_;
  hdfsFS client_;
  hdfsFile handle_;

  HdfsFile() : driver_(nullptr), client_(nullptr), handle_(nullptr) {}
  ~HdfsFile() {
    if (handle_ && driver_->CloseFile(client_, handle_) == -1) {
      LOG(ERROR) << "Unable to close file, errno: " << errno;
    }
  }

  void open(
      filesystems::arrow::io::internal::LibHdfsShim* driver,
      hdfsFS client,
      const std::string& path) {
    driver_ = driver;
    client_ = client;
    handle_ = driver->OpenFile(client, path.data(), O_RDONLY, 0, 0, 0);
    VELOX_CHECK_NOT_NULL(
        handle_,
        "Unable to open file {}. got error: {}",
        path,
        driver_->GetLastExceptionRootCause());
  }

  void seek(uint64_t offset) const {
    VELOX_CHECK_EQ(
        driver_->Seek(client_, handle_, offset),
        0,
        "Cannot seek through HDFS file, error is : {}",
        driver_->GetLastExceptionRootCause());
  }

  int32_t read(char* pos, uint64_t length) const {
    auto bytesRead = driver_->Read(client_, handle_, pos, length);
    VELOX_CHECK(bytesRead >= 0, "Read failure in HDFSReadFile::preadInternal.");
    return bytesRead;
  }
};

HdfsReadFile::HdfsReadFile(
    filesystems::arrow::io::internal::LibHdfsShim* driver,
    hdfsFS hdfs,
    const std::string_view path)
    : driver_(driver), hdfsClient_(hdfs), filePath_(path) {
  fileInfo_ = driver_->GetPathInfo(hdfsClient_, filePath_.data());
  if (fileInfo_ == nullptr) {
    auto error = fmt::format(
        "FileNotFoundException: Path {} does not exist.", filePath_);
    auto errMsg = fmt::format(
        "Unable to get file path info for file: {}. got error: {}",
        filePath_,
        error);
    if (error.find("FileNotFoundException") != std::string::npos) {
      VELOX_FILE_NOT_FOUND_ERROR(errMsg);
    }
    VELOX_FAIL(errMsg);
  }
}

HdfsReadFile::~HdfsReadFile() {
  // should call hdfsFreeFileInfo to avoid memory leak
  if (fileInfo_) {
    driver_->FreeFileInfo(fileInfo_, 1);
  }
}

void HdfsReadFile::preadInternal(uint64_t offset, uint64_t length, char* pos)
    const {
  checkFileReadParameters(offset, length);
  folly::ThreadLocal<HdfsFile> file;
  if (!file->handle_) {
    file->open(driver_, hdfsClient_, filePath_);
  }
  file->seek(offset);
  uint64_t totalBytesRead = 0;
  while (totalBytesRead < length) {
    auto bytesRead = file->read(pos, length - totalBytesRead);
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
      endPoint);
}
} // namespace facebook::velox
