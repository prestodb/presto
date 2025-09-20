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

#include "velox/connectors/hive/storage_adapters/hdfs/HdfsWriteFile.h"
#include "velox/external/hdfs/ArrowHdfsInternal.h"

namespace facebook::velox {
HdfsWriteFile::HdfsWriteFile(
    filesystems::arrow::io::internal::LibHdfsShim* driver,
    hdfsFS hdfsClient,
    std::string_view path,
    int bufferSize,
    short replication,
    int blockSize)
    : driver_(driver), hdfsClient_(hdfsClient), filePath_(path) {
  auto pos = filePath_.rfind("/");
  auto parentDir = filePath_.substr(0, pos + 1);
  if (driver_->Exists(hdfsClient_, parentDir.c_str()) == -1) {
    driver_->MakeDirectory(hdfsClient_, parentDir.c_str());
  }

  hdfsFile_ = driver_->OpenFile(
      hdfsClient_,
      filePath_.c_str(),
      O_WRONLY,
      bufferSize,
      replication,
      blockSize);
  VELOX_CHECK_NOT_NULL(
      hdfsFile_,
      "Failed to open hdfs file: {}, with error: {}",
      filePath_,
      driver_->GetLastExceptionRootCause());
}

HdfsWriteFile::~HdfsWriteFile() {
  if (hdfsFile_) {
    close();
  }
}

void HdfsWriteFile::close() {
  int success = driver_->CloseFile(hdfsClient_, hdfsFile_);
  common::testutil::TestValue::adjust(
      "facebook::velox::connectors::hive::HdfsWriteFile::close", &success);
  hdfsFile_ = nullptr;
  VELOX_CHECK_EQ(
      success,
      0,
      "Failed to close hdfs file: {}",
      driver_->GetLastExceptionRootCause());
}

void HdfsWriteFile::flush() {
  VELOX_CHECK_NOT_NULL(
      hdfsFile_,
      "Cannot flush HDFS file because file handle is null, file path: {}",
      filePath_);
  int success = driver_->Flush(hdfsClient_, hdfsFile_);
  VELOX_CHECK_EQ(
      success, 0, "Hdfs flush error: {}", driver_->GetLastExceptionRootCause());
}

void HdfsWriteFile::append(std::string_view data) {
  if (data.size() == 0) {
    return;
  }
  VELOX_CHECK_NOT_NULL(
      hdfsFile_,
      "Cannot append to HDFS file because file handle is null, file path: {}",
      filePath_);
  int64_t totalWrittenBytes = driver_->Write(
      hdfsClient_, hdfsFile_, std::string(data).c_str(), data.size());
  VELOX_CHECK_EQ(
      totalWrittenBytes,
      data.size(),
      "Write failure in HDFSWriteFile::append {}",
      driver_->GetLastExceptionRootCause());
}

uint64_t HdfsWriteFile::size() const {
  auto fileInfo = driver_->GetPathInfo(hdfsClient_, filePath_.c_str());
  uint64_t size = fileInfo->mSize;
  // should call hdfsFreeFileInfo to avoid memory leak
  driver_->FreeFileInfo(fileInfo, 1);
  return size;
}

} // namespace facebook::velox
