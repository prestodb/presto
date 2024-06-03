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

#include "velox/connectors/hive/FileHandle.h"
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"

#include <atomic>

namespace facebook::velox {

uint64_t FileHandleSizer::operator()(const FileHandle& fileHandle) {
  // TODO: add to support variable file cache size support when the file system
  // underneath supports.
  return 1;
}

namespace {
// The group tracking is at the level of the directory, i.e. Hive partition.
std::string groupName(const std::string& filename) {
  const char* slash = strrchr(filename.c_str(), '/');
  return slash ? std::string(filename.data(), slash - filename.data())
               : filename;
}
} // namespace

std::unique_ptr<FileHandle> FileHandleGenerator::operator()(
    const std::string& filename,
    const FileProperties* properties) {
  // We have seen cases where drivers are stuck when creating file handles.
  // Adding a trace here to spot this more easily in future.
  process::TraceContext trace("FileHandleGenerator::operator()");
  uint64_t elapsedTimeUs{0};
  std::unique_ptr<FileHandle> fileHandle;
  {
    MicrosecondTimer timer(&elapsedTimeUs);
    fileHandle = std::make_unique<FileHandle>();
    filesystems::FileOptions options;
    if (properties) {
      options.fileSize = properties->fileSize;
    }
    fileHandle->file = filesystems::getFileSystem(filename, properties_)
                           ->openFileForRead(filename, options);
    fileHandle->uuid = StringIdLease(fileIds(), filename);
    fileHandle->groupId = StringIdLease(fileIds(), groupName(filename));
    VLOG(1) << "Generating file handle for: " << filename
            << " uuid: " << fileHandle->uuid.id();
  }
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricHiveFileHandleGenerateLatencyMs, elapsedTimeUs / 1000);
  // TODO: build the hash map/etc per file type -- presumably after reading
  // the appropriate magic number from the file, or perhaps we include the file
  // type in the file handle key.
  return fileHandle;
}

} // namespace facebook::velox
