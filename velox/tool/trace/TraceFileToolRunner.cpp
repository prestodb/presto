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

#include "velox/tool/trace/TraceFileToolRunner.h"
#include "velox/common/file/FileSystems.h"

DEFINE_string(trace_file_op, "copy", "Operation type of this run");
DEFINE_string(
    source_root_dir,
    "",
    "Source root directory of the tracing data, it must be set");
DEFINE_string(
    dest_root_dir,
    "",
    "Dest root directory, it must be set if the operation type is copy");
DEFINE_string(trace_query_id, "", "Specify the trace query id");
DEFINE_string(trace_task_id, "", "Specify the trace task id");

namespace facebook::velox::tool::trace {

TraceFileToolRunner::TraceFileToolRunner()
    : sourceRootDir_(FLAGS_source_root_dir), destRootDir_(FLAGS_dest_root_dir) {
  VELOX_USER_CHECK(!sourceRootDir_.empty());
}

void TraceFileToolRunner::init() {
  filesystems::registerLocalFileSystem();
  sourceFs_ = filesystems::getFileSystem(sourceRootDir_, nullptr);
  VELOX_CHECK_NOT_NULL(sourceFs_);
  if (FLAGS_trace_file_op == "copy") {
    VELOX_USER_CHECK(!destRootDir_.empty());
    destFs_ = filesystems::getFileSystem(destRootDir_, nullptr);
    VELOX_CHECK_NOT_NULL(destFs_);
    std::string copyRootDir;
    if (FLAGS_trace_query_id.empty()) {
      VELOX_CHECK(
          FLAGS_trace_task_id.empty(),
          "Trace query ID is empty but trace task ID is not empty");
      copyRootDir = sourceRootDir_;
    } else if (FLAGS_trace_task_id.empty()) {
      copyRootDir = fmt::format("{}/{}", sourceRootDir_, FLAGS_trace_query_id);
    } else {
      copyRootDir = fmt::format(
          "{}/{}/{}",
          sourceRootDir_,
          FLAGS_trace_query_id,
          FLAGS_trace_task_id);
    }
    listFiles(copyRootDir);
  } else {
    VELOX_UNSUPPORTED(
        "Unsupported trace file operation type {}", FLAGS_trace_file_op);
  }
}

void TraceFileToolRunner::run() {
  if (FLAGS_trace_file_op == "copy") {
    copyFiles();
  } else {
    VELOX_UNSUPPORTED(
        "Unsupported trace file operation type {}", FLAGS_trace_file_op);
  }
}

void TraceFileToolRunner::copyFiles() const {
  const auto prefixLen = sourceFs_->extractPath(sourceRootDir_).length();
  for (const auto& source : sourceFiles_) {
    const auto targetFile =
        fmt::format("{}/{}", destRootDir_, source.substr(prefixLen));
    const auto readFile = sourceFs_->openFileForRead(source);
    const auto writeFile = destFs_->openFileForWrite(
        targetFile,
        filesystems::FileOptions{
            .values = {},
            .fileSize = std::nullopt,
            .shouldCreateParentDirectories = true});
    const auto fileSize = readFile->size();
    constexpr auto batchSize = 4 << 10;
    const auto ioBuf = folly::IOBuf::create(batchSize);
    uint64_t offset = 0;
    while (offset < fileSize) {
      const auto curLen = std::min<uint64_t>(fileSize - offset, batchSize);
      const auto dataView =
          readFile->pread(offset, curLen, ioBuf->writableData());
      writeFile->append(dataView);
      ioBuf->append(curLen);
      offset += curLen;
      ioBuf->clear();
    }
    writeFile->flush();
    writeFile->close();
  }
}

void TraceFileToolRunner::listFiles(const std::string& path) {
  VELOX_USER_CHECK(sourceFs_->exists(path), "{} dose not exist", path);
  if (!sourceFs_->isDirectory(path)) {
    sourceFiles_.push_back(path);
    return;
  }

  for (const auto& p : sourceFs_->list(sourceFs_->extractPath(path))) {
    listFiles(p);
  }
}

} // namespace facebook::velox::tool::trace
