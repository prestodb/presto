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

#pragma once

#include <folly/io/IOBuf.h>
#include <gflags/gflags.h>
#include <string>
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/Utils.h"

DECLARE_string(source_root_dir);
DECLARE_string(dest_root_dir);
DECLARE_string(trace_file_op);
DECLARE_string(trace_query_id);
DECLARE_string(trace_task_id);

namespace facebook::velox::tool::trace {

/// The trace replay runner. It is configured through a set of gflags passed
/// from replayer tool command line.
class TraceFileToolRunner {
 public:
  TraceFileToolRunner();
  virtual ~TraceFileToolRunner() = default;

  /// Initializes the trace file tool runner by setting the velox runtime
  /// environment for the trace file operations. It is invoked before run().
  virtual void init();

  /// Runs the trace file operations.
  void run();

 private:
  // List all the files in the source root dir recursively.
  void listFiles(const std::string& path);

  void copyFiles() const;

  const std::string sourceRootDir_;
  const std::string destRootDir_;
  std::shared_ptr<filesystems::FileSystem> sourceFs_;
  std::shared_ptr<filesystems::FileSystem> destFs_;
  std::vector<std::string> sourceFiles_;
};

} // namespace facebook::velox::tool::trace
