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

#include <gflags/gflags.h>
#include "velox/common/memory/Memory.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/type/Type.h"

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGCSFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/tool/trace/QueryTraceReplayer.h"

using namespace facebook::velox;

namespace {
void init() {
  memory::initializeMemoryManager({});
  filesystems::registerLocalFileSystem();
  filesystems::registerS3FileSystem();
  filesystems::registerHdfsFileSystem();
  filesystems::registerGCSFileSystem();
  filesystems::abfs::registerAbfsFileSystem();
  Type::registerSerDe();
  core::PlanNode::registerSerDe();
  core::ITypedExpr::registerSerDe();
  exec::registerPartitionFunctionSerDe();
}
} // namespace

int main(int argc, char** argv) {
  if (argc == 1) {
    LOG(ERROR) << "\n" << tool::trace::QueryTraceReplayer::usage();
    return 1;
  }

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_usage) {
    LOG(INFO) << "\n" << tool::trace::QueryTraceReplayer::usage();
    return 0;
  }

  if (FLAGS_root.empty()) {
    LOG(ERROR) << "Root dir is not provided!\n"
               << tool::trace::QueryTraceReplayer::usage();
    return 1;
  }

  if (!FLAGS_summary && !FLAGS_short_summary) {
    LOG(ERROR) << "Only support to print traced query metadata for now";
    return 1;
  }

  init();
  const auto tool = std::make_unique<tool::trace::QueryTraceReplayer>();
  if (FLAGS_summary || FLAGS_short_summary) {
    tool->printSummary();
    return 0;
  }

  VELOX_UNREACHABLE(tool::trace::QueryTraceReplayer::usage());
}
