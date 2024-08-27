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

#include "velox/exec/trace/QueryTraceUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::exec::trace {

void createTraceDirectory(const std::string& traceDir) {
  try {
    const auto fs = filesystems::getFileSystem(traceDir, nullptr);
    if (fs->exists(traceDir)) {
      fs->rmdir(traceDir);
    }
    fs->mkdir(traceDir);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to create trace directory '{}' with error: {}",
        traceDir,
        e.what());
  }
}

} // namespace facebook::velox::exec::trace
