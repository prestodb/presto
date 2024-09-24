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

#include <string>
#include <vector>
#include "velox/common/file/FileSystems.h"

#include <folly/dynamic.h>

namespace facebook::velox::exec::trace {

/// Creates a directory to store the query trace metdata and data.
void createTraceDirectory(const std::string& traceDir);

/// Extracts task ids of the query tracing by listing the trace directory.
std::vector<std::string> getTaskIds(
    const std::string& traceDir,
    const std::shared_ptr<filesystems::FileSystem>& fs);

/// Gets the metadata from the given task directory which includes query plan,
/// configs and connector properties.
folly::dynamic getMetadata(
    const std::string& traceTaskDir,
    const std::shared_ptr<filesystems::FileSystem>& fs);
} // namespace facebook::velox::exec::trace
