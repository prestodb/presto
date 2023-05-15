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

#include "velox/common/file/File.h"

namespace facebook::velox::tests::utils {

struct Result {
  std::vector<std::string> buffers;
  std::vector<ReadFile::Segment> segments;
};

Result getSegments(
    std::vector<std::string> buffers,
    const std::unordered_set<size_t>& skip = {});

} // namespace facebook::velox::tests::utils
