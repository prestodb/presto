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

#include <gflags/gflags.h>

DECLARE_bool(usage);
DECLARE_string(root);
DECLARE_bool(summary);
DECLARE_bool(short_summary);
DECLARE_bool(pretty);
DECLARE_string(task_id);

namespace facebook::velox::tool::trace {
/// The tool used to print or replay the traced query metadata and operations.
class QueryTraceReplayer {
 public:
  QueryTraceReplayer();

  void printSummary() const;
  static std::string usage();

 private:
  const std::string rootDir_;
  const std::string taskId_;
};

} // namespace facebook::velox::tool::trace
