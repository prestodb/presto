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

#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec::trace {
class QueryMetadataReader {
 public:
  explicit QueryMetadataReader(std::string traceDir, memory::MemoryPool* pool);

  void read(
      std::unordered_map<std::string, std::string>& queryConfigs,
      std::unordered_map<
          std::string,
          std::unordered_map<std::string, std::string>>& connectorProperties,
      core::PlanNodePtr& queryPlan) const;

 private:
  const std::string traceDir_;
  const std::shared_ptr<filesystems::FileSystem> fs_;
  const std::string metaFilePath_;
  memory::MemoryPool* const pool_;
};
} // namespace facebook::velox::exec::trace
