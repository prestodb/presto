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

namespace facebook::velox::exec::trace {
class TaskTraceMetadataReader {
 public:
  TaskTraceMetadataReader(std::string traceDir, memory::MemoryPool* pool);

  /// Returns trace query config;
  std::unordered_map<std::string, std::string> queryConfigs() const;

  /// Returns trace query connector properties;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
  connectorProperties() const;

  /// Returns trace query plan;
  core::PlanNodePtr queryPlan() const;

  /// Returns node name in the trace query plan by ID.
  std::string nodeName(const std::string& nodeId) const;

  /// Returns optional of connector ID in the TableScanNode. If nullptr, then no
  /// connector will be registered.
  std::optional<std::string> connectorId(const std::string& nodeId) const;

 private:
  const std::string traceDir_;
  const std::shared_ptr<filesystems::FileSystem> fs_;
  const std::string traceFilePath_;
  memory::MemoryPool* const pool_;
  const folly::dynamic metadataObj_;
  const core::PlanNodePtr tracePlanNode_;
};
} // namespace facebook::velox::exec::trace
