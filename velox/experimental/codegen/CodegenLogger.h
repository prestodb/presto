/*
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

#include <glog/logging.h>
#include "velox/experimental/codegen/Codegen.h"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"

namespace facebook::velox::codegen {

class Codegen;

class ICodegenLogger {
 public:
  virtual ~ICodegenLogger() {}
  virtual void onInitialize(bool lazyLoading) const = 0;
  virtual void onInitializeFromFile(
      const std::filesystem::path& codegenOptionsJsonFile,
      bool lazyLoading) const = 0;
  virtual void onCompileStart(const core::PlanNode& planNode) const = 0;
  virtual void onCompileEnd(
      DefaultScopedTimer::EventSequence& eventSequence,
      const core::PlanNode& planNode) const = 0;
};

class CodegenTaskLoggerBase : public ICodegenLogger {
 public:
  explicit CodegenTaskLoggerBase(const std::string& taskId) : taskId_(taskId) {}
  virtual ~CodegenTaskLoggerBase() override {}

  void onInitialize(bool lazyLoading) const override {
    auto status = lazyLoading ? "enabled" : "disabled";
    LOG(INFO) << "Initializing codegen with lazyLoading " << status;
  }

  void onInitializeFromFile(
      const std::filesystem::path& codegenOptionsJsonFile,
      bool lazyLoading) const override {
    auto status = lazyLoading ? "enabled" : "disabled";
    LOG(INFO) << "Initializing using CodegenOptions from "
              << codegenOptionsJsonFile << "with lazyLoading " << status;
  }

  void onCompileStart(const core::PlanNode& planNode) const override {
    LOG(INFO) << "Starting codegen planNode transformation for planNode "
              << planNode.toString() << ", taskId " << taskId_;
  }

  void onCompileEnd(
      DefaultScopedTimer::EventSequence& eventSequence,
      const core::PlanNode& planNode) const override;

 protected:
  const std::string& taskId_;
};

using DefaultLogger = CodegenTaskLoggerBase;

} // namespace facebook::velox::codegen
