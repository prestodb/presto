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

#include <filesystem>
#include <string>
#include "velox/core/PlanNode.h"
#include "velox/experimental/codegen/CodegenLogger.h"
#include "velox/experimental/codegen/proto/codegen_proto.pb.h"

namespace facebook {
namespace velox {
namespace codegen {

class CodeManager;
class UDFManager;
class CodegenCompiledExpressionTransform;
class ICodegenLogger;

/// Main interface/entry point with the  code generation system.
/// The init method has to be called before any other method on this object.
class Codegen {
 public:
  explicit Codegen(std::shared_ptr<ICodegenLogger> codegenLogger)
      : codegenLogger_(codegenLogger) {}

  bool initialize(
      const std::string_view& codegenOptionsJson,
      bool lazyLoading = true);

  bool initializeFromFile(
      const std::filesystem::path& codegenOptionsJsonFile,
      bool lazyLoading = true);

  std::shared_ptr<const core::PlanNode> compile(const core::PlanNode& planNode);

 private:
  std::shared_ptr<ICodegenLogger> codegenLogger_;

  std::shared_ptr<CodeManager> codeManager_;
  std::shared_ptr<UDFManager> udfManager_;
  std::shared_ptr<CodegenCompiledExpressionTransform> transform_;

  // We need to type erase DefaultEventSequence here to break the dep. with
  // NestedScopedTimer.h
  std::shared_ptr<void /*DefaultEventSequence*/> eventSequence_;

  // Follows Velox, defaults to false
  bool useSymbolsForArithmetic_ = false;

  bool initializeCodeManager(
      const proto::CompilerOptionsProto& compilerOptionsProto);

  bool initializeUDFManager();

  bool initializeTransform();

  bool runInitializationTests();
};

} // namespace codegen
} // namespace velox
}; // namespace facebook
