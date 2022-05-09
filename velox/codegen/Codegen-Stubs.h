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

#if CODEGEN_ENABLED == 1
#error "This files shouldn't be included when the codegen is enabled"
#endif

#include <filesystem>
#include "glog/logging.h"
#include "velox/core/PlanNode.h"

namespace facebook {
namespace velox {
namespace codegen {

/// Main interface/entry point with the code generation system.
/// This is stub code used when the code generation is disabled.

class Codegen {
 public:
  Codegen() {}

  bool initialize(
      const std::string_view& codegenOptionsJson,
      bool lazyLoading = true) {
    LOG(INFO) << "Codegen disabled, doing nothing : " << std::endl;
    return true;
  }

  bool initializeFromFile(
      const std::filesystem::path& codegenOptionsJsonFile,
      bool lazyLoading = true) {
    LOG(INFO) << "Codegen disabled, doing nothing : " << std::endl;
    return true;
  };

  std::shared_ptr<const core::PlanNode> compile(
      const core::PlanNode& planNode) {
    LOG(INFO) << "Codegen disabled, doing nothing" << std::endl;
    return nullptr;
  }
};

} // namespace codegen
} // namespace velox
}; // namespace facebook
