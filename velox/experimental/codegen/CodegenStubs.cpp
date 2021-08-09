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

#include "glog/logging.h"
#include "velox/experimental/codegen/Codegen.h"
#include "velox/experimental/codegen/CodegenExceptions.h"
#include "velox/experimental/codegen/CodegenLogger.h"

namespace facebook {
namespace velox {
namespace codegen {

__attribute__((weak)) bool Codegen::initialize(
    [[maybe_unused]] const std::string_view& codegenOptionsJson,
    [[maybe_unused]] bool lazyLoading) {
  throw CodegenStubsException("Codegen::initialize()");
}

__attribute__((weak)) bool Codegen::initializeFromFile(
    [[maybe_unused]] const std::filesystem::path& codegenOptionsJsonFile,
    [[maybe_unused]] bool lazyLoading) {
  throw CodegenStubsException("Codegen::initializeFromFile()");
}

__attribute__((weak)) std::shared_ptr<const core::PlanNode> Codegen::compile(
    [[maybe_unused]] const core::PlanNode& planNode) {
  throw CodegenStubsException("Codegen::compile()");
}

bool Codegen::initializeCodeManager(
    [[maybe_unused]] const proto::CompilerOptionsProto& compilerOptionsProto) {
  throw CodegenStubsException("Codegen::initializeCodeManager()");
}

bool Codegen::initializeUDFManager() {
  throw CodegenStubsException("Codegen::initializeUDFManager()");
}

bool Codegen::initializeTransform() {
  throw CodegenStubsException("Codegen::initializeTransform()");
}

bool Codegen::runInitializationTests() {
  throw CodegenStubsException("Codegen::runInitializationTests()");
}

} // namespace codegen
} // namespace velox
}; // namespace facebook
