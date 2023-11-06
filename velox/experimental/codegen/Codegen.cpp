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

#include "velox/experimental/codegen/Codegen.h"
#include <glog/logging.h>
#include <memory>
#include "velox/core/PlanNode.h"
#include "velox/experimental/codegen/CodegenCompiledExpressionTransform.h"
#include "velox/experimental/codegen/CodegenExceptions.h"
#include "velox/experimental/codegen/external_process/Command.h"
#include "velox/experimental/codegen/external_process/subprocess.h"
#include "velox/experimental/codegen/proto/ProtoUtils.h"
#include "velox/experimental/codegen/udf_manager/UDFManager.h"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"

namespace facebook {
namespace velox {
namespace codegen {

bool Codegen::initialize(
    const std::string_view& codegenOptionsJson,
    bool lazyLoading) {
  try {
    codegenLogger_->onInitialize(lazyLoading);
    auto codegenOptionsProto = proto::proto_utils::ProtoUtils<
        proto::CodegenOptionsProto>::loadProtoFromJson(codegenOptionsJson);

    useSymbolsForArithmetic_ = codegenOptionsProto.usesymbolsforarithmetic();
    initializeCodeManager(codegenOptionsProto.compileroptions());
    initializeUDFManager();
    initializeTransform();

    if (!lazyLoading) {
      runInitializationTests();
    }
  } catch (std::exception& e) {
    throw CodegenInitializationException(e.what());
  }

  return true;
}

bool Codegen::initializeFromFile(
    const std::filesystem::path& codegenOptionsJsonFile,
    bool lazyLoading) {
  codegenLogger_->onInitializeFromFile(codegenOptionsJsonFile, lazyLoading);
  return initialize(
      proto::proto_utils::readFromFile(codegenOptionsJsonFile).str(),
      lazyLoading);
}

std::shared_ptr<const core::PlanNode> Codegen::compile(
    const core::PlanNode& planNode) {
  codegenLogger_->onCompileStart(planNode);
  auto transformedPlanNode = transform_->transform(planNode);
  codegenLogger_->onCompileEnd(
      *std::static_pointer_cast<DefaultScopedTimer::EventSequence>(
          eventSequence_),
      planNode);
  return transformedPlanNode;
}

bool Codegen::initializeCodeManager(
    const proto::CompilerOptionsProto& compilerOptionsProto) {
  LOG(INFO) << "Codegen: initializing CodeManager";
  eventSequence_ = std::make_shared<DefaultScopedTimer::EventSequence>();
  codeManager_ = std::make_shared<CodeManager>(
      CompilerOptions::fromProto(compilerOptionsProto),
      *std::static_pointer_cast<DefaultEventSequence>(eventSequence_));

  return true;
}

bool Codegen::initializeUDFManager() {
  LOG(INFO) << "Codegen: initializing UDFManager";
  // TODO: we want to register UDFs via config files rather than in code
  udfManager_ = std::make_shared<UDFManager>();
  registerVeloxArithmeticUDFs(*udfManager_);
  return true;
}

bool Codegen::initializeTransform() {
  LOG(INFO) << "Codegen: initializing Transform";
  transform_ = std::make_shared<CodegenCompiledExpressionTransform>(
      CodegenCompiledExpressionTransform(
          codeManager_->compiler().compilerOptions(),
          *udfManager_,
          useSymbolsForArithmetic_,
          *std::static_pointer_cast<DefaultEventSequence>(eventSequence_)));
  return true;
}

bool Codegen::runInitializationTests() {
  LOG(INFO) << "Codegen: running initialization tests";

  auto sourceCode1 = R"a(
  extern "C" {
  int f() {
    return 24;
  };
  }
  )a";

  auto sourceCode2 = R"a(
  extern "C" {
  int g() {
    return 32;
  };
  }
  )a";

  Compiler compiler = codeManager_->compiler();

  LOG(INFO) << "Codegen: attempting to compile test binary1";

  auto binary = compiler.compileString({}, sourceCode1);
  VELOX_CHECK(std::filesystem::exists(binary));
  VELOX_CHECK_GT(std::filesystem::file_size(binary), 0);

  LOG(INFO) << "Codegen: attempting to compile test binary2";

  auto binary2 = compiler.compileString({}, sourceCode2);
  VELOX_CHECK(std::filesystem::exists(binary2));
  VELOX_CHECK_GT(std::filesystem::file_size(binary2), 0);

  LOG(INFO) << "Codegen: attempting to link test sharedObject";

  auto sharedObject = compiler.link({}, {binary, binary2});
  VELOX_CHECK(std::filesystem::exists(sharedObject));
  VELOX_CHECK_GT(std::filesystem::file_size(sharedObject), 0);

  // From dlopen man page, one of RTLD_NOW or RTLD_LAZY must be set
  // in linux machines. Note that mac has no such requirements.
  // https://man7.org/linux/man-pages/man3/dlopen.3.html
  auto libraryPtr =
      dlopen(sharedObject.string().c_str(), RTLD_LOCAL | RTLD_LAZY);
  VELOX_CHECK_EQ(dlerror(), nullptr);

  auto sym_g = (int (*)())dlsym(libraryPtr, "g");
  VELOX_CHECK_EQ(dlerror(), nullptr);
  VELOX_CHECK_EQ(sym_g(), 32);

  auto sym_f = (int (*)())dlsym(libraryPtr, "f");
  VELOX_CHECK_EQ(dlerror(), nullptr);
  VELOX_CHECK_EQ(sym_f(), 24);

  LOG(INFO) << "Codegen: initialization tests done";
  return true;
}

} // namespace codegen
} // namespace velox
}; // namespace facebook
