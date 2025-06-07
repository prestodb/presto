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

#include "presto_cpp/main/functions/remote/client/Remote.h"

#include <folly/io/async/EventBase.h>
#include <string>

#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"
#include "velox/common/base/Exceptions.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/fbhive/HiveTypeSerializer.h"

using namespace facebook::velox;
namespace facebook::presto::functions {
namespace {

std::string serializeType(const TypePtr& type) {
  // Use hive type serializer.
  return type::fbhive::HiveTypeSerializer::serialize(type);
}

class PrestoRemoteFunction : public velox::exec::VectorFunction {
 public:
  PrestoRemoteFunction(
      const std::string& functionName,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      const PrestoRemoteFunctionsMetadata& metadata)
      : functionName_(functionName), metadata_(metadata) {
    std::vector<TypePtr> types;
    types.reserve(inputArgs.size());
    serializedInputTypes_.reserve(inputArgs.size());

    for (const auto& arg : inputArgs) {
      types.emplace_back(arg.type);
      serializedInputTypes_.emplace_back(serializeType(arg.type));
    }
    remoteInputType_ = ROW(std::move(types));
    remoteClient_ = std::make_unique<RestRemoteClient>(
        metadata_.location,
        functionName_,
        remoteInputType_,
        serializedInputTypes_,
        metadata_);
  }

 private:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    try {
      VELOX_CHECK(remoteClient_, "Remote client not initialized.");
      remoteClient_->applyRemote(rows, args, outputType, context, result);
    } catch (const VeloxRuntimeError&) {
      throw;
    } catch (const std::exception&) {
      context.setErrors(rows, std::current_exception());
    }
  }

  const std::string functionName_;
  const PrestoRemoteFunctionsMetadata metadata_;

  RowTypePtr remoteInputType_;
  std::vector<std::string> serializedInputTypes_;

  std::unique_ptr<RestRemoteClient> remoteClient_;
};

std::shared_ptr<exec::VectorFunction> createRemoteFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/,
    const PrestoRemoteFunctionsMetadata& metadata) {
  return std::make_unique<PrestoRemoteFunction>(name, inputArgs, metadata);
}

} // namespace

void registerPrestoRemoteFunction(
    const std::string& name,
    std::vector<exec::FunctionSignaturePtr> signatures,
    const PrestoRemoteFunctionsMetadata& metadata,
    bool overwrite) {
  registerStatefulVectorFunction(
      name,
      signatures,
      std::bind(
          createRemoteFunction,
          std::placeholders::_1,
          std::placeholders::_2,
          std::placeholders::_3,
          metadata),
      metadata,
      overwrite);
}

} // namespace facebook::presto::functions
