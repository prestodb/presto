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

#include "presto_cpp/main/functions/remote/client/VeloxRemoteFunction.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"
#include "velox/functions/remote/if/GetSerde.h"
#include "velox/type/fbhive/HiveTypeSerializer.h"
#include "velox/vector/VectorStream.h"

using namespace facebook::velox;
namespace facebook::presto::functions::rest {
namespace {

std::string serializeType(const TypePtr& type) {
  // Use hive type serializer.
  return type::fbhive::HiveTypeSerializer::serialize(type);
}

RowTypePtr createRemoteInputType(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  std::vector<TypePtr> types;
  types.reserve(inputArgs.size());
  for (const auto& arg : inputArgs) {
    types.emplace_back(arg.type);
  }
  return ROW(std::move(types));
}

class VeloxRemoteFunction : public velox::exec::VectorFunction {
 public:
  VeloxRemoteFunction(
      const std::string& functionName,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      const VeloxRemoteFunctionMetadata& metadata,
      RestRemoteClientPtr remoteClient)
      : functionName_(functionName),
        metadata_(metadata),
        serde_(velox::functions::getSerde(metadata_.serdeFormat)),
        remoteInputType_(createRemoteInputType(inputArgs)),
        remoteClient_(std::move(remoteClient)) {
    serializedInputTypes_.reserve(inputArgs.size());

    for (const auto& arg : inputArgs) {
      serializedInputTypes_.emplace_back(serializeType(arg.type));
    }
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

      auto remoteRowVector = std::make_shared<RowVector>(
          context.pool(),
          remoteInputType_,
          BufferPtr{},
          rows.end(),
          std::move(args));

      auto requestBody = std::make_unique<folly::IOBuf>(rowVectorToIOBuf(
          remoteRowVector, rows.end(), *context.pool(), serde_.get()));

      auto responseBody = remoteClient_->invokeFunction(
          metadata_.location, metadata_.serdeFormat, std::move(requestBody));

      if (!responseBody) {
        VELOX_FAIL("No response received from remote function invocation.");
      }

      auto outputRowVector = IOBufToRowVector(
          *responseBody, ROW({outputType}), *context.pool(), serde_.get());

      result = outputRowVector->childAt(0);
    } catch (const VeloxRuntimeError&) {
      throw;
    } catch (const std::exception&) {
      context.setErrors(rows, std::current_exception());
    }
  }

  const std::string functionName_;
  const VeloxRemoteFunctionMetadata metadata_;
  const std::unique_ptr<velox::VectorSerde> serde_;

  const RowTypePtr remoteInputType_;
  std::vector<std::string> serializedInputTypes_;

  const RestRemoteClientPtr remoteClient_;
};

std::shared_ptr<exec::VectorFunction> createRemoteFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/,
    const VeloxRemoteFunctionMetadata& metadata,
    RestRemoteClientPtr remoteClient) {
  return std::make_shared<VeloxRemoteFunction>(
      name, inputArgs, metadata, remoteClient);
}

} // namespace

void registerVeloxRemoteFunction(
    const std::string& name,
    const std::vector<exec::FunctionSignaturePtr>& signatures,
    VeloxRemoteFunctionMetadata metadata,
    RestRemoteClientPtr remoteClient,
    bool overwrite) {
  registerStatefulVectorFunction(
      name,
      signatures,
      std::bind(
          createRemoteFunction,
          std::placeholders::_1,
          std::placeholders::_2,
          std::placeholders::_3,
          metadata,
          remoteClient),
      std::move(metadata),
      overwrite);
}

} // namespace facebook::presto::functions::rest
