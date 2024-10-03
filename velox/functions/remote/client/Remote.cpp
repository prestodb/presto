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

#include "velox/functions/remote/client/Remote.h"

#include <folly/io/async/EventBase.h>
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/remote/client/ThriftClient.h"
#include "velox/functions/remote/if/GetSerde.h"
#include "velox/functions/remote/if/gen-cpp2/RemoteFunctionServiceAsyncClient.h"
#include "velox/type/fbhive/HiveTypeSerializer.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::functions {
namespace {

std::string serializeType(const TypePtr& type) {
  // Use hive type serializer.
  return type::fbhive::HiveTypeSerializer::serialize(type);
}

class RemoteFunction : public exec::VectorFunction {
 public:
  RemoteFunction(
      const std::string& functionName,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      const RemoteVectorFunctionMetadata& metadata)
      : functionName_(functionName),
        location_(metadata.location),
        thriftClient_(getThriftClient(location_, &eventBase_)),
        serdeFormat_(metadata.serdeFormat),
        serde_(getSerde(serdeFormat_)) {
    std::vector<TypePtr> types;
    types.reserve(inputArgs.size());
    serializedInputTypes_.reserve(inputArgs.size());

    for (const auto& arg : inputArgs) {
      types.emplace_back(arg.type);
      serializedInputTypes_.emplace_back(serializeType(arg.type));
    }
    remoteInputType_ = ROW(std::move(types));
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    try {
      applyRemote(rows, args, outputType, context, result);
    } catch (const VeloxRuntimeError&) {
      throw;
    } catch (const std::exception&) {
      context.setErrors(rows, std::current_exception());
    }
  }

 private:
  void applyRemote(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    // Create type and row vector for serialization.
    auto remoteRowVector = std::make_shared<RowVector>(
        context.pool(),
        remoteInputType_,
        BufferPtr{},
        rows.end(),
        std::move(args));

    // Send to remote server.
    remote::RemoteFunctionResponse remoteResponse;
    remote::RemoteFunctionRequest request;
    request.throwOnError_ref() = context.throwOnError();

    auto functionHandle = request.remoteFunctionHandle_ref();
    functionHandle->name_ref() = functionName_;
    functionHandle->returnType_ref() = serializeType(outputType);
    functionHandle->argumentTypes_ref() = serializedInputTypes_;

    auto requestInputs = request.inputs_ref();
    requestInputs->rowCount_ref() = remoteRowVector->size();
    requestInputs->pageFormat_ref() = serdeFormat_;

    // TODO: serialize only active rows.
    requestInputs->payload_ref() = rowVectorToIOBuf(
        remoteRowVector, rows.end(), *context.pool(), serde_.get());

    try {
      thriftClient_->sync_invokeFunction(remoteResponse, request);
    } catch (const std::exception& e) {
      VELOX_FAIL(
          "Error while executing remote function '{}' at '{}': {}",
          functionName_,
          location_.describe(),
          e.what());
    }

    auto outputRowVector = IOBufToRowVector(
        remoteResponse.get_result().get_payload(),
        ROW({outputType}),
        *context.pool(),
        serde_.get());
    result = outputRowVector->childAt(0);

    if (auto errorPayload = remoteResponse.get_result().errorPayload()) {
      auto errorsRowVector = IOBufToRowVector(
          *errorPayload, ROW({VARCHAR()}), *context.pool(), serde_.get());
      auto errorsVector =
          errorsRowVector->childAt(0)->asFlatVector<StringView>();
      VELOX_CHECK(errorsVector, "Should be convertible to flat vector");

      SelectivityVector selectedRows(errorsRowVector->size());
      selectedRows.applyToSelected([&](vector_size_t i) {
        if (errorsVector->isNullAt(i)) {
          return;
        }
        try {
          throw std::runtime_error(errorsVector->valueAt(i));
        } catch (const std::exception& ex) {
          context.setError(i, std::current_exception());
        }
      });
    }
  }

  const std::string functionName_;
  folly::SocketAddress location_;

  folly::EventBase eventBase_;
  std::unique_ptr<RemoteFunctionClient> thriftClient_;
  remote::PageFormat serdeFormat_;
  std::unique_ptr<VectorSerde> serde_;

  // Structures we construct once to cache:
  RowTypePtr remoteInputType_;
  std::vector<std::string> serializedInputTypes_;
};

std::shared_ptr<exec::VectorFunction> createRemoteFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/,
    const RemoteVectorFunctionMetadata& metadata) {
  return std::make_unique<RemoteFunction>(name, inputArgs, metadata);
}

} // namespace

void registerRemoteFunction(
    const std::string& name,
    std::vector<exec::FunctionSignaturePtr> signatures,
    const RemoteVectorFunctionMetadata& metadata,
    bool overwrite) {
  exec::registerStatefulVectorFunction(
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

} // namespace facebook::velox::functions
