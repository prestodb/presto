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

#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"

using namespace facebook::velox;
namespace facebook::presto::functions {
RestRemoteClient::RestRemoteClient(
    const std::string& url,
    const std::string& functionName,
    RowTypePtr remoteInputType,
    std::vector<std::string> serializedInputTypes,
    const PrestoRemoteFunctionsMetadata& metadata)
    : RemoteClient(
          functionName,
          std::move(remoteInputType),
          std::move(serializedInputTypes),
          metadata),
      restClient_(getRestClient()),
      url_(url) {}
void RestRemoteClient::applyRemote(
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    const TypePtr& outputType,
    exec::EvalCtx& context,
    VectorPtr& result) const {
  VELOX_DCHECK_NOT_NULL(restClient_, "Rest Client not initialized properly.");
  try {
    auto remoteRowVector = std::make_shared<RowVector>(
        context.pool(),
        remoteInputType_,
        BufferPtr{},
        rows.end(),
        std::move(args));

    std::unique_ptr<folly::IOBuf> requestBody =
        std::make_unique<folly::IOBuf>(rowVectorToIOBuf(
            remoteRowVector, rows.end(), *context.pool(), serde_.get()));

    std::unique_ptr<folly::IOBuf> responseBody = restClient_->invokeFunction(
        metadata_.location, std::move(requestBody), metadata_.serdeFormat);

    auto outputRowVector = IOBufToRowVector(
        *responseBody, ROW({outputType}), *context.pool(), serde_.get());

    result = outputRowVector->childAt(0);
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Error while executing remote function '{}': {}",
        functionName_,
        e.what());
  }
}

} // namespace facebook::presto::functions
