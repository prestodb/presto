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

#include "presto_cpp/main/functions/remote/RestRemoteFunction.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"
#include "velox/functions/remote/client/RemoteVectorFunction.h"

using namespace facebook::velox;
namespace facebook::presto::functions::remote::rest {
namespace {

class RestRemoteFunction : public velox::functions::RemoteVectorFunction {
 public:
  RestRemoteFunction(
      const std::string& functionName,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      const VeloxRemoteFunctionMetadata& metadata,
      RestRemoteClientPtr restClient)
      : RemoteVectorFunction(functionName, inputArgs, metadata),
        location_(metadata.location),
        serdeFormat_(metadata.serdeFormat),
        restClient_(std::move(restClient)) {}

 protected:
  std::unique_ptr<velox::functions::remote::RemoteFunctionResponse>
  invokeRemoteFunction(
      const velox::functions::remote::RemoteFunctionRequest& request)
      const override {
    VELOX_CHECK(restClient_, "Remote client not initialized.");

    // Clone the request payload for the REST call
    auto requestBody = request.inputs()->payload()->clone();

    auto responseBody = restClient_->invokeFunction(
        location_, serdeFormat_, std::move(requestBody));

    if (!responseBody) {
      VELOX_FAIL("No response received from remote function invocation.");
    }

    // Convert REST response to RemoteFunctionResponse
    auto response =
        std::make_unique<velox::functions::remote::RemoteFunctionResponse>();
    velox::functions::remote::RemoteFunctionPage result;
    result.payload_ref() = std::move(*responseBody);
    response->result_ref() = std::move(result);
    return response;
  }

  std::string remoteLocationToString() const override {
    return location_;
  }

 private:
  const std::string location_;
  const velox::functions::remote::PageFormat serdeFormat_;
  const RestRemoteClientPtr restClient_;
};

std::shared_ptr<exec::VectorFunction> createRestRemoteFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/,
    const VeloxRemoteFunctionMetadata& metadata,
    RestRemoteClientPtr restClient) {
  return std::make_shared<RestRemoteFunction>(
      name, inputArgs, metadata, restClient);
}

} // namespace

void registerVeloxRemoteFunction(
    const std::string& name,
    const std::vector<exec::FunctionSignaturePtr>& signatures,
    VeloxRemoteFunctionMetadata metadata,
    RestRemoteClientPtr restClient,
    bool overwrite) {
  registerStatefulVectorFunction(
      name,
      signatures,
      std::bind(
          createRestRemoteFunction,
          std::placeholders::_1,
          std::placeholders::_2,
          std::placeholders::_3,
          metadata,
          restClient),
      std::move(metadata),
      overwrite);
}

} // namespace facebook::presto::functions::remote::rest
