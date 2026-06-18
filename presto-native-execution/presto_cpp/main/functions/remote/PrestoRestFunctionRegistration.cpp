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

#include "presto_cpp/main/functions/remote/PrestoRestFunctionRegistration.h"

#include <boost/url/encode.hpp>
#include <boost/url/rfc/unreserved_chars.hpp>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/functions/remote/RestRemoteFunction.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"

using facebook::velox::functions::remote::PageFormat;

namespace facebook::presto::functions::remote::rest {

PrestoRestFunctionRegistration::PrestoRestFunctionRegistration()
    : kRemoteFunctionServerRestURL_(
          SystemConfig::instance()->remoteFunctionServerRestURL()) {}

PrestoRestFunctionRegistration& PrestoRestFunctionRegistration::getInstance() {
  static PrestoRestFunctionRegistration instance;
  return instance;
}

PageFormat PrestoRestFunctionRegistration::getSerdeFormat() {
  static const auto serdeFormat =
      SystemConfig::instance()->remoteFunctionServerSerde();
  if (serdeFormat == "presto_page") {
    return PageFormat::PRESTO_PAGE;
  } else if (serdeFormat == "spark_unsafe_row") {
    return PageFormat::SPARK_UNSAFE_ROW;
  } else {
    VELOX_FAIL(
        "Unknown serde name for remote function server: '{}'", serdeFormat);
  }
}

std::string PrestoRestFunctionRegistration::urlEncode(
    const std::string& value) {
  return boost::urls::encode(value, boost::urls::unreserved_chars);
}

std::string PrestoRestFunctionRegistration::getFunctionName(
    const protocol::SqlFunctionId& functionId) {
  // Example: "namespace.schema.function;TYPE;TYPE".
  const auto nameEnd = functionId.find(';');
  // Assuming the possibility of missing ';' if there are no function arguments.
  return nameEnd != std::string::npos ? functionId.substr(0, nameEnd)
                                      : functionId;
}

velox::exec::FunctionSignaturePtr
PrestoRestFunctionRegistration::buildVeloxSignatureFromPrestoSignature(
    const protocol::Signature& prestoSignature) {
  velox::exec::FunctionSignatureBuilder signatureBuilder;

  for (const auto& typeVar : prestoSignature.typeVariableConstraints) {
    signatureBuilder.typeVariable(typeVar.name);
  }

  for (const auto& longVar : prestoSignature.longVariableConstraints) {
    signatureBuilder.integerVariable(longVar.name);
  }
  signatureBuilder.returnType(prestoSignature.returnType);

  for (const auto& argType : prestoSignature.argumentTypes) {
    signatureBuilder.argumentType(argType);
  }

  if (prestoSignature.variableArity) {
    signatureBuilder.variableArity();
  }
  return signatureBuilder.build();
}

std::string PrestoRestFunctionRegistration::getRemoteFunctionServerUrl(
    const protocol::RestFunctionHandle& restFunctionHandle) const {
  if (restFunctionHandle.executionEndpoint &&
      !restFunctionHandle.executionEndpoint->empty()) {
    return *restFunctionHandle.executionEndpoint;
  }
  return kRemoteFunctionServerRestURL_;
}

void PrestoRestFunctionRegistration::registerFunction(
    const protocol::RestFunctionHandle& restFunctionHandle) {
  const std::string functionId = restFunctionHandle.functionId;

  const std::string remoteFunctionServerRestURL =
      getRemoteFunctionServerUrl(restFunctionHandle);
  json functionHandleJson;
  to_json(functionHandleJson, restFunctionHandle);
  functionHandleJson["url"] = remoteFunctionServerRestURL;
  const std::string serializedFunctionHandle = functionHandleJson.dump();

  {
    std::lock_guard<std::mutex> lock(registrationMutex_);
    auto it = registeredFunctionHandles_.find(functionId);
    if (it != registeredFunctionHandles_.end() &&
        it->second == serializedFunctionHandle) {
      return;
    }
  }

  // Get or create shared RestRemoteClient for this server URL
  RestRemoteClientPtr remoteClient;
  {
    std::lock_guard<std::mutex> lock(registrationMutex_);
    auto clientIt = restClients_.find(remoteFunctionServerRestURL);
    if (clientIt == restClients_.end()) {
      restClients_[remoteFunctionServerRestURL] =
          std::make_shared<RestRemoteClient>(remoteFunctionServerRestURL);
    }
    remoteClient = restClients_[remoteFunctionServerRestURL];
  }

  VeloxRemoteFunctionMetadata metadata;

  // Extract function name parts using the utility function
  const std::string functionName =
      getFunctionName(restFunctionHandle.functionId);
  const auto parts = util::getFunctionNameParts(functionName);
  const std::string schema = parts[1];
  const std::string function = parts[2];

  const std::string functionLocation = fmt::format(
      "{}/v1/functions/{}/{}/{}/{}",
      remoteFunctionServerRestURL,
      schema,
      function,
      urlEncode(restFunctionHandle.functionId),
      restFunctionHandle.version);
  metadata.location = functionLocation;
  metadata.serdeFormat = getSerdeFormat();

  auto veloxSignature =
      buildVeloxSignatureFromPrestoSignature(restFunctionHandle.signature);
  std::vector<velox::exec::FunctionSignaturePtr> veloxSignatures = {
      veloxSignature};

  registerVeloxRemoteFunction(
      getFunctionName(restFunctionHandle.functionId),
      veloxSignatures,
      metadata,
      remoteClient);

  // Update registration map
  {
    std::lock_guard<std::mutex> lock(registrationMutex_);
    registeredFunctionHandles_[functionId] = serializedFunctionHandle;
  }
}
} // namespace facebook::presto::functions::remote::rest
