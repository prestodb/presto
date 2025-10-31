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

#include "presto_cpp/main/functions/remote/PrestoToRestFunctionExpr.h"

#include <boost/url/encode.hpp>
#include <boost/url/rfc/unreserved_chars.hpp>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/functions/remote/RestRemoteFunction.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"

using facebook::velox::functions::remote::PageFormat;

namespace facebook::presto::functions::remote::rest {
namespace {
// Returns the serialization/deserialization format used by the remote function
// server. The format is determined by the system configuration value
// "remoteFunctionServerSerde". Supported formats:
//   - "presto_page": Uses Presto page format.
//   - "spark_unsafe_row": Uses Spark unsafe row format.
// @return PageFormat enum value corresponding to the configured serde format.
// @throws VeloxException if the configured format is unknown.
PageFormat getSerdeFormat() {
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

// Encodes a string for safe inclusion in a URL by escaping non-alphanumeric
// characters using percent-encoding. Alphanumeric characters and '-', '_', '.',
// '~' are left unchanged. All other characters are replaced with '%' followed
// by their two-digit hexadecimal value.
// @param value The input string to encode.
// @return The URL-encoded string.
std::string urlEncode(const std::string& value) {
  return boost::urls::encode(value, boost::urls::unreserved_chars);
}

std::string getFunctionName(const protocol::SqlFunctionId& functionId) {
  // Example: "namespace.schema.function;TYPE;TYPE".
  const auto nameEnd = functionId.find(';');
  // Assuming the possibility of missing ';' if there are no function arguments.
  return nameEnd != std::string::npos ? functionId.substr(0, nameEnd)
                                      : functionId;
}

// Constructs a Velox function signature from a Presto function signature. This
// function translates type variable constraints, integer variable constraints,
// return type, argument types, and variable arity from the Presto signature to
// the corresponding Velox signature builder.
// @param prestoSignature The Presto function signature to convert.
// @return A pointer to the constructed Velox function signature.
velox::exec::FunctionSignaturePtr buildVeloxSignatureFromPrestoSignature(
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

} // namespace

void registerRestRemoteFunction(
    const protocol::RestFunctionHandle& restFunctionHandle) {
  static std::mutex registrationMutex;
  static std::unordered_map<std::string, std::string> registeredFunctionHandles;
  static std::unordered_map<std::string, functions::rest::RestRemoteClientPtr>
      restClient;
  static const std::string remoteFunctionServerRestURL =
      SystemConfig::instance()->remoteFunctionServerRestURL();

  const std::string functionId = restFunctionHandle.functionId;

  json functionHandleJson;
  to_json(functionHandleJson, restFunctionHandle);
  functionHandleJson["url"] = remoteFunctionServerRestURL;
  const std::string serializedFunctionHandle = functionHandleJson.dump();

  // Check if already registered (read-only, no lock needed for initial check)
  {
    std::lock_guard<std::mutex> lock(registrationMutex);
    auto it = registeredFunctionHandles.find(functionId);
    if (it != registeredFunctionHandles.end() &&
        it->second == serializedFunctionHandle) {
      return;
    }
  }

  // Get or create shared RestRemoteClient for this server URL
  functions::rest::RestRemoteClientPtr remoteClient;
  {
    std::lock_guard<std::mutex> lock(registrationMutex);
    auto clientIt = restClient.find(remoteFunctionServerRestURL);
    if (clientIt == restClient.end()) {
      restClient[remoteFunctionServerRestURL] =
          std::make_shared<functions::rest::RestRemoteClient>(
              remoteFunctionServerRestURL);
    }
    remoteClient = restClient[remoteFunctionServerRestURL];
  }

  functions::rest::VeloxRemoteFunctionMetadata metadata;

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

  functions::rest::registerVeloxRemoteFunction(
      getFunctionName(restFunctionHandle.functionId),
      veloxSignatures,
      metadata,
      remoteClient);

  // Update registration map
  {
    std::lock_guard<std::mutex> lock(registrationMutex);
    registeredFunctionHandles[functionId] = serializedFunctionHandle;
  }
}
} // namespace facebook::presto::functions::remote::rest
