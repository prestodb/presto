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

#include "presto_cpp/main/functions/remote/PrestoToVeloxRemoteFunctionExpr.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"
#include "presto_cpp/main/functions/remote/client/VeloxRemoteFunction.h"

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
PageFormat getSerde() {
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

// Extracts the schema name from a fully qualified Presto function identifier.
// The function identifier is expected in the format
// "namespace.schema.function;TYPE;TYPE". This function returns the substring
// between the first and second dots in the function name.If the schema cannot
// be determined, "default" is returned.
// @param functionId The fully qualified Presto function identifier.
// @return The extracted schema name or "default".
std::string getSchemaName(const protocol::SqlFunctionId& functionId) {
  // Example: "json.x4.eq;INTEGER;INTEGER".
  const auto nameEnd = functionId.find(';');
  std::string functionName = (nameEnd != std::string::npos)
      ? functionId.substr(0, nameEnd)
      : functionId;

  const auto firstDot = functionName.find('.');
  const auto secondDot = functionName.find('.', firstDot + 1);
  if (firstDot != std::string::npos && secondDot != std::string::npos) {
    return functionName.substr(firstDot + 1, secondDot - firstDot - 1);
  }

  return "default";
}

// Extracts the function name from a fully qualified function identifier string.
// The input is expected to be in the format "namespace.schema.function", and
// this function returns the substring after the last dot. If there is no dot,
// the entire input string is returned.
// @param input The fully qualified function identifier.
// @return The extracted function name.
std::string extractFunctionName(const std::string& input) {
  size_t lastDot = input.find_last_of('.');
  if (lastDot != std::string::npos) {
    return input.substr(lastDot + 1);
  }
  return input;
}

// Encodes a string for safe inclusion in a URL by escaping non-alphanumeric
// characters using percent-encoding. Alphanumeric characters and '-', '_', '.',
// '~' are left unchanged. All other characters are replaced with '%' followed
// by their two-digit hexadecimal value.
// @param value The input string to encode.
// @return The URL-encoded string.
std::string urlEncode(const std::string& value) {
  std::ostringstream escaped;
  escaped.fill('0');
  escaped << std::hex;
  for (char c : value) {
    if (isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' ||
        c == '.' || c == '~') {
      escaped << c;
    } else {
      escaped << '%' << std::setw(2) << int(static_cast<unsigned char>(c));
    }
  }
  return escaped.str();
}
std::string getFunctionName(const protocol::SqlFunctionId& functionId) {
  // Example: "json.x4.eq;INTEGER;INTEGER".
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
  static std::unordered_map<std::string, std::string> registeredFunctionHandles;
  static std::unordered_map<std::string, functions::rest::RestRemoteClientPtr>
      remoteClients;
  static const std::string remoteFunctionServerRestURL =
      SystemConfig::instance()->remoteFunctionServerRestURL();
  const std::string functionId = restFunctionHandle.functionId;

  json functionHandleJson;
  to_json(functionHandleJson, restFunctionHandle);
  functionHandleJson["url"] = remoteFunctionServerRestURL;
  const std::string serializedFunctionHandle = functionHandleJson.dump();

  auto it = registeredFunctionHandles.find(functionId);
  if (it != registeredFunctionHandles.end() &&
      it->second == serializedFunctionHandle) {
    return;
  }

  // Get or create shared RestRemoteClient for this server URL
  auto clientIt = remoteClients.find(remoteFunctionServerRestURL);
  if (clientIt == remoteClients.end()) {
    remoteClients[remoteFunctionServerRestURL] =
        std::make_shared<functions::rest::RestRemoteClient>(
            remoteFunctionServerRestURL);
  }
  auto remoteClient = remoteClients[remoteFunctionServerRestURL];

  functions::rest::VeloxRemoteFunctionMetadata metadata;

  const std::string functionLocation = fmt::format(
      "{}/v1/functions/{}/{}/{}/{}",
      remoteFunctionServerRestURL,
      getSchemaName(restFunctionHandle.functionId),
      extractFunctionName(getFunctionName(restFunctionHandle.functionId)),
      urlEncode(restFunctionHandle.functionId),
      restFunctionHandle.version);
  metadata.location = functionLocation;
  metadata.serdeFormat = getSerde();

  auto veloxSignature =
      buildVeloxSignatureFromPrestoSignature(restFunctionHandle.signature);
  std::vector<velox::exec::FunctionSignaturePtr> veloxSignatures = {
      veloxSignature};

  functions::rest::registerVeloxRemoteFunction(
      getFunctionName(restFunctionHandle.functionId),
      veloxSignatures,
      metadata,
      remoteClient);

  registeredFunctionHandles[functionId] = serializedFunctionHandle;
}
} // namespace facebook::presto::functions::remote::rest
