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

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "presto_cpp/main/functions/remote/RestRemoteFunction.h"
#include "presto_cpp/main/functions/remote/client/RestRemoteClient.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/expression/FunctionSignature.h"

namespace facebook::presto::functions::remote::rest {

/// Manages registration of REST-based remote functions in Velox.
/// This class provides a thread-safe singleton interface for registering
/// remote functions that are accessed via REST API endpoints.
class PrestoRestFunctionRegistration {
 public:
  /// Returns the singleton instance of the registration manager.
  /// @return Reference to the singleton instance.
  static PrestoRestFunctionRegistration& getInstance();

  /// Registers a REST remote function with Velox.
  /// This method is thread-safe and handles duplicate registrations.
  /// @param restFunctionHandle The Presto REST function handle containing
  ///        function metadata, signature, and location information.
  void registerFunction(const protocol::RestFunctionHandle& restFunctionHandle);

  // Delete copy constructor and assignment operator
  PrestoRestFunctionRegistration(const PrestoRestFunctionRegistration&) =
      delete;
  PrestoRestFunctionRegistration& operator=(
      const PrestoRestFunctionRegistration&) = delete;

 private:
  // Private constructor for singleton pattern.
  PrestoRestFunctionRegistration();

  // Resolves the remote function server URL from the function handle.
  // @param restFunctionHandle The Presto REST function handle that may
  //        contain an execution endpoint.
  // @return The resolved remote function server URL.
  std::string getRemoteFunctionServerUrl(
      const protocol::RestFunctionHandle& restFunctionHandle) const;

  // Returns the serialization/deserialization format used by the remote
  // function server.
  // @return PageFormat enum value corresponding to the configured serde
  // format.
  static velox::functions::remote::PageFormat getSerdeFormat();

  // Encodes a string for safe inclusion in a URL.
  // @param value The input string to encode.
  // @return The URL-encoded string.
  static std::string urlEncode(const std::string& value);

  // Extracts the function name from a function ID.
  // @param functionId The SQL function ID.
  // @return The function name without type parameters.
  static std::string getFunctionName(const protocol::SqlFunctionId& functionId);

  // Constructs a Velox function signature from a Presto function signature.
  // @param prestoSignature The Presto function signature to convert.
  // @return A pointer to the constructed Velox function signature.
  static velox::exec::FunctionSignaturePtr
  buildVeloxSignatureFromPrestoSignature(
      const protocol::Signature& prestoSignature);

  // Mutex for thread-safe registration operations.
  std::mutex registrationMutex_;

  // Map of registered function IDs to their serialized handles.
  std::unordered_map<std::string, std::string> registeredFunctionHandles_;

  // Map of REST server URLs to their corresponding client instances.
  std::unordered_map<std::string, RestRemoteClientPtr> restClients_;

  // The base URL for the remote function server REST API.
  const std::string kRemoteFunctionServerRestURL_;

  VELOX_FRIEND_TEST(
      PrestoRestFunctionRegistrationTest,
      getRemoteFunctionServerUrlWithExecutionEndpoint);
  VELOX_FRIEND_TEST(
      PrestoRestFunctionRegistrationTest,
      getRemoteFunctionServerUrlWithEmptyExecutionEndpoint);
  VELOX_FRIEND_TEST(
      PrestoRestFunctionRegistrationTest,
      getRemoteFunctionServerUrlWithoutExecutionEndpoint);
  VELOX_FRIEND_TEST(
      PrestoRestFunctionRegistrationTest,
      getRemoteFunctionServerUrlConsistency);
  VELOX_FRIEND_TEST(
      PrestoRestFunctionRegistrationTest,
      getRemoteFunctionServerUrlWithDifferentProtocols);
  VELOX_FRIEND_TEST(
      PrestoRestFunctionRegistrationTest,
      getRemoteFunctionServerUrlWithComplexUrls);
};
} // namespace facebook::presto::functions::remote::rest
