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

#pragma once

#include <folly/io/IOBuf.h>
#include <string>

#include "velox/functions/remote/if/gen-cpp2/RemoteFunction_types.h"

using namespace facebook::velox;
namespace facebook::presto::functions {
/// This class uses a HTTP client library - cpr to:
///  - Send a POST request to the specified @p url.
///  - Attach serialization format information as HTTP headers based on
///    @p serdeFormat (e.g. `Content-Type: application/presto+<format>`).
///  - Return the response payload as a folly::IOBuf.
class RestClient {
 public:
  /// @brief Invokes a function using a HTTP POST request.
  /// Constructs and sends a HTTP POST request to the provided @p url with
  /// @p requestPayload in the wire format specified by @p serdeFormat.
  /// The chosen format is also indicated in the request headers for the server
  /// to parse correctly. The server's response is expected in the same format
  /// and is returned in a folly::IOBuf.
  ///
  /// @param url The endpoint to which the request should be sent.
  /// @param requestPayload A pointer to the serialized request body.
  /// @param serdeFormat The wire format for serialization/deserialization,
  ///        which is conveyed through HTTP headers.
  ///
  /// @return A unique_ptr to a folly::IOBuf containing the server's serialized
  /// response.
  ///
  /// @throws VeloxException if there is an error initializing or making
  /// the request, if the server returns an error status, or if the response
  /// cannot be parsed.
  std::unique_ptr<folly::IOBuf> invokeFunction(
      const std::string& url,
      std::unique_ptr<folly::IOBuf> requestPayload,
      velox::functions::remote::PageFormat serdeFormat);
};

/// @brief Factory function to create an instance of RestClient.
/// @return A unique pointer to an RestClient implementation.
std::unique_ptr<RestClient> getRestClient();

} // namespace facebook::presto::functions
