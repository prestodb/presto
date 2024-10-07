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

#include <folly/SocketAddress.h>
#include "velox/expression/VectorFunction.h"
#include "velox/functions/remote/if/gen-cpp2/RemoteFunction_types.h"

using namespace facebook::velox;
namespace facebook::presto::functions {
struct PrestoRemoteFunctionsMetadata : public exec::VectorFunctionMetadata {
  /// URL of the HTTP/REST server for remote function.
  std::string location;

  /// The serialization format to be used when sending data to the remote.
  velox::functions::remote::PageFormat serdeFormat{
      velox::functions::remote::PageFormat::PRESTO_PAGE};
};

void registerPrestoRemoteFunction(
    const std::string& name,
    std::vector<exec::FunctionSignaturePtr> signatures,
    const PrestoRemoteFunctionsMetadata& metadata = {},
    bool overwrite = true);
;

} // namespace facebook::presto::functions
