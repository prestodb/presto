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

namespace facebook::velox::functions {

struct RemoteVectorFunctionMetadata : public exec::VectorFunctionMetadata {
  /// Network address of the servr to communicate with. Note that this can hold
  /// a network location (ip/port pair) or a unix domain socket path (see
  /// SocketAddress::makeFromPath()).
  folly::SocketAddress location;

  /// The serialization format to be used
  remote::PageFormat serdeFormat{remote::PageFormat::PRESTO_PAGE};
};

/// Registers a new remote function. It will use the meatadata defined in
/// `RemoteVectorFunctionMetadata` to control the serialization format and
/// remote server address.
//
/// Remote functions are registered as regular statufull functions (using the
/// same internal catalog), and hence conflict if there already exists a
/// (non-remote) function registered with the same name. The `overwrite` flag
/// controls whether to overwrite in these cases.
void registerRemoteFunction(
    const std::string& name,
    std::vector<exec::FunctionSignaturePtr> signatures,
    const RemoteVectorFunctionMetadata& metadata = {},
    bool overwrite = true);

} // namespace facebook::velox::functions
