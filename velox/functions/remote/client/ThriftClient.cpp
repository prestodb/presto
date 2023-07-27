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

#include "velox/functions/remote/client/ThriftClient.h"
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

namespace facebook::velox::functions {

std::unique_ptr<RemoteFunctionClient> getThriftClient(
    folly::SocketAddress location,
    folly::EventBase* eventBase) {
  auto sock = folly::AsyncSocket::newSocket(eventBase, location);
  return std::make_unique<RemoteFunctionClient>(
      apache::thrift::HeaderClientChannel::newChannel(std::move(sock)));
}

} // namespace facebook::velox::functions
