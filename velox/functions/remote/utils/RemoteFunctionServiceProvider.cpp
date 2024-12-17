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

#include "velox/functions/remote/utils/RemoteFunctionServiceProvider.h"

#include "thrift/lib/cpp2/server/ThriftServer.h"
#include "velox/functions/remote/server/RemoteFunctionService.h"

namespace facebook::velox::functions {

RemoteFunctionServiceParams
RemoteFunctionServiceProviderForLocalThrift::getRemoteFunctionServiceParams() {
  folly::call_once(initializeServiceFlag_, [&]() { initializeServer(); });
  return RemoteFunctionServiceParams{
      remotePrefix_,
      location_,
  };
}

void RemoteFunctionServiceProviderForLocalThrift::initializeServer() {
  auto handler =
      std::make_shared<velox::functions::RemoteFunctionServiceHandler>(
          remotePrefix_);
  server_ = std::make_shared<apache::thrift::ThriftServer>();
  server_->setInterface(handler);
  server_->setAddress(location_);

  thread_ = std::make_unique<std::thread>([&] { server_->serve(); });
  VELOX_CHECK(waitForRunning(), "Unable to initialize thrift server.");
  LOG(INFO) << "Thrift server is up and running in local port " << location_;
}

bool RemoteFunctionServiceProviderForLocalThrift::waitForRunning() {
  for (size_t i = 0; i < 100; ++i) {
    if (server_->getServerStatus() ==
        apache::thrift::ThriftServer::ServerStatus::RUNNING) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  return false;
}

RemoteFunctionServiceProviderForLocalThrift::
    ~RemoteFunctionServiceProviderForLocalThrift() {
  server_->stop();
  thread_->join();
  LOG(INFO) << "Thrift server stopped.";
}

RemoteFunctionServiceParams startLocalThriftServiceAndGetParams() {
  static folly::Singleton<IRemoteFunctionServiceProvider>
      remoteFunctionServiceProviderForLocalThriftSingleton{
          []() { return new RemoteFunctionServiceProviderForLocalThrift(); }};
  auto provider =
      remoteFunctionServiceProviderForLocalThriftSingleton.try_get();
  VELOX_CHECK(provider, "local remoteFunctionProvider is not available");
  return provider->getRemoteFunctionServiceParams();
}

} // namespace facebook::velox::functions
