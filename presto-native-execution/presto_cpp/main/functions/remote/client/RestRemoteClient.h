/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#pragma once

#include <folly/io/async/ScopedEventBaseThread.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpClient.h"
#include "velox/functions/remote/if/gen-cpp2/RemoteFunctionServiceAsyncClient.h"

namespace facebook::presto::functions::rest {

class RestRemoteClient {
 public:
  RestRemoteClient(const std::string& url);

  ~RestRemoteClient();

  std::unique_ptr<folly::IOBuf> invokeFunction(
      const std::string& fullUrl,
      velox::functions::remote::PageFormat serdeFormat,
      std::unique_ptr<folly::IOBuf> requestPayload) const;

 private:
  const std::string url_;
  std::unique_ptr<folly::ScopedEventBaseThread> evbThread_;
  std::shared_ptr<http::HttpClient> httpClient_;
  std::shared_ptr<velox::memory::MemoryPool> memPool_;

  const std::chrono::milliseconds requestTimeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeRequestTimeoutMs());

  const std::chrono::milliseconds connectTimeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeConnectTimeoutMs());
};

using RestRemoteClientPtr = std::shared_ptr<RestRemoteClient>;

} // namespace facebook::presto::functions::rest
