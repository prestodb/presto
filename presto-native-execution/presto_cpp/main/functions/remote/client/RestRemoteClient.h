/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#pragma once

#include <folly/io/async/ScopedEventBaseThread.h>

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpClient.h"

#include "presto_cpp/main/functions/remote/client/Remote.h"
#include "velox/functions/remote/client/Remote.h"
#include "velox/functions/remote/if/gen-cpp2/RemoteFunctionServiceAsyncClient.h"
#include "velox/vector/VectorStream.h"

using namespace facebook::velox;

namespace facebook::presto::functions {

class RestRemoteClient {
 public:
  RestRemoteClient(
      const std::string& url,
      const std::string& functionName,
      RowTypePtr remoteInputType,
      std::vector<std::string> serializedInputTypes,
      const PrestoRemoteFunctionsMetadata& metadata);

  void applyRemote(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const;

 private:
  std::string functionName_;
  RowTypePtr remoteInputType_;
  std::vector<std::string> serializedInputTypes_;
  velox::functions::remote::PageFormat serdeFormat_;
  PrestoRemoteFunctionsMetadata metadata_;
  std::unique_ptr<VectorSerde> serde_;
  std::string url_;

  const std::chrono::milliseconds requestTimeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeRequestTimeoutMs());

  const std::chrono::milliseconds connectTimeoutMs =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          SystemConfig::instance()->exchangeConnectTimeoutMs());

  std::unique_ptr<folly::IOBuf> invokeFunction(
      const std::string& fullUrl,
      std::unique_ptr<folly::IOBuf> requestPayload,
      velox::functions::remote::PageFormat serdeFormat) const;
};

} // namespace facebook::presto::functions
