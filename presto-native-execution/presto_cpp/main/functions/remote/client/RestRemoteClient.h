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

#include "presto_cpp/main/functions/remote/client/RemoteClient.h"
#include "presto_cpp/main/functions/remote/client/RestClient.h"

using namespace facebook::velox;
namespace facebook::presto::functions {
class RestRemoteClient : public RemoteClient {
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
      VectorPtr& result) const override;

 private:
  std::unique_ptr<RestClient> restClient_;
  std::string url_;
};
} // namespace facebook::presto::functions
