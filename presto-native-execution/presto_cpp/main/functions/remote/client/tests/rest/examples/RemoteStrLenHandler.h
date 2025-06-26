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

#include "presto_cpp/main/functions/remote/client/tests/rest/RemoteFunctionRestHandler.h"
#include "velox/type/fbhive/HiveTypeParser.h"

using namespace facebook::velox;
namespace facebook::presto::functions {
class RemoteStrLenHandler : public RemoteFunctionRestHandler {
 public:
  RemoteStrLenHandler(RowTypePtr inputTypes, TypePtr outputType)
      : RemoteFunctionRestHandler(
            std::move(inputTypes),
            std::move(outputType)) {}

 protected:
  void compute(
      const RowVectorPtr& inputVector,
      const VectorPtr& resultVector,
      std::string& errorMessage) {
    auto inputFlat = inputVector->childAt(0)->asFlatVector<StringView>();
    auto outFlat = resultVector->asFlatVector<int32_t>();
    const auto numRows = inputVector->size();

    for (vector_size_t i = 0; i < numRows; ++i) {
      if (inputFlat->isNullAt(i)) {
        outFlat->setNull(i, true);
      } else {
        int32_t stringLen = inputFlat->valueAt(i).size();
        outFlat->set(i, stringLen);
      }
    }
  }
};

} // namespace facebook::presto::functions
