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

#include <string>
#include "presto_cpp/main/functions/remote/utils/rest/RemoteFunctionRestHandler.h"

using namespace facebook::velox;
namespace facebook::presto::functions {

class RemoteRemoveCharHandler final : public RemoteFunctionRestHandler {
 public:
  RemoteRemoveCharHandler(RowTypePtr inputTypes, TypePtr outputType)
      : RemoteFunctionRestHandler(
            std::move(inputTypes),
            std::move(outputType)) {}

 protected:
  void compute(
      const RowVectorPtr& inputVector,
      const VectorPtr& resultVector,
      std::string& /*errorMessage*/) override {
    auto inputFlat = inputVector->childAt(0)->asFlatVector<StringView>();
    auto removeFlat = inputVector->childAt(1)->asFlatVector<StringView>();
    auto outFlat = resultVector->asFlatVector<StringView>();

    const auto numRows = inputVector->size();

    for (vector_size_t i = 0; i < numRows; ++i) {
      if (inputFlat->isNullAt(i) || removeFlat->isNullAt(i)) {
        outFlat->setNull(i, true);
        continue;
      }
      std::string src(
          inputFlat->valueAt(i).data(), inputFlat->valueAt(i).size());
      const auto removeView = removeFlat->valueAt(i);
      if (removeView.empty()) {
        outFlat->set(i, StringView(src));
        continue;
      }
      const char ch = removeView.data()[0];
      src.erase(std::remove(src.begin(), src.end(), ch), src.end());
      outFlat->set(i, StringView(src));
    }
  }
};

} // namespace facebook::presto::functions
