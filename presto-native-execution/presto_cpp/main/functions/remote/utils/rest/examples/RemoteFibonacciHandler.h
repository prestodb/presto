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

#include <boost/math/special_functions/fibonacci.hpp>
#include "presto_cpp/main/functions/remote/utils/rest/RemoteFunctionRestHandler.h"

using namespace facebook::velox;
namespace facebook::presto::functions {

class RemoteFibonacciHandler : public RemoteFunctionRestHandler {
 public:
  RemoteFibonacciHandler(RowTypePtr inputTypes, TypePtr outputType)
      : RemoteFunctionRestHandler(
            std::move(inputTypes),
            std::move(outputType)) {}

 protected:
  void compute(
      const RowVectorPtr& inputVector,
      const VectorPtr& resultVector,
      std::string& /*errorMessage*/) {
    auto numFlat = inputVector->childAt(0)->asFlatVector<int64_t>();
    auto outFlat = resultVector->asFlatVector<int64_t>();

    const auto numRows = inputVector->size();
    for (vector_size_t i = 0; i < numRows; ++i) {
      if (numFlat->isNullAt(i)) {
        outFlat->setNull(i, true);
      } else {
        int64_t num = numFlat->valueAt(i);
        outFlat->set(i, boost::math::fibonacci<long long>(num));
      }
    }
  }
};

} // namespace facebook::presto::functions
