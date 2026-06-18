/*
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
#include "presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestHandler.h"

namespace facebook::presto::functions::remote::rest::test {

class RemoteFibonacciHandler : public RemoteFunctionRestHandler {
 public:
  RemoteFibonacciHandler() = default;

  velox::RowTypePtr getInputTypes() const override {
    return velox::ROW({"c0"}, {velox::BIGINT()});
  }

  velox::TypePtr getOutputType() const override {
    return velox::BIGINT();
  }

 protected:
  void compute(
      const velox::RowVectorPtr& inputVector,
      const velox::VectorPtr& resultVector,
      std::string& /*errorMessage*/) override {
    auto numFlat = inputVector->childAt(0)->asFlatVector<int64_t>();
    auto outFlat = resultVector->asFlatVector<int64_t>();

    const auto numRows = inputVector->size();
    for (velox::vector_size_t i = 0; i < numRows; ++i) {
      if (numFlat->isNullAt(i)) {
        outFlat->setNull(i, true);
      } else {
        int64_t num = numFlat->valueAt(i);
        outFlat->set(i, boost::math::fibonacci<long long>(num));
      }
    }
  }
};

} // namespace facebook::presto::functions::remote::rest::test
