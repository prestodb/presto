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

#include "presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestHandler.h"

namespace facebook::presto::functions::remote::rest::test {

class RemoteDoubleDivHandler : public RemoteFunctionRestHandler {
 public:
  RemoteDoubleDivHandler() = default;

  velox::RowTypePtr getInputTypes() const override {
    return velox::ROW({"c0", "c1"}, {velox::DOUBLE(), velox::DOUBLE()});
  }

  velox::TypePtr getOutputType() const override {
    return velox::DOUBLE();
  }

 protected:
  void compute(
      const velox::RowVectorPtr& inputVector,
      const velox::VectorPtr& resultVector,
      std::string& /*errorMessage*/) override {
    auto numerator = inputVector->childAt(0)->asFlatVector<double>();
    auto denominator = inputVector->childAt(1)->asFlatVector<double>();
    auto outFlat = resultVector->asFlatVector<double>();

    const auto numRows = inputVector->size();
    for (velox::vector_size_t i = 0; i < numRows; ++i) {
      // If either input is null, output is null.
      if (numerator->isNullAt(i) || denominator->isNullAt(i)) {
        outFlat->setNull(i, true);
      } else {
        double numVal = numerator->valueAt(i);
        double denVal = denominator->valueAt(i);
        // If denominator is zero, produce a null.
        if (denVal == 0.0) {
          outFlat->setNull(i, true);
        } else {
          outFlat->set(i, numVal / denVal);
        }
      }
    }
  }
};

} // namespace facebook::presto::functions::remote::rest::test
