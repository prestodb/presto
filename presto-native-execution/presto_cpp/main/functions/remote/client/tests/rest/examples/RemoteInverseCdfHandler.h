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

#include <boost/math/distributions/chi_squared.hpp>
#include "presto_cpp/main/functions/remote/client/tests/rest/RemoteFunctionRestHandler.h"

namespace facebook::presto::functions {
namespace {
inline double inverse_chi_squared_cdf(double p, double nu) {
  if (p <= 0.0 || p >= 1.0) {
    throw std::domain_error("inverse_chi_squared_cdf: p must be in (0,1)");
  }
  if (nu <= 0.0) {
    throw std::domain_error(
        "inverse_chi_squared_cdf: degrees of freedom must be > 0");
  }

  const boost::math::chi_squared_distribution<double> chi2(nu);
  double result = boost::math::quantile(chi2, p);
  return std::round(result * 100.0) / 100.0;
}
} // namespace

class RemoteInverseCdfHandler : public RemoteFunctionRestHandler {
 public:
  RemoteInverseCdfHandler(
      velox::RowTypePtr inputTypes,
      velox::TypePtr outputType)
      : RemoteFunctionRestHandler(
            std::move(inputTypes),
            std::move(outputType)) {}

 protected:
  void compute(
      const velox::RowVectorPtr& inputVector,
      const velox::VectorPtr& resultVector,
      std::string& errorMessage) {
    auto p = inputVector->childAt(0)->asFlatVector<double>();
    auto nu = inputVector->childAt(1)->asFlatVector<double>();
    auto outFlat = resultVector->asFlatVector<double>();

    const auto numRows = inputVector->size();
    for (velox::vector_size_t i = 0; i < numRows; ++i) {
      // If either input is null, output is null.
      if (p->isNullAt(i) || nu->isNullAt(i)) {
        outFlat->setNull(i, true);
      } else {
        try {
          double pVal = p->valueAt(i);
          double nuVal = nu->valueAt(i);
          outFlat->set(i, inverse_chi_squared_cdf(pVal, nuVal));
        } catch (const std::domain_error& ex) {
          errorMessage = ex.what();
        }
      }
    }
  }
};

} // namespace facebook::presto::functions
