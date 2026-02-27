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

#include <optional>

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/sidecar/function/BaseFunctionMetadataProvider.h"

namespace facebook::presto {

class NativeFunctionMetadata : public BaseFunctionMetadataProvider {
 public:
  NativeFunctionMetadata()
      : scalarFunctions_{velox::getFunctionSignatures()},
        aggregateFunctions_{facebook::velox::exec::getAggregateFunctionSignatures()},
        windowFunctions_{facebook::velox::exec::windowFunctions()} {}

  const velox::FunctionSignatureMap& scalarFunctions() const override;

  std::optional<velox::exec::VectorFunctionMetadata> getVeloxFunctionMetadata(
      const std::string& name) const override;

  bool isInternalScalarFunction(const std::string& name) const override;

  const AggregationFunctionMetadataPtr getAggregationFunctionMetadata(
      const std::string& name,
      const facebook::velox::exec::AggregateFunctionSignature* signature)
      const override;

  const velox::exec::AggregateFunctionSignatureMap& aggregateFunctions() const override;

  const velox::exec::WindowFunctionMap& windowFunctions() const override;

 private:
  velox::FunctionSignatureMap scalarFunctions_;
  velox::exec::AggregateFunctionSignatureMap aggregateFunctions_;
  velox::exec::WindowFunctionMap windowFunctions_;
  // Local copy of aggregate function entries to maintain pointer validity
  AggregateFunctionMap aggregateFunctionEntries_;
};

const NativeFunctionMetadata& nativeFunctionMetadata();
nlohmann::json getFunctionsMetadata(
    const std::optional<std::string>& catalog = std::nullopt);

} // namespace facebook::presto
