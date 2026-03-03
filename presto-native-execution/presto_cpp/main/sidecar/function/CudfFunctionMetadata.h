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
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"

namespace facebook::presto::cudf {

class CudfFunctionMetadataProvider
    : public facebook::presto::BaseFunctionMetadataProvider {
 public:
    CudfFunctionMetadataProvider() {
        scalarFunctions_ = facebook::velox::cudf_velox::getCudfFunctionSignatureMap();
        aggregateFunctions_ =
            facebook::velox::cudf_velox::getCudfAggregationFunctionSignatureMap();
    }

    const velox::FunctionSignatureMap& scalarFunctions() const override;

    const velox::exec::AggregateFunctionSignatureMap& aggregateFunctions() const override;

    const velox::exec::WindowFunctionMap& windowFunctions() const override;

    bool isInternalScalarFunction(const std::string& name) const override;

    std::optional<velox::exec::VectorFunctionMetadata> getVeloxFunctionMetadata(
        const std::string& name) const override;

    const AggregationFunctionMetadataPtr getAggregationFunctionMetadata(
        const std::string& name,
        const facebook::velox::exec::AggregateFunctionSignature* signature)
        const override;

 private:
    velox::FunctionSignatureMap scalarFunctions_;
    velox::exec::AggregateFunctionSignatureMap aggregateFunctions_;
};

// Returns a shared static provider instance for CUDF function metadata.
const facebook::presto::cudf::CudfFunctionMetadataProvider&
cudfFunctionMetadataProvider();

// Returns metadata for all registered CUDF functions as json.
// When PRESTO_ENABLE_CUDF is enabled, this returns CUDF function metadata.
nlohmann::json getFunctionsMetadata(
    const std::optional<std::string>& catalog = std::nullopt);

} // namespace facebook::presto::cudf
