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
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include "folly/String.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/sidecar/function/BaseFunctionMetadataProvider.h"
#include "presto_cpp/main/sidecar/function/CudfFunctionMetadata.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::presto::cudf {

const velox::FunctionSignatureMap&
CudfFunctionMetadataProvider::scalarFunctions() const {
  // scalarFunctions_ = facebook::velox::cudf_velox::getCudfFunctionSignatureMap();
  LOG(INFO) << "[CudfFunctionMetadata] scalarFunctions() returning "
            << scalarFunctions_.size() << " scalar functions";
  return scalarFunctions_;
}

const velox::exec::AggregateFunctionSignatureMap&
CudfFunctionMetadataProvider::aggregateFunctions() const {
  // aggregateFunctions_ =
  //     facebook::velox::cudf_velox::getCudfAggregationFunctionSignatureMap();
  LOG(INFO) << "[CudfFunctionMetadata] aggregateFunctions() returning "
            << aggregateFunctions_.size() << " aggregate functions";
  return aggregateFunctions_;
}

const velox::exec::WindowFunctionMap&
CudfFunctionMetadataProvider::windowFunctions() const {
  static const velox::exec::WindowFunctionMap kEmpty;
  return kEmpty;
}

bool CudfFunctionMetadataProvider::isInternalScalarFunction(
    const std::string& /*name*/) const {
  // CUDF sidecar currently exposes only externally visible functions.
  return false;
}

std::optional<velox::exec::VectorFunctionMetadata>
CudfFunctionMetadataProvider::getVeloxFunctionMetadata(
    const std::string& /*name*/) const {
  // Vector metadata is not available for CUDF sidecar functions.
  return std::nullopt;
}

const facebook::presto::AggregationFunctionMetadataPtr
CudfFunctionMetadataProvider::getAggregationFunctionMetadata(
    const std::string& /*name*/,
    const facebook::velox::exec::AggregateFunctionSignature* signature) const {
  auto metadata = std::make_shared<protocol::AggregationFunctionMetadata>();
  metadata->intermediateType =
      boost::algorithm::to_lower_copy(signature->intermediateType().toString());
  metadata->isOrderSensitive = false;
  return metadata;
}

CudfFunctionMetadataProvider& cudfFunctionMetadataProviderInternal() {
  static CudfFunctionMetadataProvider instance;
  return instance;
}

const facebook::presto::cudf::CudfFunctionMetadataProvider&
cudfFunctionMetadataProvider() {
  return cudfFunctionMetadataProviderInternal();
}

nlohmann::json getFunctionsMetadata(const std::optional<std::string>& catalog) {
  LOG(INFO) << "[CudfFunctionMetadata] getFunctionsMetadata called for catalog: "
            << (catalog.has_value() ? catalog.value() : "<all>");
  // if (!facebook::velox::cudf_velox::cudfIsRegistered()) {
    // LOG(INFO) << "[CudfFunctionMetadata] cuDF not registered yet, calling registerCudf()";
    // facebook::velox::cudf_velox::registerCudf();
  //   LOG(INFO) << "[CudfFunctionMetadata] registerCudf() completed";
  // } else {
  //   LOG(INFO) << "[CudfFunctionMetadata] cuDF already registered";
  // }
  auto result = cudfFunctionMetadataProviderInternal().getFunctionsMetadata(catalog);
  LOG(INFO) << "[CudfFunctionMetadata] getFunctionsMetadata returning JSON with "
            << result.size() << " entries";
  return result;
}

} // namespace facebook::presto::cudf
