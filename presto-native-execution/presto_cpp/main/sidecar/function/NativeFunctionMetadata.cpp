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

#include "presto_cpp/main/sidecar/function/NativeFunctionMetadata.h"

#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SimpleFunctionRegistry.h"

#include <unordered_set>

namespace facebook::presto {

namespace {

NativeFunctionMetadata& nativeFunctionMetadataInternal() {
  static NativeFunctionMetadata instance;
  return instance;
}

} // namespace

const velox::FunctionSignatureMap&
NativeFunctionMetadata::scalarFunctions() const {
  return scalarFunctions_;
}

const velox::exec::AggregateFunctionSignatureMap&
NativeFunctionMetadata::aggregateFunctions() const {
  return aggregateFunctions_;
}

const velox::exec::WindowFunctionMap&
NativeFunctionMetadata::windowFunctions() const {
  return windowFunctions_;
}

const NativeFunctionMetadata& nativeFunctionMetadata() {
  return nativeFunctionMetadataInternal();
}

nlohmann::json getFunctionsMetadata(const std::optional<std::string>& catalog) {
  return nativeFunctionMetadata().getFunctionsMetadata(catalog);
}

std::optional<velox::exec::VectorFunctionMetadata>
NativeFunctionMetadata::getVeloxFunctionMetadata(const std::string& name) const {
  auto simpleFunctionMetadata =
      facebook::velox::exec::simpleFunctions().getFunctionSignaturesAndMetadata(
          name);
  if (!simpleFunctionMetadata.empty()) {
    return simpleFunctionMetadata.back().first;
  }

  return facebook::velox::exec::getVectorFunctionMetadata(name);
}

bool NativeFunctionMetadata::isInternalScalarFunction(
    const std::string& name) const {
  static const std::unordered_set<std::string> kBlockList = {
      "row_constructor", "in", "is_null"};
  const auto metadata = getVeloxFunctionMetadata(name);
  return (
      kBlockList.count(name) != 0 ||
      name.find("$internal$") != std::string::npos ||
      (metadata.has_value() && metadata->companionFunction));
}

const AggregationFunctionMetadataPtr
NativeFunctionMetadata::getAggregationFunctionMetadata(
    const std::string& name,
    const facebook::velox::exec::AggregateFunctionSignature* signature) const {
  auto metadata = std::make_shared<protocol::AggregationFunctionMetadata>();
  metadata->intermediateType =
      boost::algorithm::to_lower_copy(signature->intermediateType().toString());
  metadata->isOrderSensitive =
      facebook::velox::exec::getAggregateFunctionEntry(name)
          ->metadata.orderSensitive;
  return metadata;
}

} // namespace facebook::presto
