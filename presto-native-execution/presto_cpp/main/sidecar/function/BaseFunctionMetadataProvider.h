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
#include <unordered_map>

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Window.h"
#include "velox/expression/FunctionMetadata.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/FunctionRegistry.h"

namespace facebook::presto {

using AggregateFunctionMap =
    std::unordered_map<std::string, velox::exec::AggregateFunctionEntry>;
using AggregationFunctionMetadataPtr =
    std::shared_ptr<protocol::AggregationFunctionMetadata>;
using FunctionMetadataPtr =
    std::shared_ptr<protocol::JsonBasedUdfFunctionMetadata>;

class BaseFunctionMetadataProvider {
 public:
  virtual ~BaseFunctionMetadataProvider() = default;

  json getFunctionsMetadata(
      const std::optional<std::string>& catalog = std::nullopt) const;

 protected:
  virtual bool isInternalScalarFunction(const std::string& name) const = 0;

  virtual const velox::FunctionSignatureMap& scalarFunctions() const = 0;

  virtual const velox::exec::AggregateFunctionSignatureMap& aggregateFunctions() const = 0;

  virtual const velox::exec::WindowFunctionMap& windowFunctions() const = 0;

  FunctionMetadataPtr buildFunctionMetadata(
      const std::string& name,
      const std::string& schema,
      const protocol::FunctionKind& kind,
      const facebook::velox::exec::FunctionSignature* signature) const;

  virtual std::optional<velox::exec::VectorFunctionMetadata>
  getVeloxFunctionMetadata(const std::string& name) const = 0;

  json::array_t buildScalarMetadata(
      const std::string& name,
      const std::string& schema,
      const std::vector<const facebook::velox::exec::FunctionSignature*>&
          signatures) const;

  virtual const AggregationFunctionMetadataPtr getAggregationFunctionMetadata(
      const std::string& name,
      const facebook::velox::exec::AggregateFunctionSignature* signature)
      const = 0;

  json::array_t buildAggregateMetadata(
      const std::string& name,
      const std::string& schema,
      const std::vector<velox::exec::AggregateFunctionSignaturePtr>&
          signatures) const;

  json::array_t buildWindowMetadata(
      const std::string& name,
      const std::string& schema,
      const std::vector<velox::exec::FunctionSignaturePtr>& signatures) const;
};

} // namespace facebook::presto
