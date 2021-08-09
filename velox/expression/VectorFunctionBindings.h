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
#include "velox/core/FunctionRegistry.h"
#include "velox/expression/VectorFunctionRegistry.h"
#include "velox/expression/VectorUdfTypeSystem.h"

namespace facebook::velox::exec {

template <typename UDF>
void registerVectorFunctionInternal(
    const std::shared_ptr<const core::IScalarFunction>& metadata,
    const core::FunctionKey& key) {
  AdaptedVectorFunctions().Register(
      key,
      [metadata]() {
        return std::make_unique<VectorAdapterFactoryImpl<UDF>>(
            metadata->returnType());
      },
      metadata->signature());
}

// This function should be called once and alone.
template <typename UDF>
void registerVectorFunction(
    const std::vector<std::string>& names,
    std::shared_ptr<const Type> returnType) {
  auto metadata =
      core::GetSingletonUdfMetadata<typename UDF::Metadata>(move(returnType));
  // The function has to have a name to be callable.
  if (names.empty()) {
    registerVectorFunctionInternal<UDF>(metadata, metadata->key());
  }
  for (const auto& name : names) {
    const core::FunctionKey aliasKey{name, metadata->key().types()};
    registerVectorFunctionInternal<UDF>(metadata, aliasKey);
  }
}

} // namespace facebook::velox::exec
