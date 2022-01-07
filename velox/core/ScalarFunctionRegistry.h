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

#include "folly/hash/Hash.h"
#include "velox/common/serialization/Registry.h"
#include "velox/core/ScalarFunction.h"
#include "velox/type/Type.h"

namespace facebook {
namespace velox {
namespace core {

template <typename T>
std::shared_ptr<const IScalarFunction> GetSingletonUdfMetadata(
    std::shared_ptr<const Type> returnType) {
  static auto instance = std::make_shared<const T>(std::move(returnType));
  return instance;
}

template <typename T>
std::unique_ptr<T> CreateUdf(std::shared_ptr<const Type> returnType) {
  return std::make_unique<T>(std::move(returnType));
}

using ScalarFunctionRegistry =
    Registry<FunctionKey, std::unique_ptr<IScalarFunction>()>;

ScalarFunctionRegistry& ScalarFunctions();

template <typename UDF>
void registerFunctionInternal(
    const std::shared_ptr<const IScalarFunction>& metadata,
    const FunctionKey& key) {
  ScalarFunctions().Register(
      key,
      [metadata]() { return CreateUdf<UDF>(metadata->returnType()); },
      metadata->helpMessage(key.name()));
}

// This function should be called once and alone.
template <typename UDF>
void registerFunction(
    const std::vector<std::string>& names,
    std::shared_ptr<const Type> returnType) {
  auto metadata =
      GetSingletonUdfMetadata<typename UDF::Metadata>(std::move(returnType));

  // A function has to have a name to be callable.
  if (names.empty()) {
    registerFunctionInternal<UDF>(metadata, metadata->key());
  }
  for (const auto& name : names) {
    const FunctionKey aliasKey{name, metadata->argTypes()};
    registerFunctionInternal<UDF>(metadata, aliasKey);
  }
}

} // namespace core
} // namespace velox
} // namespace facebook
