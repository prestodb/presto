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

#include "velox/core/SimpleFunctionMetadata.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/SimpleFunctionAdapter.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

template <typename T>
const std::shared_ptr<const T>& singletonUdfMetadata(
    bool defaultNullBehavior,
    const std::vector<exec::SignatureVariable>& constraints = {}) {
  static auto instance =
      std::make_shared<const T>(defaultNullBehavior, constraints);
  return instance;
}

using Function = SimpleFunctionAdapterFactory;
using Metadata = core::ISimpleFunctionMetadata;
using FunctionFactory = std::function<std::unique_ptr<Function>()>;

struct FunctionEntry {
  FunctionEntry(
      const std::shared_ptr<const Metadata>& metadata,
      const FunctionFactory& factory)
      : metadata_{metadata}, factory_{factory} {}

  const Metadata& getMetadata() const {
    return *metadata_;
  }

  std::unique_ptr<Function> createFunction() const {
    return factory_();
  }

 private:
  const std::shared_ptr<const Metadata> metadata_;
  const FunctionFactory factory_;
};

using SignatureMap = std::unordered_map<
    FunctionSignature,
    std::vector<std::unique_ptr<const FunctionEntry>>>;
using FunctionMap = std::unordered_map<std::string, SignatureMap>;

class SimpleFunctionRegistry {
 public:
  /// Register a UDF with the given aliases and constraints. If an alias already
  /// exists and 'overwrite' is true, the existing entry in the function
  /// registry is overwritten by the current UDF. If an alias already exists and
  /// 'overwrite' is false, the current UDF is not registered with this alias.
  /// This method returns true if all 'aliases' are registered successfully. It
  /// returns false if any alias in 'aliases' already exists in the registry and
  /// is not overwritten.
  template <typename UDF>
  bool registerFunction(
      const std::vector<std::string>& aliases,
      const std::vector<exec::SignatureVariable>& constraints,
      bool overwrite) {
    const auto& metadata = singletonUdfMetadata<typename UDF::Metadata>(
        UDF::is_default_null_behavior, constraints);
    const auto factory = []() { return CreateUdf<UDF>(); };

    if (aliases.empty()) {
      return registerFunctionInternal(
          metadata->getName(), metadata, factory, overwrite);
    } else {
      bool registered = true;
      for (const auto& name : aliases) {
        registered &=
            registerFunctionInternal(name, metadata, factory, overwrite);
      }
      return registered;
    }
  }

  std::vector<std::string> getFunctionNames() const {
    std::vector<std::string> result;
    registeredFunctions_.withRLock([&](const auto& map) {
      result.reserve(map.size());

      for (const auto& entry : map) {
        result.push_back(entry.first);
      }
    });
    return result;
  }

  void clearRegistry() {
    registeredFunctions_.withWLock([&](auto& map) { map.clear(); });
  }

  std::vector<const FunctionSignature*> getFunctionSignatures(
      const std::string& name) const;

  std::vector<std::pair<VectorFunctionMetadata, const FunctionSignature*>>
  getFunctionSignaturesAndMetadata(const std::string& name) const;

  class ResolvedSimpleFunction {
   public:
    ResolvedSimpleFunction(
        const FunctionEntry& functionEntry,
        const TypePtr& type)
        : functionEntry_(functionEntry), type_(type) {}

    auto createFunction() {
      return functionEntry_.createFunction();
    }

    const TypePtr& type() const {
      return type_;
    }

    std::string helpMessage(const std::string& name) const {
      return functionEntry_.getMetadata().helpMessage(name);
    }

    VectorFunctionMetadata metadata() const {
      return VectorFunctionMetadata{
          false,
          functionEntry_.getMetadata().isDeterministic(),
          functionEntry_.getMetadata().defaultNullBehavior()};
    }

   private:
    const FunctionEntry& functionEntry_;
    const TypePtr type_;
  };

  std::optional<ResolvedSimpleFunction> resolveFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes) const;

 private:
  template <typename T>
  static std::unique_ptr<T> CreateUdf() {
    return std::make_unique<T>();
  }

  /// Registers a function with the given name and metadata. If an entry with
  /// the name already exists and 'overwrite' is true, the existing entry is
  /// overwritten. If an entry with the name already exists and 'overwrite' is
  /// false, the function reigstry remain unchanged. This method returns true if
  /// the function is successfully registered. It returns false if the function
  /// registry remains unchanged.
  bool registerFunctionInternal(
      const std::string& name,
      const std::shared_ptr<const Metadata>& metadata,
      const FunctionFactory& factory,
      bool overwrite);

  folly::Synchronized<FunctionMap> registeredFunctions_;
};

const SimpleFunctionRegistry& simpleFunctions();

SimpleFunctionRegistry& mutableSimpleFunctions();

/// This function should be called once and alone.
/// @param names Aliases for the function.
/// @param constraints Additional constraints for variables used in function
/// signature. Primarily used to specify rules for calculating precision and
/// scale for decimal result types.
/// @param overwrite If true, overwrites existing entries in the function
/// registry with the same names.
template <typename UDFHolder>
bool registerSimpleFunction(
    const std::vector<std::string>& names,
    const std::vector<exec::SignatureVariable>& constraints,
    bool overwrite) {
  return mutableSimpleFunctions()
      .registerFunction<SimpleFunctionAdapterFactoryImpl<UDFHolder>>(
          names, constraints, overwrite);
}

} // namespace facebook::velox::exec
