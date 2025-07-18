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

#include "velox/core/ITypedExpr.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/parse/Expressions.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::parse {

// Registers type resolver with the parser to resolve return types of special
// forms (if, switch, and, or, etc.) and vector functions.
void registerTypeResolver();

// Given scalar function name and input types, resolves the return type. Throws
// an exception if return type cannot be resolved. Returns null if return type
// cannot be resolved and 'nullOnFailure' is true.
TypePtr resolveScalarFunctionType(
    const std::string& name,
    const std::vector<TypePtr>& args,
    bool nullOnFailure = false);
} // namespace facebook::velox::parse

namespace facebook::velox::core {

class Expressions {
 public:
  using TypeResolverHook = std::function<TypePtr(
      const std::vector<TypedExprPtr>& inputs,
      const std::shared_ptr<const CallExpr>& expr,
      bool nullOnFailure)>;

  using FieldAccessHook = std::function<TypedExprPtr(
      std::shared_ptr<const FieldAccessExpr> fae,
      std::vector<TypedExprPtr>& children)>;

  static TypedExprPtr inferTypes(
      const ExprPtr& expr,
      const TypePtr& input,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants = nullptr);

  static void setTypeResolverHook(TypeResolverHook hook) {
    resolverHook_ = std::move(hook);
  }

  static TypeResolverHook getResolverHook() {
    return resolverHook_;
  }

  static void setFieldAccessHook(FieldAccessHook hook) {
    fieldAccessHook_ = std::move(hook);
  }

  static FieldAccessHook getFieldAccessHook() {
    return fieldAccessHook_;
  }

  static TypedExprPtr inferTypes(
      const ExprPtr& expr,
      const TypePtr& input,
      const std::vector<TypePtr>& lambdaInputTypes,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants = nullptr);

 private:
  static TypedExprPtr resolveLambdaExpr(
      const std::shared_ptr<const LambdaExpr>& lambdaExpr,
      const TypePtr& inputRow,
      const std::vector<TypePtr>& lambdaInputTypes,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants = nullptr);

  static TypedExprPtr tryResolveCallWithLambdas(
      const std::shared_ptr<const CallExpr>& expr,
      const TypePtr& input,
      memory::MemoryPool* pool,
      const VectorPtr& complexConstants = nullptr);

  static TypeResolverHook resolverHook_;
  static FieldAccessHook fieldAccessHook_;
};

/// Returns a signature for 'call' if a signature involving lambda arguments is
/// found, nullptr otherwise.
/// Assumes no overlap in function names between scalar and aggregate
// functions,
/// i.e. 'foo' is either a scalar or aggregate function.
const exec::FunctionSignature* findLambdaSignature(
    const std::shared_ptr<const CallExpr>& callExpr);

} // namespace facebook::velox::core
