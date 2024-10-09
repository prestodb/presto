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

#include "velox/expression/FunctionSignature.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"

namespace facebook::velox {

template <typename T>
struct FuncOne {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Set func_one as non-deterministic.
  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& /* result */,
      const arg_type<velox::Varchar>& /* arg1 */) {
    return true;
  }
};

template <typename T>
struct FuncTwo {
  template <typename T1, typename T2>
  FOLLY_ALWAYS_INLINE bool callNullable(
      int64_t& /* result */,
      const T1* /* arg1 */,
      const T2* /* arg2 */) {
    return true;
  }
};

template <typename T>
struct FuncThree {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      ArrayWriter<int64_t>& /* result */,
      const ArrayVal<int64_t>& /* arg1 */) {
    return true;
  }
};

template <typename T>
struct FuncFour {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& /* result */,
      const arg_type<velox::Varchar>& /* arg1 */) {
    return true;
  }
};

template <typename T>
struct FuncFive {
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const int64_t& /* arg1 */) {
    result = 5;
    return true;
  }
};

// FuncSix has the same signature as FuncFive. It's used to test overwrite
// during registration.
template <typename T>
struct FuncSix {
  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const int64_t& /* arg1 */) {
    result = 6;
    return true;
  }
};

template <typename T>
struct VariadicFunc {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& /* result */,
      const arg_type<Variadic<velox::Varchar>>& /* arg1 */) {
    return true;
  }
};

class VectorFuncOne : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx& /* context */,
      velox::VectorPtr& /* result */) const override {}

  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    // varchar -> bigint
    return {velox::exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("varchar")
                .build()};
  }
};

class VectorFuncTwo : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx& /* context */,
      velox::VectorPtr& /* result */) const override {}

  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    // array(varchar) -> array(bigint)
    return {velox::exec::FunctionSignatureBuilder()
                .returnType("array(bigint)")
                .argumentType("array(varchar)")
                .build()};
  }
};

class VectorFuncThree : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx& /* context */,
      velox::VectorPtr& /* result */) const override {}

  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    // ... -> opaque
    return {velox::exec::FunctionSignatureBuilder()
                .returnType("opaque")
                .argumentType("any")
                .build()};
  }
};

class VectorFuncFour : public velox::exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& /* rows */,
      std::vector<velox::VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      velox::exec::EvalCtx& /* context */,
      velox::VectorPtr& /* result */) const override {}

  static std::vector<std::shared_ptr<velox::exec::FunctionSignature>>
  signatures() {
    // map(K,V) -> array(K)
    return {velox::exec::FunctionSignatureBuilder()
                .knownTypeVariable("K")
                .typeVariable("V")
                .returnType("array(K)")
                .argumentType("map(K,V)")
                .build()};
  }
};

} // namespace facebook::velox
