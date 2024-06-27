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

#include "velox/common/base/CompareFlags.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

// Comparison functions that implement SparkSQL semantics.
// Intended to be used with scalar types (including strings and timestamps).
template <typename T>
static inline bool isNan(const T& value) {
  if constexpr (std::is_floating_point<T>::value) {
    return std::isnan(value);
  } else {
    return false;
  }
}

template <typename T>
struct Less {
  constexpr bool operator()(const T& a, const T& b) const {
    if (isNan(a)) {
      return false;
    }
    if (isNan(b)) {
      return true;
    }
    return a < b;
  }
};

template <typename T>
struct Greater : private Less<T> {
  constexpr bool operator()(const T& a, const T& b) const {
    return Less<T>::operator()(b, a);
  }
};

template <typename T>
struct Equal {
  constexpr bool operator()(const T& a, const T& b) const {
    // In SparkSQL, NaN is defined to equal NaN.
    if (isNan(a)) {
      return isNan(b);
    }
    return a == b;
  }
};

template <typename T>
struct LessOrEqual {
  constexpr bool operator()(const T& a, const T& b) const {
    Less<T> less;
    Equal<T> equal;
    return less(a, b) || equal(a, b);
  }
};

template <typename T>
struct GreaterOrEqual : private Less<T> {
  constexpr bool operator()(const T& a, const T& b) const {
    Less<T> less;
    Equal<T> equal;
    return less(b, a) || equal(a, b);
  }
};

/// Supported Types:
/// TINYINT
/// SMALLINT
/// INTEGER
/// BIGINT
/// HUGEINT
/// REAL
/// DOUBLE
/// BOOLEAN
/// VARCHAR
/// TIMESTAMP
/// VARBINARY

/// Special cases:
/// NaN in Spark is handled differently from standard floating point semantics.
/// It is considered larger than any other numeric values.

std::shared_ptr<exec::VectorFunction> makeEqualTo(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig&);

std::shared_ptr<exec::VectorFunction> makeLessThan(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig&);

std::shared_ptr<exec::VectorFunction> makeGreaterThan(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig&);

std::shared_ptr<exec::VectorFunction> makeLessThanOrEqual(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig&);

std::shared_ptr<exec::VectorFunction> makeGreaterThanOrEqual(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig&);

inline std::vector<std::shared_ptr<exec::FunctionSignature>>
comparisonSignatures() {
  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  signatures.emplace_back(exec::FunctionSignatureBuilder()
                              .integerVariable("a_precision")
                              .integerVariable("a_scale")
                              .returnType("boolean")
                              .argumentType("DECIMAL(a_precision, a_scale)")
                              .argumentType("DECIMAL(a_precision, a_scale)")
                              .build());
  for (const auto& inputType :
       {"tinyint",
        "smallint",
        "integer",
        "bigint",
        "real",
        "double",
        "boolean",
        "varchar",
        "varbinary",
        "timestamp",
        "date",
        "interval day to second",
        "interval year to month"}) {
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .returnType("boolean")
                                .argumentType(inputType)
                                .argumentType(inputType)
                                .build());
  }
  return signatures;
}

template <typename T>
struct BetweenFunction {
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(
      bool& result,
      const TInput& value,
      const TInput& low,
      const TInput& high) {
    result = value >= low && value <= high;
  }
};

std::shared_ptr<exec::VectorFunction> makeEqualToNullSafe(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

template <typename T>
struct EqualToFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  // For arbitrary nested complex types.
  FOLLY_ALWAYS_INLINE void call(
      bool& out,
      const arg_type<Generic<T1>>& lhs,
      const arg_type<Generic<T1>>& rhs) {
    static constexpr CompareFlags kFlags =
        CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

    auto result = lhs.compare(rhs, kFlags);
    out = (result.value() == 0);
  }
};

template <typename T>
struct EqualNullSafeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  // For arbitrary nested complex types.
  FOLLY_ALWAYS_INLINE void callNullable(
      bool& out,
      const arg_type<Generic<T1>>* lhs,
      const arg_type<Generic<T1>>* rhs) {
    static constexpr CompareFlags kFlags =
        CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);
    if (!lhs && !rhs) {
      out = true;
    } else if (!lhs || !rhs) {
      out = false;
    } else {
      auto result = lhs->compare(*rhs, kFlags);
      out = (result.value() == 0);
    }
  }
};

} // namespace facebook::velox::functions::sparksql
