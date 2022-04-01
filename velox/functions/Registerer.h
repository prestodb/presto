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
#include "velox/expression/SimpleFunctionAdapter.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox {

// This is a temp wrapper that defines udf<X> for a complete UDF.
// We can remove this once macro-based udfs are completely deprecated.

template <typename T>
struct TempWrapper {
  template <typename B>
  using udf = T;
};

template <template <class...> typename T, typename... TArgs>
using ParameterBinder = TempWrapper<T<exec::VectorExec, TArgs...>>;

template <typename Func, typename TReturn, typename... TArgs>
void registerFunction(
    const std::vector<std::string>& aliases = {},
    std::shared_ptr<const Type> returnType = nullptr) {
  using funcClass = typename Func::template udf<exec::VectorExec>;
  using holderClass =
      core::UDFHolder<funcClass, exec::VectorExec, TReturn, TArgs...>;
  exec::registerSimpleFunction<holderClass>(aliases, move(returnType));
}

// New registration function; mostly a copy from the function above, but taking
// the inner "udf" struct directly, instead of the wrapper. We can keep both for
// a while to maintain backwards compatibility, but the idea is to remove the
// one above eventually.
template <template <class> typename Func, typename TReturn, typename... TArgs>
void registerFunction(
    const std::vector<std::string>& aliases = {},
    std::shared_ptr<const Type> returnType = nullptr) {
  using funcClass = Func<exec::VectorExec>;
  using holderClass =
      core::UDFHolder<funcClass, exec::VectorExec, TReturn, TArgs...>;
  exec::registerSimpleFunction<holderClass>(aliases, move(returnType));
}

} // namespace facebook::velox
