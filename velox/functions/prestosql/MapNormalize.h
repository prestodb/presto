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

#include "velox/expression/ComplexViewTypes.h"
#include "velox/functions/Udf.h"

namespace facebook::velox::functions {

template <typename TExec>
struct MapNormalizeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  void call(
      out_type<Map<Varchar, double>>& out,
      const arg_type<Map<Varchar, double>>& inputMap) {
    double totalSum = 0.0;
    for (const auto& entry : inputMap) {
      if (entry.second.has_value()) {
        totalSum += entry.second.value();
      }
    }

    // totalSum can be zero, but that's OK. See
    // https://github.com/prestodb/presto/issues/22209 for Presto Java
    // semantics.

    for (const auto& entry : inputMap) {
      if (!entry.second.has_value()) {
        auto& keyWriter = out.add_null();
        keyWriter.copy_from(entry.first);
      } else {
        auto [keyWriter, valueWriter] = out.add_item();
        keyWriter.copy_from(entry.first);
        valueWriter = entry.second.value() / totalSum;
      }
    }
  }
};

} // namespace facebook::velox::functions
