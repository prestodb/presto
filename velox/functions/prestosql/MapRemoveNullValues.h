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

namespace facebook::velox::functions {

template <typename TExec>
struct MapRemoveNullValues {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  void call(
      out_type<Map<Generic<T1>, Generic<T2>>>& out,
      const arg_type<Map<Generic<T1>, Generic<T2>>>& inputMap) {
    for (const auto& entry : inputMap) {
      if (entry.second.has_value()) {
        auto [keyWriter, valueWriter] = out.add_item();
        keyWriter.copy_from(entry.first);
        valueWriter.copy_from(entry.second.value());
      }
    }
  }
};

} // namespace facebook::velox::functions
