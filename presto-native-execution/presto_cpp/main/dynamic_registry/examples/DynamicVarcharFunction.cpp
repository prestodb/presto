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
#include "presto_cpp/main/dynamic_registry/DynamicFunctionRegistrar.h"
// This file defines a mock function that will be dynamically linked and
// registered. There are no restrictions as to how the function needs to be
// defined, but the library (.so) needs to provide a `void registerExtensions()`
// C function in the top-level namespace.
//
// (note the extern "C" directive to prevent the compiler from mangling the
// symbol name).

namespace facebook::velox::common::dynamicRegistry {

template <typename T>
struct DynamicVarcharFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& prefix,
      const arg_type<Varchar>& main) {
    std::string temp = fmt::format("{}|{}", prefix, main);
    result = temp;
    return true;
  }
};

} // namespace facebook::velox::common::dynamicRegistry

extern "C" {
// The function registerExtensions is the entry point to execute the
// registration of the UDF and cannot be changed.
void registerExtensions() {
  facebook::presto::registerPrestoFunction<
      facebook::velox::common::dynamicRegistry::DynamicVarcharFunction,
      facebook::velox::Varchar,
      facebook::velox::Varchar,
      facebook::velox::Varchar>("dynamic_varchar");
}
}
