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

 #include "presto_cpp/main/dynamic_registry/DynamicFunctionRegistrar.h"
 #include "velox/expression/SimpleFunctionRegistry.h"
 #include <iostream>
 // This file defines a mock function that will be dynamically linked and
 // registered. There are no restrictions as to how the function needs to be
 // defined, but the library (.so) needs to provide a `void registerExtensions()`
 // C function in the top-level namespace.
 //
 // (note the extern "C" directive to prevent the compiler from mangling the
 // symbol name).

namespace facebook::velox::common::dynamicRegistry {

    template <typename T>
    struct DynamicFunctionCustomAdd {
      VELOX_DEFINE_FUNCTION_TYPES(T);
      FOLLY_ALWAYS_INLINE bool call(
        out_type<int64_t>& result,
          const arg_type<int64_t>& x1,
          const arg_type<int64_t>& x2) {
        result = x1 + x2;
        return true;
      }
    };
} // namespace facebook::velox::common::dynamicRegistry

extern "C" {
    // In this case, we assume that facebook::presto::registerPrestoFunction
    // will be available and resolve when this library gets loaded.
    void registerExtensions() {
      facebook::presto::registerPrestoFunction<
          facebook::velox::common::dynamicRegistry::DynamicFunctionCustomAdd,
          int64_t,
          int64_t,
          int64_t>("custom_add");
    }
}