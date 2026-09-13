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

 #include <iostream>
 #include "presto_cpp/main/functions/dynamic_registry/DynamicFunctionRegistrar.h"
 #include "velox/expression/SimpleFunctionRegistry.h"
 // This file defines a function that will be dynamically linked and
 // registered. This function implements a custom function in the definition,
 // and this library (.so) needs to provide a `void registerExtensions()`
 // C function in the top-level namespace.
 //
 // (note the extern "C" directive to prevent the compiler from mangling the
 // symbol name).

namespace custom::functionRegistry {
    template <typename T>
    struct CustomAdd {
      VELOX_DEFINE_FUNCTION_TYPES(T);
      FOLLY_ALWAYS_INLINE bool call(
        out_type<int64_t>& result,
          const arg_type<int64_t>& x1,
          const arg_type<int64_t>& x2) {
        result = x1 + x2;
        return true;
      }
    };

    template <typename T>
    struct SumArray {
      VELOX_DEFINE_FUNCTION_TYPES(T);
      FOLLY_ALWAYS_INLINE bool call(
        out_type<int64_t>& result,
          const arg_type<facebook::velox::Array<int64_t>>& arr) {
        int64_t sum = 0;
	for (auto val : arr) {
	  if (val.has_value()) {
	    sum += val.value();
	  }
	}
        result = sum;
        return true;
      }
    };

    template <typename T>
    struct MapSize {
      VELOX_DEFINE_FUNCTION_TYPES(T);
      FOLLY_ALWAYS_INLINE bool call(
        out_type<int64_t>& result,
          const arg_type<facebook::velox::Map<int64_t, int64_t>>& m) {
        result = m.size();
        return true;
      }
    };

    template <typename T>
    struct SumNestedArrayElements {
      VELOX_DEFINE_FUNCTION_TYPES(T);
      FOLLY_ALWAYS_INLINE bool call(
        out_type<int64_t>& result,
          const arg_type<facebook::velox::Array<facebook::velox::Array<int64_t>>>& arr) {
        int64_t sum = 0;
        for (auto innerOpt : arr) {
          if (innerOpt.has_value()) {
            for (auto val : innerOpt.value()) {
              if (val.has_value()) {
                sum += val.value();
              }
            }
          }
        }
        result = sum;
        return true;
    }
};
} // namespace custom::functionRegistry

extern "C" {
    void registerExtensions() {
      facebook::presto::registerPrestoFunction<
          custom::functionRegistry::CustomAdd,
          int64_t,
          int64_t,
          int64_t>("dynamic_custom_add");

      facebook::presto::registerPrestoFunction<
                custom::functionRegistry::CustomAdd,
                int64_t,
                int64_t,
                int64_t>("custom_add");

      facebook::presto::registerPrestoFunction<
          custom::functionRegistry::SumArray,
          int64_t,
          facebook::velox::Array<int64_t>>("sum_array");

      facebook::presto::registerPrestoFunction<
          custom::functionRegistry::MapSize,
          int64_t,
          facebook::velox::Map<int64_t, int64_t>>("map_size");

      facebook::presto::registerPrestoFunction<
          custom::functionRegistry::SumNestedArrayElements,
          int64_t,
          facebook::velox::Array<facebook::velox::Array<int64_t>>>("sum_nested_array_elements");
    }
}
