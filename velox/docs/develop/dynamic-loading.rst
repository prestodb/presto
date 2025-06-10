***********************************
Dynamic Loading of Velox Extensions
***********************************

This generic utility adds extensibility features to load User Defined Functions (UDFs) without having to fork and build Velox, through the use of shared libraries. Support for connectors and types will be added in the future.

This mechanism relies on ABI compatibility, meaning it is only guarenteed to work if the shared libraries and the Velox build are based on the same Velox version.
Using shared libraries built against a different version of Velox may result in undefined behavior or runtime errors due to ABI mismatches.

Getting Started
===============

1. **Create a C++ file for your dynamic library**

   For dynamically loaded function registration, the format followed mirrors that of built-in function registration with some noted differences. Using `DynamicTestFunction.cpp` as an example, the function uses the `extern "C"` keyword to protect against name mangling.
   The `registrationFunctionName` function here acts as the entrypoint for the dynamic library for loading symbols. The `registrationFunctionName` function name is customizable and defaults to `registerExtensions` when not specified in the library loading call.

   Make sure to also include the necessary header file:

   .. code-block:: cpp

      #include "velox/functions/Udf.h"

   Example template for a function with no arguments returning a BIGINT:

   .. code-block:: cpp

      #include "velox/functions/Udf.h"

      namespace example_namespace {

      template <typename T>
      struct DynamicFunction {
        FOLLY_ALWAYS_INLINE bool call(int64_t& result) {
          // Code to calculate result.
        }
      };
      }

      extern "C" {
      void registrationFunctionName() {
        facebook::velox::registerFunction<
            example_namespace::DynamicFunction,
            int64_t>({"your_function_name"});
      }
      }

2. **Register functions dynamically by creating `.dylib` (MacOS) or `.so` (Linux) shared libraries**

   These shared libraries may be created using a CMakeLists file like the following:

   .. code-block:: cmake

      add_library(name_of_dynamic_fn SHARED TestFunction.cpp)
      target_link_libraries(name_of_dynamic_fn PRIVATE fmt::fmt Folly::folly glog::glog xsimd)

      if(APPLE)
      set(COMMON_LIBRARY_LINK_OPTIONS "-Wl,-undefined,dynamic_lookup")
      else()
      set(COMMON_LIBRARY_LINK_OPTIONS "-Wl,--exclude-libs,ALL")
      endif()

      target_link_options(name_of_dynamic_fn PRIVATE ${COMMON_LIBRARY_LINK_OPTIONS})

   Additional details:

   - **Required Libraries**:
      - The `fmt::fmt`, `Folly::folly`, and `xsimd` libraries are required for all necessary symbols to be defined when loading `TestFunction.cpp` dynamically.
      - The `glog::glog` library is required on macOS but optional on Linux.

   - **Linking Options**:
      - On **macOS**: The `target_link_options` (`"-Wl,-undefined,dynamic_lookup"`) allow symbols to be resolved at runtime.
      - On **Linux**: The `target_link_options` (`"-Wl,--exclude-libs,ALL"`) prevent errors related to symbols being linked both statically and dynamically.

Notes
=====

- In Velox, a function's signature is determined solely by its name and argument types. The return type is not taken into account. As a result, if a function with an identical signature is added but with a different return type, it will overwrite the existing function.
- Function overloading is supported. Therefore, multiple functions can share the same name as long as they differ in the number or types of arguments.
