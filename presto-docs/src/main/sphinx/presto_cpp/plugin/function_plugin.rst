===============
Function Plugin
===============

Creating a Shared Library for UDFs
----------------------------------
User defined functions (UDFs) allow users to create custom functions without the need to rebuild the executable. 
There are many benefits to UDFs, such as:

* Simplify SQL queries by creating UDFs for repetitive logic.
* Implement custom logic pertaining to the specific business use cases of the users.
* Once defined, easily reusable and called multiple times just like built in functions.
* Shorter compile times.

1. To create the UDF, create a new C++ file in the same format as the below example file named ``ExampleFunction.cpp``:

   .. code-block:: c++

      #include "presto_cpp/main/dynamic_registry/DynamicFunctionRegistrar.h"

      template <typename T>
      struct NameOfStruct {
         VELOX_DEFINE_FUNCTION_TYPES(T);
         FOLLY_ALWAYS_INLINE bool call(out_type<int64_t>& result, const arg_type<Varchar> in) {
            ...
         }
      };

      extern "C" {
         void registerExtensions() {
            facebook::presto::registerPrestoFunction<
               NameOfStruct,
               int64_t, facebook::velox::Varchar>("function_name", "test.namespace");
            }
      }  

   Note: The ``int64_t`` return type and the ``Varchar`` input type can be changed as needed. Additional or no arguments may be specified as well. For more examples, see the `examples <https://github.com/soumiiow/presto/tree/dylib_new/presto-native-execution/presto_cpp/main/dynamic_registry/examples>`_.
   The functions should follow the `Velox scalar function API <https://facebookincubator.github.io/velox/develop/scalar-functions.html>`_. Return types and argument types include:

   * simple types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, TIMESTAMP, DATE, VARCHAR, VARBINARY.
   * complex types: ARRAY, MAP, ROW. They can be mixed with other simple and complex types. For example: MAP(INTEGER, ARRAY(BIGINT)).

2. Create a shared library using ``CMakeLists.txt`` like the following:

   .. code-block:: text

      add_library(name_of_dynamic_fn SHARED ExampleFunction.cpp)
      target_link_libraries(name_of_dynamic_fn PRIVATE presto_dynamic_function_registrar fmt::fmt gflags::gflags xsimd)

3. Place your shared libraries in the ``plugin`` directory. The path to this directory defaults to the ``plugin`` directory relative to the directory in which the process is being run but it is configurable in the ``plugin.dir`` property set in :doc:`../plugin`. 