=======================
Function Plugin
=======================

Creating a Shared Library for UDFs
----------------------------------
User defined functions (UDFs) allow users to create custom functions without the need to rebuild the executable. 
There are many benefits to UDFs other than shorter compile times, such as:

* Simplify SQL queries by creating UDFs for repetitive logic.
* Implement custom logic pertaining to the specific business use cases of the users.
* Once defined, easily reusable and called multiple times just like built in functions.

1. To create the UDF, create a new C++ file in the following format:

   .. code-block:: c++

      #include "presto_cpp/main/dynamic_registry/DynamicFunctionRegistrar.h"

      template <typename T>
      struct nameOfStruct {
         FOLLY_ALWAYS_INLINE bool call(int64_t& result) {
            ...
         }
      };

      extern "C" {
         void registry() {
            facebook::presto::registerPrestoFunction<
               nameOfStruct,
               int64_t>("function_name");
            }
      }  

   Note: the ``int64_t`` return type can be changed as needed. For more examples, see the `README <https://github.com/prestodb/presto-native-execution/main/dynamic_registry/README.md>`_.

2. Create a shared library which may be made using CMakeLists like the following:

   .. code-block:: text

      add_library(name_of_dynamic_fn SHARED TestFunction.cpp)
      target_link_libraries(name_of_dynamic_fn PRIVATE fmt::fmt Folly::folly gflags::gflags)

3. Put your shared libraries in the plugin directory described in :doc:`../plugin`.