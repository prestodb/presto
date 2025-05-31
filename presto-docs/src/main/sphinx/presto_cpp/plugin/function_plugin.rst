=======================
Function Plugin
=======================

Creating a Shared Library for UDFs
----------------------------------
User defined functions (UDFs) allow users to create custom functions without the need to rebuild the executable. 
There are many benefits to UDFs, such as:

* Simplify SQL queries by creating UDFs for repetitive logic.
* Implement custom logic pertaining to the specific business use cases of the users.
* Once defined, easily reusable and called multiple times just like built in functions.
* Shorter compile times.

1. To create the UDF, create a new C++ file in the same format as ``TestFunction.cpp`` below:

   .. code-block:: c++

      #include "presto_cpp/main/dynamic_registry/DynamicFunctionRegistrar.h"

      template <typename T>
      struct NameOfStruct {
         FOLLY_ALWAYS_INLINE bool call(int64_t& result) {
            ...
         }
      };

      extern "C" {
         void registryTest() {
            facebook::presto::registerPrestoFunction<
               NameOfStruct,
               int64_t>("function_name", "test.namespace");
            }
      }  

   Note: the ``int64_t`` return type, ``registryTest`` registry symbol name can be changed as needed. For more examples, see the `examples <https://github.com/prestodb/presto/tree/master/presto-native-execution/main/dynamic_registry/examples>`_.

2. Create a shared library which may be made using ``CMakeLists.txt`` like the following:

   .. code-block:: text

      add_library(name_of_dynamic_fn SHARED TestFunction.cpp)
      target_link_libraries(name_of_dynamic_fn PRIVATE fmt::fmt Folly::folly gflags::gflags)

3. Place your shared libraries in the plugin directory. The path to this directory must be the same as the ``plugin.dir`` property set in :doc:`../plugin`.

4. Create a JSON configuration file in the same format as below:

   .. code-block::json  

      {
      "dynamicUdfSignatureMap": {
         "name_of_subdirectory": {
            "function_name": [
            {
               "docString": "",
               "functionKind": "SCALAR",
               "outputType": "INTEGER",
               "entrypoint": "registryTest", 
               "fileName": "name_of_dynamic_fn",
               "paramTypes": [
                  "INTEGER"
               ],
               "nameSpace": "presto.default",
               "routineCharacteristics": {
                  "language": "CPP",
                  "determinism": "DETERMINISTIC",
                  "nullCallClause": "CALLED_ON_NULL_INPUT"
               }
            }
            ]
         }
      }
      } 

   * ``dynamicUdfSignatureMap``: required constant top-level field.
   * ``name_of_subdirectory``: replace this with the subdirectory your shared library is situated in inside the plugin directory.
   * ``function_name``: needs to be the same as the function name in ``TestFunction.cpp``.
   * ``outputType`` and  ``paramTypes``: must match the output and parameter types in ``TestFunction.cpp``. 
      * Examples of typenames include: 
         * simple types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, TIMESTAMP, DATE, VARCHAR, VARBINARY.
         * complex types: ARRAY, MAP, ROW. They can be mixed with other simple and complex types. For example: MAP(INTEGER, ARRAY(BIGINT)).
   * ``registryTest``: needs to match the name of the registry function name in ``TestFunction.cpp``. If this field is not specified in the JSON config, it defaults to ``registerExtensions``.
   * ``fileName``: matches the name of the shared object library (without a .so/.dylib suffix).
   * ``nameSpace``: optional field. omitting this field gives the function the default namespace similarly to the function registeration. must match the namespace of the function.  
   * ``docString``, ``routineCharacteristics``, ``functionKind``: collected for checking metadata.

5. Set the ``plugin.config`` property to the path of the Json configuration file created in Step 4. See :doc:`../plugin`.