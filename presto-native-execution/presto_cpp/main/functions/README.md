# Prestissimo: Dynamically Loading Function Library

This library adds the ability to load User Defined Functions (UDFs) without having to fork and build Prestissimo, through the use of shared libraries that a Prestissimo worker can access. The dynamic functions are to be loaded on launch of the Presto server. The Presto server searches for any .so or .dylib files and loads them using this library.

## Getting started
### 1. Create a cpp file for your dynamic library
For dynamically loaded function registration, the format followed is mirrored of that of built-in function registration with some noted differences. Using [MyDynamicTestFunction.cpp](tests/MyDynamicTestFunction.cpp) as an example, the function uses the extern "C" keyword to protect against name mangling. A registry() function call is also necessary here.

### 2. Register functions dynamically by creating .dylib or .so shared libraries and dropping them in a plugin directory
These shared libraries may be made using CMakeLists like the following:
```
add_library(name_of_dynamic_fn SHARED TestFunction.cpp)
target_link_libraries(name_of_dynamic_fn PRIVATE xsimd fmt::fmt velox_expression)
```

### 3. In the Prestissimo worker's config.properties file, set the plugin.dir property
Set the value of plugin.dir to the file path of the directory where the shared libraries are located.  
```
plugin.dir="User\Test\Path\plugin"
```
### 4. When the worker or the sidecar process starts, it will scan the plugin directory and attempt to dynamically load all shared libraries