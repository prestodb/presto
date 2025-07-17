# Dependency List
Following is the list of libraries and their minimum version
that Velox requires. Some of these libraries can be installed
via a platform's package manager (eg. `brew` on macOS).
The versions of certain libraries is the default provided by
the platform's package manager. Some libraries can be bundled
by Velox. See details on bundling below.

| Library Name      | Minimum Version | Bundled? |
|-------------------|-----------------|----------|
| ninja             | default         | No       |
| ccache            | default         | No       |
| icu4c             | default         | Yes      |
| gflags            | default         | Yes      |
| glog              | default         | Yes      |
| gtest (testing)   | default         | Yes      |
| libevent          | default         | No       |
| libsodium         | default         | No       |
| lz4               | default         | No       |
| snappy            | default         | No       |
| xz                | default         | No       |
| zstd              | default         | No       |
| openssl           | default         | No       |
| protobuf          | 21.7 >= x < 22  | Yes      |
| boost             | 1.77.0          | Yes      |
| flex              | 2.5.13          | No       |
| bison             | 3.0.4           | No       |
| cmake             | 3.28            | No       |
| double-conversion | 3.1.5           | No       |
| xsimd             | 10.0.0          | Yes      |
| re2               | 2024-07-02      | Yes      |
| fmt               | 10.1.1          | Yes      |
| simdjson          | 3.9.3           | Yes      |
| faiss             | 1.11.0          | Yes      |
| folly             | v2025.04.28.00  | Yes      |
| fizz              | v2025.04.28.00  | No       |
| wangle            | v2025.04.28.00  | No       |
| mvfst             | v2025.04.28.00  | No       |
| fbthrift          | v2025.04.28.00  | No       |
| libstemmer        | 2.2.0           | Yes      |
| DuckDB (testing)  | 0.8.1           | Yes      |
| cpr (testing)     | 1.10.15         | Yes      |
| arrow             | 15.0.0          | Yes      |
| geos              | 3.10.7          | Yes      |
| fast_float        | v8.0.2          | Yes      |
| xxhash            | default         | No       |
| thrift            | 0.16            | No       |

# Bundled Dependency Management
This module provides a dependency management system that allows us to automatically fetch and build dependencies from source if needed.

By default, the system will use dependencies installed on the host and fallback to building from source. This behaviour can be changed by setting environment variables:

- `VELOX_DEPENDENCY_SOURCE=AUTO|BUNDLED|SYSTEM` for all dependencies or
- `<package>_SOURCE=AUTO|BUNDLED|SYSTEM`  for each dependency individually "package" has to use the same spelling as used in `CMakelists.txt`.

## Find<package> Modules
These modules override the find-modules provided by cmake and prevent system versions of the dependencies to be found and allow sub-projects to use the existing targets. (If a dependency uses variables instead of targets setting these or patching the dependency might be necessary).

They are in subfolders and not the root `CMake` folder, so they can selectively be added to the module path when needed:
`list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/icu)`

## Patches
Some dependencies require us to patch them to make them work seamlessly with velox. If a dependency is updated the patche(s) used for it need to be checked for necessary changes. The easiest way to do this, is to check the cmake log for the patch step of the dependency, it will contain a line from git with the notice `patch does not apply` and the build will most likely fail.

There are two common reasons to patch a dependency:

- `*-no-export.patch` this disables install/export of packages for dependencies that do not have a CMake flag to disable it (unlike e.g. protobuf). This is necessary if the dependency uses other dependencies that do not export their targets, as this will cause an error when the consuming dependency tries to install itself.
- `*-config.patch` removes `include()` of generated cmake files that cause a cmake error when the config is used by another dependency in the same project.

Ideally all patches should be upstream when possible and removed once merged.

## Adding new dependencies

- Copy `template.cmake` and rename it to the name used in `find_package` but all lower-case.
- Switch `find_package` vs `velox_set_source('package')` `velox_resolve_dependency('package' 'optional args for find_package')` in `CMakeLists.txt`
- Update the template with the correct package name and download url/repo etc., set any necessary package options
- Try to build and make necessary changes
  - Repeat until success :D (Feel free to raise and issue for review & support)

## Specify a custom url/file path for an offline build

Set environment variables `VELOX_<PACKAGE>_URL` to specify a custom dependency url or local tar file path, an optional sha256 checksum can be provided as `VELOX_<PACKAGE>_SHA256`.
