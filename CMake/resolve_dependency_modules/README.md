# Bundled Dependency Management
This module provides a dependency management system that allows us to automatically fetch and build dependencies from source if needed.

By default the system will use dependencies installed on the host and fallback to building from source. This behaviour can be changed by setting environment variables:

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
- Switch `find_package` vs `set_source('package')` `resolve_dependency('package' 'optional args for find_package')` in `CMakeLists.txt`
- Update the template with the correct package name and download url/repo etc., set any necessary package options
- Try to build and make necessary changes
  - Repeat until success :D (Feel free to raise and issue for review & support)
