<!--
Copyright (c) Facebook, Inc. and its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# CMake: Use Base Functions

> [!IMPORTANT]  Please use `target_link_libraries` and `add_library`
> instead of the `velox_*` functions when adding or linking to targets
> within wave/ and label tests with `cuda_driver`.

The `wave` GPU component links against the CUDA driver in several targets.
They can be built on machines without the actual driver installed, this
requires the relevant 'stub' packages to be installed (see setup scripts).

Any library that statically links against the stubs **can not** run on a
machine without an actual CUDA driver installed (like our CI).
For this reason we need to use the base functions to create standalone
libraries for wave to avoid linking statically against the stubs when
building the monolithic library and label any tests with 'cuda_driver'
to allow excluding them from ctest on machines without the driver.
