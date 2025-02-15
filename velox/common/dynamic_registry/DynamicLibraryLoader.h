/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#pragma once

namespace facebook::velox {

/// Dynamically opens and registers functions defined in a shared library.
///
/// The library being linked needs to provide a function with the following
/// signature:
///
/// void registry();
///
/// The registration function needs to be defined in the global namespace,
/// and be enclosed by a extern "C" directive to prevent the compiler from
/// mangling the symbol name.

/// The function uses dlopen to load the shared library.
/// It then searches for the "void registry()" C function which typically
/// contains all the registration code for the user-defined Velox components
/// such as functions defined in library. After locating the function it
/// executes the registration bringing the user-defined Velox components
/// such as function in the scope of the Velox runtime.
///
/// Loading a library twice can cause a components to be registered twice.
/// This can fail for certain Velox components.
void loadDynamicLibrary(const std::string& fileName);

} // namespace facebook::velox
