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

#pragma once

namespace facebook::presto {

/// Dynamically opens and registers functions defined in a shared library (.so)
///
/// Given a shared library name (.so), this function will open it using dlopen,
/// search for a "void registry()" C function containing the registration code
/// for the functions defined in library, and execute it.
///
/// The library being linked needs to provide a function with the following
/// signature:
///
///   void registry();
///
/// The registration function needs to be defined in the top-level namespace,
/// and be enclosed by a extern "C" directive to prevent the compiler from
/// mangling the symbol name.
void loadDynamicLibraryFunctions(const char* fileName);

} // namespace facebook::presto

