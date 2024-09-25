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

#include "presto_cpp/main/functions/DynamicLibraryLoader.h"
#include <dlfcn.h>
#include <iostream>
#include "velox/common/base/Exceptions.h"
#include "velox/expression/SimpleFunctionRegistry.h"
namespace facebook::presto {

static constexpr const char* kSymbolName = "registry";

bool loadDynamicLibraryFunctions(const char* fileName) {
  auto& simpleFunctions = velox::exec::simpleFunctions();
  // Try to dynamically load the shared library.
  void* handler = dlopen(fileName, RTLD_NOW);

  if (handler == nullptr) {
    VELOX_USER_FAIL("Error while loading shared library: {}", dlerror());
  }
  using simpleFunctionsInternal = velox::exec::SimpleFunctionRegistry* (*)();
   simpleFunctionsInternal simpleFunctionsInternalInLoader = (simpleFunctionsInternal) dlsym(handler, "simpleFunctionsInternalGetInstance");
  velox::exec::SimpleFunctionRegistry* sharedsimpleFunctionsInternal = simpleFunctionsInternalInLoader();
   // Lookup the symbol.
  void* registrySymbol = dlsym(handler, kSymbolName);
  auto registryFunction = reinterpret_cast<bool (*)()>(registrySymbol);
  char* error = dlerror();

  if (error != nullptr) {
    VELOX_USER_FAIL("Couldn't find Velox registry symbol: {}", error);
  }
  dlclose(handler);
  return registryFunction();
}

} // namespace facebook::presto

