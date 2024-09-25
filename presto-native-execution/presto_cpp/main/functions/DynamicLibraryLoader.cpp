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
namespace facebook::presto {

static constexpr const char* kSymbolName = "registry";

void loadDynamicLibraryFunctions(const char* fileName) {
  // Try to dynamically load the shared library.
  void* handler = dlopen(fileName, RTLD_NOW);

  if (handler == nullptr) {
    VELOX_USER_FAIL("Error while loading shared library: {}", dlerror());
  }

  // Lookup the symbol.
  void* registrySymbol = dlsym(handler, kSymbolName);
  auto registryFunction = reinterpret_cast<void (*)()>(registrySymbol);
  char* error = dlerror();

  if (error != nullptr) {
    VELOX_USER_FAIL("Couldn't find Velox registry symbol: {}", error);
  }
  registryFunction();
  std::cout << "LOADED DYLLIB 1" << std::endl;
}

} // namespace facebook::presto

