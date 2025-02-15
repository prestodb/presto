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

#include <dlfcn.h>
#include <iostream>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

static constexpr const char* kSymbolName = "registry";

void loadDynamicLibrary(const std::string& fileName) {
  // Try to dynamically load the shared library.
  void* handler = dlopen(fileName.c_str(), RTLD_NOW);

  if (handler == nullptr) {
    VELOX_USER_FAIL("Error while loading shared library: {}", dlerror());
  }

  LOG(INFO) << "Loaded library " << fileName << ". Searching registry symbol "
            << kSymbolName;
  // Lookup the symbol.
  void* registrySymbol = dlsym(handler, kSymbolName);
  auto loadUserLibrary = reinterpret_cast<void (*)()>(registrySymbol);
  const char* error = dlerror();

  // Check for an error first as a null symbol pointer is not necessarily an
  // error.
  if (error != nullptr) {
    VELOX_USER_FAIL("Couldn't find Velox registry symbol: {}", error);
  }

  if (loadUserLibrary == nullptr) {
    VELOX_USER_FAIL(
        "Symbol '{}' resolved to a nullptr, unable to invoke it.", kSymbolName);
  }

  // Invoke the registry function.
  LOG(INFO) << "Found registry function. Invoking it.";
  loadUserLibrary();
  LOG(INFO) << "Registration complete.";
}

} // namespace facebook::velox
