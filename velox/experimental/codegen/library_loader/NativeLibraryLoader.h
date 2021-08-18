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
#include <dlfcn.h>
#include <algorithm>
#include <filesystem>
#include <map>
#include <string>
#include <vector>
#include "fmt/format.h"
#include "velox/experimental/codegen/library_loader/FunctionTable.h"
#include "velox/experimental/codegen/library_loader/NativeLibraryLoader.h"
#include "velox/experimental/codegen/library_loader/NativeLibraryLoaderException.h"

namespace facebook {
namespace velox {
namespace codegen {
namespace native_loader {

/// Load and manage the life cycle of dynamically generated libraries.

class NativeLibraryLoader {
  struct LoadedLibraryInfo {
    std::filesystem::path path;
    void* libraryPtr;
    std::shared_ptr<FunctionTable> functionTable;
  };

 public:
  static NativeLibraryLoader& getDefaultLoader() {
    static NativeLibraryLoader loader;
    return loader;
  }

  // Force loading a dynamic library with out checking, intializing or
  // extracting function table. Used in unit testing only.
  void* uncheckedLoadLibrary(const std::filesystem::path& path) {
    auto it = std::find_if(
        loadedLibraries.begin(),
        loadedLibraries.end(),
        [&path](const auto& loadedLibraryEntry) {
          const auto& libraryInfo = loadedLibraryEntry.second;
          return libraryInfo.path == path;
        });
    if (it != loadedLibraries.end()) {
      // What  happens when we have different library with the same path.
      throw NativeLibraryLoaderException(
          "Library : " + path.string() + "already loaded");
    };
    void* libraryPtr = loadLibraryInternal(path);
    loadedLibraries.emplace(
        libraryPtr, LoadedLibraryInfo{path, libraryPtr, nullptr});
    return libraryPtr;
  }

  // Unload an unchecked laoded library. Used in unit testing only.
  void uncheckedUnLoadLibrary(void* libraryPtr) {
    if (auto mapIterator = loadedLibraries.find(libraryPtr);
        mapIterator == loadedLibraries.end()) {
      throw NativeLibraryLoaderException(fmt::format(
          "Attempting to unload unknown library at {}", libraryPtr));
    }

    loadedLibraries.erase(loadedLibraries.find(libraryPtr));
    dlclose(libraryPtr);
    auto error = dlErrorToException();
    if (error) {
      throw error.value();
    }
  }

  // Extract function with out checking function table. Used in unit testing
  // only.
  template <typename Func>
  Func uncheckedGetFunction(const std::string& functionName, void* libraryPtr) {
    constexpr bool isFunctionPtr = std::is_pointer_v<Func> &&
        std::is_function_v<std::remove_pointer_t<Func>>;
    static_assert(
        isFunctionPtr, "Template argument need to be a function pointer");
    if (loadedLibraries.find(libraryPtr) == loadedLibraries.end()) {
      throw NativeLibraryLoaderException(
          fmt::format("No library loaded at {}", libraryPtr));
    };

    return uncheckedGetSymbolPtr<Func>(libraryPtr, functionName);
  }

  /// Loads and verifies a given dynamic libraries
  /// \param path
  /// \return pointer to the loader libraries
  void* loadLibrary(
      const std::filesystem::path& path,
      void* initArgument = nullptr) {
    auto it = std::find_if(
        loadedLibraries.begin(),
        loadedLibraries.end(),
        [&path](const auto& loadedLibraryEntry) {
          const auto& libraryInfo = loadedLibraryEntry.second;
          return libraryInfo.path == path;
        });
    if (it != loadedLibraries.end()) {
      // What  happens when we have different library with the same path.
      throw NativeLibraryLoaderException(
          "Library : " + path.string() + "already loaded");
    };
    void* libraryPtr = loadLibraryInternal(path);
    checkLibrary(libraryPtr);
    auto functionTable = initLibrary(libraryPtr, initArgument);
    loadedLibraries.emplace(
        libraryPtr,
        LoadedLibraryInfo{path, libraryPtr, std::move(functionTable)});
    return libraryPtr;
  }

  ///
  /// \tparam Func expected funuction's ype signature
  /// \param functionName function name
  /// \param libPtr previously loaded library pointer (typically return by
  /// loadLibrary) \return Function object, or throw on errors
  template <typename Func>
  Func getFunction(const std::string& functionName, void* libraryPtr) {
    constexpr bool isFunctionPtr = std::is_pointer_v<Func> &&
        std::is_function_v<std::remove_pointer_t<Func>>;
    static_assert(
        isFunctionPtr, "Template argument need to be a function pointer");
    if (loadedLibraries.find(libraryPtr) == loadedLibraries.end()) {
      throw NativeLibraryLoaderException(
          fmt::format("No library loaded at {}", libraryPtr));
    };
    auto func =
        loadedLibraries[libraryPtr].functionTable->exportedFunctions.find(
            functionName);
    if (func ==
        loadedLibraries[libraryPtr].functionTable->exportedFunctions.end()) {
      throw NativeLibraryLoaderException(fmt::format(
          "library loaded at {} doesn't export function {}",
          libraryPtr,
          functionName));
    }
    if (typeid(Func).hash_code() != func->second.typeInfo) {
      throw NativeLibraryLoaderException(fmt::format(
          "library loaded at {} exports function {} with a different type",
          libraryPtr,
          functionName));
    };
    return reinterpret_cast<Func>(func->second.functionAddress);
  }

  void unLoadLibrary(void* libraryPtr, void* releaseArgument) {
    if (auto mapIterator = loadedLibraries.find(libraryPtr);
        mapIterator == loadedLibraries.end()) {
      throw NativeLibraryLoaderException(fmt::format(
          "Attempting to unload unknown library at {}", libraryPtr));
    }

    uncheckedGetSymbolPtr<decltype(&release)>(
        libraryPtr, "release")(releaseArgument);
    loadedLibraries.erase(loadedLibraries.find(libraryPtr));
    dlclose(libraryPtr);
    auto error = dlErrorToException();
    if (error) {
      throw error.value();
    }
  }

  /// Load library internal
  /// \param path
  /// \return library pointers
  static void* loadLibraryInternal(const std::filesystem::path& path) {
    void* loadedLib = dlopen(path.string().c_str(), kDefaultDlopenFlags);
    auto error = dlErrorToException();
    if (error) {
      throw error.value();
    }
    return loadedLib;
  }

 private:
  /// Initialize library right after loading.
  /// The initialization process start by loading the "init" function
  /// defined in the provided library, and return the functionTable using
  /// getFunctionTable \param libraryPtr \return Function table defined in
  /// libraryPtr
  std::shared_ptr<FunctionTable> initLibrary(
      void* libraryPtr,
      void* initArgument) {
    auto initFunc = uncheckedGetSymbolPtr<decltype(&init)>(libraryPtr, "init");
    bool initOK = initFunc(initArgument);
    if (!initOK) {
      throw NativeLibraryLoaderException(
          "Call to init function returned false");
    };
    auto tableGetter = uncheckedGetSymbolPtr<decltype(&getFunctionTable)>(
        libraryPtr, "getFunctionTable");
    auto functionTable = tableGetter();
    if (functionTable == nullptr) {
      throw NativeLibraryLoaderException(
          fmt::format("Failed to get function table from lib {}", libraryPtr));
    };
    return functionTable;
  }

  /// Return a function objection without safety checks
  /// \tparam Func
  /// \param libPtr
  /// \param symbName
  /// \return Function object
  template <typename Func>
  Func uncheckedGetSymbolPtr(void* libraryPtr, const std::string& symbolName) {
    void* symbPtr = dlsym(libraryPtr, symbolName.c_str());
    auto error = dlErrorToException();
    if (error) {
      throw error.value();
    }
    LOG(INFO) << fmt::format(
        "{} {} loaded symbol {} at {} from library at {}",
        __FILE__,
        __LINE__,
        symbolName,
        symbPtr,
        symbolName);

    return reinterpret_cast<Func>(reinterpret_cast<uint64_t>(symbPtr));
  }
  /// Read dlError and converts the results into an exception object
  /// \return NativeLibraryLoaderException if dlError has an error, empty
  /// optional otherwise
  static std::optional<NativeLibraryLoaderException> dlErrorToException() {
    char* error = dlerror();
    if (error == nullptr) {
      return {};
    }
    LOG(INFO) << fmt::format(
        "{}:{} dlerror message : {}", __FILE__, __LINE__, error);
    return NativeLibraryLoaderException(error);
  }

  // Verify that the library exports the required functions.
  // Loads the three required symbols and throws if something fails;
  void checkLibrary(void* libraryPtr) {
    try {
      uncheckedGetSymbolPtr<decltype(&init)>(libraryPtr, "init");
      uncheckedGetSymbolPtr<decltype(&release)>(libraryPtr, "release");
      uncheckedGetSymbolPtr<decltype(&getFunctionTable)>(
          libraryPtr, "getFunctionTable");
    } catch (NativeLibraryLoaderException& exception) {
      throw NativeLibraryLoaderException(fmt::format(
          "Library verification failed with {} ", exception.what()));
    }
  }

  static const int kDefaultDlopenFlags = RTLD_LOCAL | RTLD_LAZY;

  /// map from addresses -> function table
  std::map<void*, LoadedLibraryInfo> loadedLibraries;
};
} // namespace native_loader
} // namespace codegen
} // namespace velox
} // namespace facebook
