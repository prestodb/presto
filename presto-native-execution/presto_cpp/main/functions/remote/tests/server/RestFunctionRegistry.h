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

#include <memory>
#include <string>
#include <unordered_map>

#include "presto_cpp/main/functions/remote/tests/server/RemoteFunctionRestHandler.h"

namespace facebook::presto::functions::remote::rest::test {

/// @brief Registry for remote function REST handlers.
/// Provides a centralized location for registering and looking up function
/// handlers by name. This follows the same pattern as WindowFunctionRegistry
/// in Velox.
class RestFunctionRegistry {
 public:
  /// Returns the singleton instance of the registry.
  static RestFunctionRegistry& getInstance();

  /// Registers a function handler for a given function name.
  /// If a handler with the same name already exists, it will be replaced.
  /// @param functionName The name of the function to register
  /// @param handler The handler implementation for the function
  /// @return true if a handler was replaced, false if this is a new
  /// registration
  bool registerFunction(
      const std::string& functionName,
      std::shared_ptr<RemoteFunctionRestHandler> handler);

  /// Unregisters a function handler by name.
  /// @param functionName The name of the function to unregister
  /// @return true if a handler was found and removed, false otherwise
  bool unregisterFunction(const std::string& functionName);

  /// Looks up a function handler by name.
  /// @param functionName The name of the function to look up
  /// @return The handler if found, nullptr otherwise
  std::shared_ptr<RemoteFunctionRestHandler> getFunction(
      const std::string& functionName) const;

  /// Checks if a function is registered.
  /// @param functionName The name of the function to check
  /// @return true if the function is registered, false otherwise
  bool hasFunction(const std::string& functionName) const;

  /// Clears all registered functions.
  /// Useful for testing scenarios.
  void clear();

 private:
  RestFunctionRegistry() = default;
  ~RestFunctionRegistry() = default;

  // Prevent copying and assignment
  RestFunctionRegistry(const RestFunctionRegistry&) = delete;
  RestFunctionRegistry& operator=(const RestFunctionRegistry&) = delete;

  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<RemoteFunctionRestHandler>>
      functionHandlers_;
};

} // namespace facebook::presto::functions::remote::rest::test
