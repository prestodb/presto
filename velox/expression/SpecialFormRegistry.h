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

#include "velox/expression/FunctionCallToSpecialForm.h"

namespace facebook::velox::exec {
using RegistryType =
    std::unordered_map<std::string, std::unique_ptr<FunctionCallToSpecialForm>>;

class SpecialFormRegistry {
 public:
  /// Registers 'functionCallToSpecialForm' for mapping user defined
  /// FunctionCallToSpecialForm subclass instances to user-defined
  /// FunctionCallToSpecialForm.
  void registerFunctionCallToSpecialForm(
      const std::string& name,
      std::unique_ptr<FunctionCallToSpecialForm> functionCallToSpecialForm);

  /// Removes all functionCallToSpecialForm registered earlier via calls to
  /// 'registerFunctionCallToSpecialForm'.
  void unregisterAllFunctionCallToSpecialForm();

  /// @brief Return null if the FunctionCallToSpeicalForm not exists
  /// @param name Function name
  /// @return FunctionCallToSpecialForm
  FunctionCallToSpecialForm* FOLLY_NULLABLE
  getSpecialForm(const std::string& name) const;

  std::vector<std::string> getSpecialFormNames() const;

 private:
  folly::Synchronized<RegistryType> registry_;
};

const SpecialFormRegistry& specialFormRegistry();

SpecialFormRegistry& mutableSpecialFormRegistry();

/// Registers 'functionCallToSpecialForm' for mapping user defined
/// FunctionCallToSpecialForm subclass instances to user-defined
/// FunctionCallToSpecialForm.
void registerFunctionCallToSpecialForm(
    const std::string& name,
    std::unique_ptr<FunctionCallToSpecialForm> functionCallToSpecialForm);

/// Removes all functionCallToSpecialForm registered earlier via calls to
/// 'registerFunctionCallToSpecialForm'.
void unregisterAllFunctionCallToSpecialForm();

/// Returns true if a FunctionCallToSpeicalForm object has been registered for
/// the given functionName.
bool isFunctionCallToSpecialFormRegistered(const std::string& functionName);

} // namespace facebook::velox::exec
