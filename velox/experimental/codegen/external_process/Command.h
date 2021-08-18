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

#include <filesystem>
#include <string>
#include <vector>
#include "velox/experimental/codegen/external_process/ExternalProcessException.h"

namespace facebook::velox::codegen::external_process {

///
/// Represent an executable command line. This is essentially a list of
/// substrings with the first substring beeing the command line name
///
/// TODO 1: add reference to the working directory, and the search path.
struct Command {
  std::filesystem::path executablePath;
  std::vector<std::string> arguments;

  /// Mainly for logging and debugging
  /// \param sep separator
  /// \return String representation
  std::string toString(const std::string& sep = "\n") const {
    std::string result = executablePath.string();
    for (const auto& arg : arguments) {
      result += sep + arg;
    };
    return result;
  }

  /// Parse strings init a command object
  /// \param command
  /// \return command object
  /// TODO: Missing implementation
  static Command fromString(const std::string& command) {
    throw std::logic_error("Missing implementation");
  }
};
} // namespace facebook::velox::codegen::external_process
