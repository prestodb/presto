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

#include <fmt/format.h>
#include <exception>
#include <string>

namespace facebook::velox::codegen {
/// An exception that is thrown during codegen if an expression/data type that
/// is not supported in code gen is encountered.
class CodegenNotSupported : public std::runtime_error {
 public:
  explicit CodegenNotSupported(const std::string& message)
      : std::runtime_error(
            fmt::format("CodegenNotSupported exception : {}", message)
                .c_str()) {}
};

/// An exception thrown by ASTNode::validate() when the validation does not
/// succeed.
class ASTValidationException : public std::runtime_error {
 public:
  explicit ASTValidationException(const std::string& message)
      : std::runtime_error(
            fmt::format("ASTNodeValidationException exception : {}", message)
                .c_str()) {}
};

/// An exception that is thrown if symbols in CodegenStubs.h is accidentally
/// linked in.
class CodegenStubsException : public std::runtime_error {
 public:
  explicit CodegenStubsException(const std::string& message)
      : std::runtime_error(
            fmt::format("CodegenStubsException exception : {}", message)
                .c_str()) {}
};

/// An exception that is thrown if codegen initialization failed
class CodegenInitializationException : public std::runtime_error {
 public:
  explicit CodegenInitializationException(const std::string& message)
      : std::runtime_error(fmt::format(
                               "CodegenInitializationException exception : {}",
                               message)
                               .c_str()) {}
};

} // namespace facebook::velox::codegen
