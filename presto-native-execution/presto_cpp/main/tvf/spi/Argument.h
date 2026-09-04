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

#include "velox/core/Expressions.h"

namespace facebook::presto::tvf {

/**
 * This class represents the three types of arguments passed to a
 * Table Function: scalar arguments, descriptor arguments, and table arguments.
 */
class Argument {
 public:
  Argument() = default;

  virtual ~Argument() = default;

 protected:
};

/// The argument specification defines the name of the argument, whether the
/// argument is required, and the default value for SQL scalar arguments.
/// Each argument is named, and either passed positionally or in a
/// `arg_name` => value` convention.
class ArgumentSpecification {
 public:
  ArgumentSpecification(
      std::string name,
      bool required,
      std::string defaultValue = "")
      : name_(std::move(name)),
        required_(required),
        defaultValue_(std::move(defaultValue)) {};

  virtual ~ArgumentSpecification() = default;

  const std::string& name() const {
    return name_;
  }

  bool required() const {
    return required_;
  }

  const std::string& defaultValue() const {
    return defaultValue_;
  }

 private:
  const std::string name_;
  const bool required_;
  const std::string defaultValue_;
};

} // namespace facebook::presto::tvf
