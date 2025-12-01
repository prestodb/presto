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

#include "velox/core/Expressions.h"

namespace facebook::presto::tvf {

class Argument {
 public:
  Argument() {}

  virtual ~Argument() = default;

 protected:
};

class ArgumentSpecification {
 public:
  ArgumentSpecification(std::string name, bool required)
      : name_(std::move(name)), required_(required){};

  virtual ~ArgumentSpecification() = default;

  const std::string& name() const {
    return name_;
  }

  const bool required() const {
    return required_;
  }

 private:
  const std::string name_;
  const bool required_;
  // TODO : Add default value.
};

} // namespace facebook::presto::tvf
