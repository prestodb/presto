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

#include "presto_cpp/main/tvf/spi/Argument.h"

#include "velox/core/Expressions.h"

namespace facebook::presto::tvf {

class ScalarArgument : public Argument {
 public:
  ScalarArgument(velox::TypePtr type, velox::VectorPtr value)
      : type_(std::move(type)), constantValue_(std::move(value)) {}

  const velox::TypePtr rowType() const {
    return type_;
  }

  const velox::VectorPtr value() const {
    return constantValue_;
  }

 private:
  const velox::TypePtr type_;
  const velox::VectorPtr constantValue_;
};

class ScalarArgumentSpecification : public ArgumentSpecification {
 public:
  ScalarArgumentSpecification(
      std::string name,
      velox::TypePtr type,
      bool required)
      : ArgumentSpecification(name, required), type_(std::move(type)){};

  const velox::TypePtr rowType() const {
    return type_;
  }

 private:
  const velox::TypePtr type_;
};

} // namespace facebook::presto::tvf
