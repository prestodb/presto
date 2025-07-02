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

#include "presto_cpp/main/tvf/spi/Argument.h"

#include "velox/core/Expressions.h"

namespace facebook::presto::tvf {

class TableArgument : public Argument {
 public:
  TableArgument(velox::RowTypePtr type) : rowType_(std::move(type)) {}

  velox::RowTypePtr rowType() {
    return rowType_;
  }

 private:
  const velox::RowTypePtr rowType_;
};

class TableArgumentSpecification : public ArgumentSpecification {
 public:
  TableArgumentSpecification(std::string name, bool required)
      : ArgumentSpecification(name, required){};
};

} // namespace facebook::presto::tvf
