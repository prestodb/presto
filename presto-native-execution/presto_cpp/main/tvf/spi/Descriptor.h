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

class Descriptor : public Argument {
 public:
  Descriptor(std::vector<std::string> names)
      : type_(DescriptorType::kOnlyName), names_(std::move(names)) {}

  Descriptor(std::vector<std::string> names, std::vector<velox::TypePtr> types)
      : type_(DescriptorType::kNameType),
        names_(std::move(names)),
        types_(std::move(types)) {
    VELOX_CHECK_EQ(names_.size(), types_.size());
    fields_.reserve(names_.size());
    for (velox::column_index_t i = 0; i < names_.size(); i++) {
      fields_.push_back(std::make_shared<velox::core::FieldAccessTypedExpr>(
          types_.at(i), names_.at(i)));
    }
  }

  const std::vector<velox::core::FieldAccessTypedExprPtr> fields() const {
    VELOX_CHECK(type_ == DescriptorType::kNameType);
    return fields_;
  }

  const std::vector<std::string> names() const {
    return names_;
  }

  const std::vector<velox::TypePtr> types() const {
    return types_;
  }

 private:
  enum class DescriptorType {
    kOnlyName,
    kNameType,
  };
  DescriptorType type_;

  std::vector<std::string> names_;
  std::vector<velox::TypePtr> types_;

  std::vector<velox::core::FieldAccessTypedExprPtr> fields_;
};

class DescriptorArgumentSpecification : public ArgumentSpecification {
 public:
  DescriptorArgumentSpecification(
      std::string name,
      Descriptor descriptor_,
      bool required)
      : ArgumentSpecification(name, required),
        descriptor_(std::move(descriptor_)){};

  const Descriptor descriptor() const {
    return descriptor_;
  }

 private:
  const Descriptor descriptor_;
};

using DescriptorPtr = std::shared_ptr<const Descriptor>;
} // namespace facebook::presto::tvf
