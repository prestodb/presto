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

#include "presto_cpp/main/tvf/spi/Descriptor.h"

namespace facebook::presto::tvf {

class ReturnTypeSpecification {
 public:
  enum class ReturnType { kGenericTable, kDescribedTable };

  ReturnTypeSpecification(ReturnType returnType) : returnType_(returnType){};

  ReturnType returnType() const {
    return returnType_;
  }

  virtual ~ReturnTypeSpecification() = default;

 private:
  ReturnType returnType_;
};
using ReturnSpecPtr = std::shared_ptr<ReturnTypeSpecification>;

class GenericTableReturnType : public ReturnTypeSpecification {
 public:
  GenericTableReturnType()
      : ReturnTypeSpecification(
            ReturnTypeSpecification::ReturnType::kGenericTable){};
};

class DescribedTableReturnType : public ReturnTypeSpecification {
 public:
  DescribedTableReturnType(DescriptorPtr descriptor)
      : ReturnTypeSpecification(
            ReturnTypeSpecification::ReturnType::kDescribedTable),
        descriptor_(std::move(descriptor)) {}

  const DescriptorPtr descriptor() const {
    return descriptor_;
  }

 private:
  DescriptorPtr descriptor_;
};

} // namespace facebook::presto::tvf
