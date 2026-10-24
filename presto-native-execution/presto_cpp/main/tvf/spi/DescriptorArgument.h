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
#include "presto_cpp/main/tvf/spi/Descriptor.h"

#include "velox/core/Expressions.h"

namespace facebook::presto::tvf {

class DescriptorArgument : public Argument {
 public:
 public:
  DescriptorArgument(DescriptorPtr descriptor)
      : descriptor_(std::move(descriptor)) {};

  const Descriptor descriptor() const {
    return *descriptor_;
  }

 private:
  const DescriptorPtr descriptor_;
};

using DescriptorArgumentPtr = std::shared_ptr<const DescriptorArgument>;

class DescriptorArgumentSpecification : public ArgumentSpecification {
 public:
  DescriptorArgumentSpecification(
      std::string name,
      DescriptorArgumentPtr descriptorArgument,
      bool required)
      : ArgumentSpecification(name, required),
        descriptorArgument_(descriptorArgument) {};

  const Descriptor descriptor() const {
    return descriptorArgument_->descriptor();
  }

 private:
  const DescriptorArgumentPtr descriptorArgument_;
};

} // namespace facebook::presto::tvf
