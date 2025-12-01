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

#include "presto_cpp/main/tvf/spi/Descriptor.h"

#include "velox/common/serialization/DeserializationRegistry.h"
#include "velox/core/Expressions.h"

namespace facebook::presto::tvf {

class TableFunctionHandle : public velox::ISerializable {
 public:
  // This name is used for looking up the deserializer for the
  // TableFunctionHandle in the registry.
  virtual std::string_view name() const = 0;

  virtual folly::dynamic serialize() const = 0;
};

using TableFunctionHandlePtr = std::shared_ptr<const TableFunctionHandle>;

class TableSplitHandle : public velox::ISerializable {
 public:
  // This name is used for looking up the deserializer for the
  // TableSplitHandle in the registry.
  virtual std::string_view name() const = 0;

  virtual folly::dynamic serialize() const = 0;
};

using TableSplitHandlePtr = std::shared_ptr<const TableSplitHandle>;

using RequiredColumnsMap =
    std::unordered_map<std::string, std::vector<velox::column_index_t>>;

// TODO : Make this implement ISerializable as well ?
class TableFunctionAnalysis {
 public:
  TableFunctionAnalysis() {}

  const TableFunctionHandlePtr tableFunctionHandle() const {
    return tableFunctionHandle_;
  }

  const DescriptorPtr returnType() const {
    return returnType_;
  }

  const RequiredColumnsMap requiredColumns() const {
    return requiredColumns_;
  }

  // Add a builder so that these can be set outside.
  // protected:
  DescriptorPtr returnType_;
  TableFunctionHandlePtr tableFunctionHandle_;
  RequiredColumnsMap requiredColumns_;
};

} // namespace facebook::presto::tvf
