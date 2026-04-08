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

#include "velox/common/serialization/DeserializationRegistry.h"
#include "velox/core/Expressions.h"

namespace facebook::presto::tvf {

/// The TableFunctionHandle is returned by the TableFunction::analyze method
/// It can be used to carry all information necessary to
/// execute the table function which is gathered at analysis time. Typically,
/// these are the values of the constant arguments, and results of
/// pre-processing arguments.
class TableFunctionHandle : public velox::ISerializable {
 public:
  // This name is used for looking up the deserializer for the
  // TableFunctionHandle in the registry.
  virtual std::string_view name() const = 0;

  virtual folly::dynamic serialize() const = 0;
};

using TableFunctionHandlePtr = std::shared_ptr<const TableFunctionHandle>;

/// The TableSplitHandle is returned by the TableFunction::getSplits() method.
/// It can be used to carry all information necessary to execute a split of the
/// table function, which is gathered at split generation time. Typically,
/// these are the values of the constant arguments or some results of
/// pre-processing from the TableFunctionHandle.
class TableSplitHandle : public velox::ISerializable {
 public:
  // This name is used for looking up the deserializer for the
  // TableSplitHandle in the registry.
  virtual std::string_view name() const = 0;

  virtual folly::dynamic serialize() const = 0;
};

using TableSplitHandlePtr = std::shared_ptr<const TableSplitHandle>;

using RequiredColumnsMap =
    std::vector<std::pair<std::string, std::vector<velox::column_index_t>>>;

/// This class is produced by the `analyze()` method of a `TableFunction`
/// implementation. The 'analyze()' method is invoked during SQL Statement
/// Analysis of the function.
///
/// The `returnedType` field is used to inform the Analyzer of the proper
/// columns returned by the Table Function, that is, the columns produced
/// by the function, as opposed to the columns passed from the input tables.
/// The `returnedType` should only be set if the declared returned type
/// is GENERIC_TABLE.
///
/// The `handle` field can be used to carry all information necessary to
/// execute the table function, gathered at analysis time. Typically, these are
/// the values of the constant arguments, and results of pre-processing
/// arguments.
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

  DescriptorPtr returnType_;
  TableFunctionHandlePtr tableFunctionHandle_;
  RequiredColumnsMap requiredColumns_;
};

} // namespace facebook::presto::tvf
