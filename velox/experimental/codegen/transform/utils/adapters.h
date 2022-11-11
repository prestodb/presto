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

#include <velox/experimental/codegen/transform/utils/ranges_utils.h>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>
#include "velox/core/PlanNode.h"

namespace facebook {
namespace velox {
namespace core {
// Adapter function so core::PlanNode respect the Tree concept
static auto children(const core::PlanNode& node) {
  // TODO:  this assume that nodePtr is never null, we need to add a constraints
  auto getNodes = [](auto& nodePtr) -> auto& {
    return *nodePtr;
  };

  auto filterNullptr = [](auto& nodePtr) -> bool { return nodePtr != nullptr; };
  return node.sources() /*| ranges::views::filter(filterNullptr) */ |
      ranges::views::transform(getNodes);
};

// Adapter function so core::ITypedExpr respect the Tree concept
static auto children(const core::ITypedExpr& node) {
  auto getNodes = [](auto& inputPtr) -> auto& {
    return *inputPtr;
  };

  auto filterNullptr = [](auto& inputPtr) -> bool {
    return inputPtr != nullptr;
  };

  return node.inputs() /*| ranges::views::filter(filterNullptr)*/ |
      ranges::views::transform(getNodes);
};

} // namespace core

namespace transform {
namespace utils {
namespace adapter {

// Adapter function so core::PlanNode respect the Tree concept
static auto children(const core::PlanNode& node) {
  // TODO:  this assume that nodePtr is never null, we need to add a constraints
  auto getNodes = [](auto& nodePtr) -> auto& {
    return *nodePtr;
  };

  auto filterNullptr = [](auto& nodePtr) -> bool { return nodePtr != nullptr; };

  return node.sources() /*| ranges::views::filter(filterNullptr)*/ |
      ranges::views::transform(getNodes);
};

// Adapter function so core::ITypedExpr respect the Tree concept
static auto children(const core::ITypedExpr& node) {
  // TODO:  this assume that nodePtr is never null, we need to add a constraints
  auto getNodes = [](auto& inputPtr) -> auto& {
    return *inputPtr;
  };

  auto filterNullptr = [](auto& nodePtr) -> bool { return nodePtr != nullptr; };

  return node.inputs() /*| ranges::views::filter(filterNullptr)*/ |
      ranges::views::transform(getNodes);
};

/**
 * Most of the tree nodes (PlanNode and IExpr) are immutable. This makes
 * implementing tree transformations somewhat verbose. This helper classes
 * simplifies the frequent case where we want to copy a given node and change
 * some(but not all) of it fields.
 *
 * In general this class is not meant to be used directly, and custom
 * specializations are provided bellow for the most important types
 * (core::PlanNode etc... etc...). Example : Given a class A {field1, field2,
 * field3,...} NodeCopy<A>::copyWith(a, newField1, _2,newField3) =  {
 * newField1,field2,newField3}
 *
 * Constraints (TODO : define formaly those constraints in term of c++ concepts)
 *
 * @tparam T
 * @tparam ValueReader
 */
template <typename CopyType, typename ValueReader>
struct NodeCopy {
  using ArgsType =
      std::invoke_result<decltype(&ValueReader::operator()), CopyType>;

  template <typename... Args>
  static std::shared_ptr<CopyType> copyWith(const CopyType& t, Args&&... args) {
    auto oldValues = ValueReader()(t);
    auto newValues = std::forward_as_tuple(std::forward<Args>(args)...);
    auto newArguments = copyWithHelper(
        std::forward<decltype(oldValues)>(oldValues),
        std::forward<decltype(newValues)>(newValues),
        std::index_sequence_for<Args...>{});
    // return std::apply(std::make_shared<T>,newArguments); for some reason
    // doesn't work
    return std::apply(
        [](auto&&... args_) {
          return std::make_shared<CopyType>(
              std::forward<decltype(args_)>(args_)...);
        },
        std::forward<decltype(newArguments)>(newArguments));
  }

 private:
  template <typename OldVals, typename NewVals, size_t... I>
  static auto copyWithHelper(
      OldVals&& oldVals,
      NewVals&& newVals,
      const std::index_sequence<I...>& indices) {
    // Returns oldVal if newVal is a placeHolder, oldVal otherwise
    // This is use to select which value to preserve between the original
    // object and the copy, and which value to replace.
    auto selectValue = [](auto&& oldVal, auto&& newVal) -> auto&& {
      if constexpr (std::is_placeholder_v<std::decay_t<decltype(newVal)>>) {
        return std::forward<decltype(oldVal)>(oldVal);
      } else {
        return std::forward<decltype(newVal)>(newVal);
      }
    };

    return std::forward_as_tuple(selectValue(
        std::get<I>(std::forward<OldVals>(oldVals)),
        std::get<I>(std::forward<NewVals>(newVals)))...);
  }
};

// Specializations

// TODO: probably can introduce a macro here.
struct ProjectNodeReader {
  auto operator()(const core::ProjectNode& project) {
    return std::forward_as_tuple(
        project.id(),
        project.names(),
        project.projections(),
        project.sources()[0]);
  }
};

using ProjectCopy = facebook::velox::transform::utils::adapter::
    NodeCopy<core::ProjectNode, ProjectNodeReader>;

struct FilterNodeReader {
  auto operator()(const core::FilterNode& filter) {
    return std::forward_as_tuple(
        filter.id(), filter.filter(), filter.sources()[0]);
  }
};

using FilterCopy = facebook::velox::transform::utils::adapter::
    NodeCopy<core::FilterNode, FilterNodeReader>;

struct TableWriteNodeReader {
  auto operator()(const core::TableWriteNode& node) {
    return std::forward_as_tuple(
        node.id(),
        node.columns(),
        node.columnNames(),
        node.insertTableHandle(),
        node.outputType(),
        node.commitStrategy(),
        node.sources()[0]);
  }
};

using TableWriteNodeCopy = facebook::velox::transform::utils::adapter::
    NodeCopy<core::TableWriteNode, TableWriteNodeReader>;

struct TableScanNodeReader {
  auto operator()(const core::TableScanNode& node) {
    return std::forward_as_tuple(
        node.id(), node.outputType(), node.tableHandle(), node.assignments());
  }
};

using TableScanNodeCopy = facebook::velox::transform::utils::adapter::
    NodeCopy<core::TableScanNode, TableScanNodeReader>;

struct ValuesNodeNodeReader {
  auto operator()(const core::ValuesNode& values) {
    return std::forward_as_tuple(
        values.id(), values.values(), values.isParallelizable());
  }
};

using ValuesCopy = facebook::velox::transform::utils::adapter::
    NodeCopy<core::ValuesNode, ValuesNodeNodeReader>;

} // namespace adapter
} // namespace utils

} // namespace transform
} // namespace velox
} // namespace facebook
