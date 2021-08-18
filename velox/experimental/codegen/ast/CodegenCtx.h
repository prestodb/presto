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
#include <fmt/format.h>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>
#include "velox/experimental/codegen/compiler_utils/LibraryDescriptor.h"
#include "velox/type/Type.h"

namespace facebook::velox::codegen {

/// A context that holds global information needed during and after code
/// generation.
///
/// TODO: Since we are now tracking inputs on a per expression basis,
/// this class should probably not be store in the code generator.
class CodegenCtx {
 public:
  /// Return a temp variable name that is unique within the context
  std::string getUniqueTemp(const std::string& prefix = "") {
    return fmt::format("{}_tmp_{}", prefix, getUniqueId());
  }

  std::string getUniqueLambda() {
    return fmt::format("lambda_{}", getUniqueId());
  }

  size_t getUniqueId() {
    return nextId_++;
  }

  void addHeaderPath(const std::string& headerPath) {
    headers_.insert(headerPath);
  }

  void addHeaderPaths(const std::vector<std::string>& headers) {
    for (auto& header : headers) {
      headers_.insert(header);
    }
  }

  void addLib(const compiler_utils::LibraryDescriptor& lib) {
    libs_.insert(lib);
  }

  void addLibs(const std::vector<compiler_utils::LibraryDescriptor>& libs) {
    for (auto& lib : libs) {
      addLib(lib);
    }
  }

  std::set<compiler_utils::LibraryDescriptor> getLibs() const {
    return libs_;
  }

  std::vector<compiler_utils::LibraryDescriptor> getLibsAsList() const {
    return {libs_.begin(), libs_.end()};
  }

  const std::unordered_set<std::string>& headers() const {
    return headers_;
  }

  std::map<std::string, std::pair<size_t, facebook::velox::TypePtr>>&
  columnNameToIndexMap() {
    return columnNameToIndexMap_;
  }

  bool computeInputTypeDuringCodegen() const {
    return computeInputType_;
  }

  void setComputeInputTypeDuringCodegen(bool flag) {
    computeInputType_ = flag;
  }

  size_t computeIndexForColumn(
      const std::string& columnName,
      const velox::TypePtr& type) {
    if (auto indexIterator = columnNameToIndexMap().find(columnName);
        indexIterator != columnNameToIndexMap().end()) {
      return indexIterator->second.first;
    }

    // If this is the first time we encounter this column name, assign
    // a new index value
    auto nextIndex = columnNameToIndexMap().size();
    columnNameToIndexMap()[columnName] = {nextIndex, type};
    return nextIndex;
  }

  std::string addConstantStringDecl(const std::string& string) {
    auto varName = fmt::format("string_constant_{}", getUniqueId());
    constantDeclarations.push_back(fmt::format(
        "const StringView {}= StringView(\"{}\")", varName, string));
    return varName;
  }

  const std::vector<std::string>& declarations() const {
    return constantDeclarations;
  }

 private:
  void addConstantDeclaration(const std::string& decl) {
    constantDeclarations.push_back(decl);
  }

  /// A storage for constant declarations that will be added to the generated
  /// expression structure as member
  std::vector<std::string> constantDeclarations;

  /// Counter that tracks unique ids and assign them to variables
  size_t nextId_ = 0;
  /// Header files required to execute the expressions generated
  std::unordered_set<std::string> headers_ = {"<optional>"};

  /// Libs required to execute the expressions generated
  std::set<compiler_utils::LibraryDescriptor> libs_;

  /// Mapping between InputRef names and index in the input tuple
  /// [columnName -> {index,velox::type}
  std::map<std::string, std::pair<size_t, facebook::velox::TypePtr>>
      columnNameToIndexMap_;

  /// Flag to decide how to compute the input indexes.
  /// When true, we assign increasing index number to each column as we
  /// discover them in the current expression
  bool computeInputType_ = false;
};
} // namespace facebook::velox::codegen
