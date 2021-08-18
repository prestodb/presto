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

/// This file define the UDFManager and some related classes. UDFManager is used
/// to register UDFs and other related information that are needed for
/// translating VELOX expression to Codegen UDFCallExpr, and also information
/// needed for codegen to correctly call the functions.

#pragma once
#include <map>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "velox/experimental/codegen/compiler_utils/LibraryDescriptor.h"
#include "velox/experimental/codegen/udf_manager/ExpressionNullMode.h"
#include "velox/type/Type.h"

namespace facebook {
namespace velox {
namespace codegen {

enum class OutputForm { Return, InOut };

// Information for single UDF function.
class UDFInformation {
 public:
  UDFInformation() {}

  // Make sure mandatory fields are set
  void validate(bool veloxNamesMustBeSet = true) const {
    if (veloxNamesMustBeSet && veloxFunctionNames_ == std::nullopt) {
      throw std::invalid_argument("UDF veloxFunctionNames_ field not set");
    }

    if (calledFunctionName_ == std::nullopt) {
      throw std::invalid_argument("UDF calledFunctionName_ field not set");
    }

    if (nullMode_ == std::nullopt) {
      throw std::invalid_argument("UDF nullMode_ field not set");
    }

    if (isOptionalArguments_ == std::nullopt) {
      throw std::invalid_argument("UDF isOptionalArguments_ field not set");
    }

    if (isOptionalOutput_ == std::nullopt) {
      throw std::invalid_argument("UDF isOptionalOutput_ field not set");
    }

    if (outputForm_ == std::nullopt) {
      throw std::invalid_argument("UDF outputForm_ field not set");
    }

    if (codegenNullInNullOutChecks_ == std::nullopt) {
      throw std::invalid_argument(
          "UDF codegenNullInNullOutChecks_ field not set");
    }
  }

  UDFInformation& setVeloxFunctionNames(
      const std::unordered_set<std::string>& veloxFunctionNames) {
    veloxFunctionNames_ = veloxFunctionNames;
    return *this;
  }

  std::unordered_set<std::string> getVeloxFunctionNames() const {
    return veloxFunctionNames_.value();
  }

  UDFInformation& setCalledFunctionName(const std::string& calledFunctionName) {
    calledFunctionName_ = calledFunctionName;
    return *this;
  }

  std::string getCalledFunctionName() const {
    return calledFunctionName_.value();
  }

  void setHeaderFiles(const std::vector<std::string>& headerFiles) {
    headerFiles_ = headerFiles;
  }

  std::vector<std::string> getHeaderFiles() const {
    return headerFiles_;
  }

  bool hasHeaderFiles() const {
    return !headerFiles_.empty();
  }

  void setLibs(const std::vector<compiler_utils::LibraryDescriptor>& libs) {
    libs_ = libs;
  }

  std::vector<compiler_utils::LibraryDescriptor> getLibs() const {
    return libs_;
  }

  bool hasLibs() const {
    return !libs_.empty();
  }

  void setNullMode(ExpressionNullMode expressionNullMode) {
    nullMode_ = expressionNullMode;
  }

  ExpressionNullMode getNullMode() const {
    return nullMode_.value();
  }

  void setOutputForm(OutputForm outputForm) {
    outputForm_ = outputForm;
  }

  OutputForm getOutputForm() const {
    return outputForm_.value();
  }

  bool isOptionalOutput() const {
    return isOptionalOutput_.value();
  }

  bool isOptionalArguments() const {
    return isOptionalArguments_.value();
  }

  void setIsOptionalOutput(bool isOptionalOutput) {
    isOptionalOutput_ = isOptionalOutput;
  }

  void setIsOptionalArguments(bool isOptionalArguments) {
    isOptionalArguments_ = isOptionalArguments;
  }

  void setCodegenNullInNullOutChecks(bool codegenNullInNullOutChecks) {
    codegenNullInNullOutChecks_ = codegenNullInNullOutChecks;
  }

  bool codegenNullInNullOutChecks() const {
    return codegenNullInNullOutChecks_.value();
  }

  bool lookUpByName() const {
    return !argumentsLookupTypes_.has_value();
  }

  void setArgumentsLookupTypes(
      const std::vector<velox::TypeKind>& argumentsTypes) {
    argumentsLookupTypes_ = argumentsTypes;
  }

  std::vector<velox::TypeKind> getArgumentsLookupTypes() const {
    return argumentsLookupTypes_.value();
  }

  UDFInformation withVeloxFunctionNames(
      const std::unordered_set<std::string>& veloxFunctionNames) {
    veloxFunctionNames_ = veloxFunctionNames;
    return *this;
  }

  UDFInformation withCalledFunctionName(const std::string& calledFunctionName) {
    calledFunctionName_ = calledFunctionName;
    return *this;
  }

  UDFInformation withHeaderFiles(const std::vector<std::string>& headerFiles) {
    headerFiles_ = headerFiles;
    return *this;
  }

  UDFInformation withLibs(
      const std::vector<compiler_utils::LibraryDescriptor>& libs) {
    libs_ = libs;
    return *this;
  }

  UDFInformation withNullMode(ExpressionNullMode expressionNullMode) {
    nullMode_ = expressionNullMode;
    return *this;
  }

  UDFInformation withOutputForm(OutputForm outputForm) {
    outputForm_ = outputForm;
    return *this;
  }

  UDFInformation withIsOptionalOutput(bool isOptionalOutput) {
    isOptionalOutput_ = isOptionalOutput;
    return *this;
  }

  UDFInformation withIsOptionalArguments(bool isOptionalArguments) {
    isOptionalArguments_ = isOptionalArguments;
    return *this;
  }

  UDFInformation withCodegenNullInNullOutChecks(
      bool codegenNullInNullOutChecks) {
    codegenNullInNullOutChecks_ = codegenNullInNullOutChecks;
    return *this;
  }

  UDFInformation withArgumentsLookupTypes(
      const std::vector<velox::TypeKind>& argumentsTypes) {
    argumentsLookupTypes_ = argumentsTypes;
    return *this;
  }

 private:
  std::optional<std::unordered_set<std::string>> veloxFunctionNames_;

  std::optional<std::string> calledFunctionName_;

  std::vector<std::string> headerFiles_;

  std::vector<compiler_utils::LibraryDescriptor> libs_;

  std::optional<ExpressionNullMode> nullMode_;

  std::optional<bool> codegenNullInNullOutChecks_;

  // If not set the function will be looked up by name
  std::optional<std::vector<velox::TypeKind>> argumentsLookupTypes_;

  std::optional<bool> isOptionalOutput_;

  std::optional<bool> isOptionalArguments_;

  std::optional<OutputForm> outputForm_;
};

// Manage registered UDFs. Thread safe.
class UDFManager {
 public:
  // Add entry to udfDictionary.
  void registerUDF(const UDFInformation& udfInformation);

  // Extract UDF information from a functionName and a list of arguments types.
  std::optional<UDFInformation> getUDFInformationTypedArgs(
      const std::string& functionName,
      const std::vector<velox::TypeKind>& argumentTypes) const;

  // Extract UDF information using a functionName only. Must have been
  // registered with name only lookup.
  std::optional<UDFInformation> getUDFInformationUnTypedArgs(
      const std::string& functionName) const;

 private:
  // Map (functionName, {argumentsTypes}) -> udf information.
  using TypedLookupKey = std::pair<std::string, std::vector<velox::TypeKind>>;

  std::map<TypedLookupKey, UDFInformation> udfDictionaryWithArguments;

  // Map (functionName) -> udf information.
  std::unordered_map<std::string, UDFInformation> udfDictionaryNameOnly;

  // Mutex for reading/writing UDF dictionary
  mutable std::shared_mutex udfDictionaryWithArgumentsMutex;
  mutable std::shared_mutex udfDictionaryNameOnlyMutex;
};

void registerVeloxArithmeticUDFs(UDFManager& udfManager);

void registerVeloxStringFunctions(UDFManager& udfManager);

UDFInformation getCastUDFBase();
UDFInformation getCastUDF(velox::TypeKind kind, bool castByTruncate);

} // namespace codegen
} // namespace velox
} // namespace facebook
