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
#include <exception>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include "velox/experimental/codegen/CodegenExceptions.h"
#include "velox/type/Type.h"

namespace facebook::velox::codegen::codegenUtils {

/// TODO: Why is this in an anon namespace
namespace {

enum TypePlacement { Input, Constant, Temp };

inline std::string codegenFixedWidthNativeType(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return "bool";
    case TypeKind::TINYINT:
      return "int8_t";
    case TypeKind::SMALLINT:
      return "int16_t";
    case TypeKind::INTEGER:
      return "int32_t";
    case TypeKind::BIGINT:
      return "int64_t";
    case TypeKind::REAL:
      return "float";
    case TypeKind::DOUBLE:
      return "double";
    default:
      VELOX_UNREACHABLE();
  }
}

inline std::string codegenStringNullableNativeType(
    TypePlacement typePlacement) {
  if (typePlacement == TypePlacement::Input) {
    return "facebook::velox::codegen::InputReferenceStringNullable";
  }
  if (typePlacement == TypePlacement::Constant) {
    return "facebook::velox::codegen::ConstantStringNullable";
  }
  if (typePlacement == TypePlacement::Temp) {
    return "facebook::velox::codegen::TempStringNullable<codegen::TempsAllocator>";
  }
  VELOX_UNREACHABLE();
}

/// Codegen the corresponding nullable native type
inline std::string codegenNullableNativeType(
    const velox::TypeKind& typeKind,
    TypePlacement typePlacement) {
  if (typeKind == TypeKind::VARCHAR) {
    return codegenStringNullableNativeType(typePlacement);
  }

  // will throw if not supported
  return fmt::format(
      "std::optional<{}>", codegenFixedWidthNativeType(typeKind));
}

/// Codegen the corresponding nullable type
inline std::string codegenNullableNativeType(
    const velox::Type& type,
    TypePlacement typePlacement) {
  return codegenNullableNativeType(type.kind(), typePlacement);
}

/// Convert Type to implType (static types)
inline std::string codegenImplTypeName(const velox::Type& type) {
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
      return fmt::format("ScalarType<TypeKind::{}>", type.kindName());
    case TypeKind::ROW: {
      std::stringstream ss;
      ss << "std::tuple<"
         << ((type.size() > 0) ? codegenImplTypeName(*type.childAt(0).get())
                               : "");
      for (size_t index = 1; index < type.size(); ++index) {
        ss << ", " << codegenImplTypeName(*type.childAt(index).get());
      };
      ss << ">";
      return ss.str();
    }
    default:
      throw CodegenNotSupported(fmt::format(
          "ImplType of {} Kind not supported in code-gen", type.kindName()));
  };
};

/// Convert Type to implType (static types) tuples
inline std::string codegenImplTypeName(
    const std::vector<std::reference_wrapper<const velox::Type>>& types) {
  std::stringstream ss;
  ss << "std::tuple<"
     << (types.size() > 0 ? codegenImplTypeName(types[0]) : "");
  for (size_t index = 1; index < types.size(); ++index) {
    ss << ", " << codegenImplTypeName(types[index]);
  };
  ss << ">";
  return ss.str();
}

/// Codegen the corresponding native type
// TODO: change this to be controlled in a header i.e ret CodegenType<type,
// mode>
inline std::string codegenNativeType(
    const velox::Type& type,
    TypePlacement typePlacement) {
  if (type.isFixedWidth()) {
    return codegenFixedWidthNativeType(type.kind());
  }

  // ROW type
  if (auto rowType = dynamic_cast<const velox::RowType*>(&type)) {
    std::stringstream outputType;
    outputType << "std::tuple<";
    bool first = true;
    for (auto child : rowType->children()) {
      if (!first) {
        outputType << ",";
      }
      first = false;
      outputType << codegenNullableNativeType(*child.get(), typePlacement);
    }
    outputType << ">";
    return outputType.str();
  }

  throw CodegenNotSupported(fmt::format(
      "ImplType of {} Kind not supported in code-gen as not optional",
      type.kindName()));
}

inline std::string codegenNativeType(
    const std::vector<std::reference_wrapper<const velox::Type>>& types) {
  std::stringstream ss;
  ss << "std::tuple<"
     << (types.size() > 0 ? codegenNativeType(types[0], TypePlacement::Input)
                          : "");
  for (size_t index = 1; index < types.size(); ++index) {
    ss << ", " << codegenNativeType(types[index], TypePlacement::Input);
  };
  ss << ">";
  return ss.str();
}

/// Codegen assignment
template <typename T, typename B>
std::string codegenAssignment(const T& lhs, const B& rhs) {
  return fmt::format("{} = {};", lhs, rhs);
}

/// Codegen if statement
inline std::string codegenIfStatement(
    const std::string& condition,
    const std::string& thenPart,
    const std::string& elsePart = "") {
  return fmt::format(
      R""""(
     if ({}) {{
       {}
     }} else{{
       {}
     }})"""",
      condition,
      thenPart,
      elsePart);
}

/// Codegen a function call
inline std::string codegenFunctionCall(
    const std::string& functionName,
    const std::vector<std::string>& args) {
  std::stringstream argList;
  for (int i = 0; i < args.size(); i++) {
    argList << ((i == 0) ? "" : ",") << args[i];
  }
  return fmt::format("{}({})", functionName, argList.str());
}

/// Codegen a binary symbol operator application on optional arguments
inline std::string codegenBinarySymbolCall(
    const std::string& symbol,
    const std::string& lhs,
    const std::string& rhs) {
  return fmt::format("*{} {} *{}", lhs, symbol, rhs);
}

/// Codegen a unary symbol operator application on optional argument
inline std::string codegenUnarySymbolCall(
    const std::string& symbol,
    const std::string& child) {
  return fmt::format("{} *{}", symbol, child);
}

/// Codegen code to access the value of the optional
inline std::string codegenOptionalAccess(
    const std::string& optionalVariableName) {
  return fmt::format("(*{}) ", optionalVariableName);
}

/// Codegen code to check if optional has value
inline std::string codegenOptionalValueCheck(
    const std::string& optionalVariableName) {
  return fmt::format("{}.has_value()", optionalVariableName);
}

inline std::string codegenHasValueCheck(
    const std::string& variable,
    bool maybeNull) {
  return maybeNull ? fmt::format("{}.has_value()", variable) : "true";
}

/// Codegen code that execute workCodeSnippet if non of the nullableArgs is
/// null, otherwise assigns outputVar to std::nullopt
inline std::string codegenNullInNullOut(
    const std::string& workCodeSnippet,
    const std::vector<std::string>& nullableArgs,
    const std::string& outputVar) {
  if (nullableArgs.size() == 0) {
    return workCodeSnippet;
  }

  std::stringstream ifCondition;
  ifCondition << "true";
  for (int i = 0; i < nullableArgs.size(); i++) {
    ifCondition << " && " << codegenOptionalValueCheck(nullableArgs[i]);
  }

  return codegenUtils::codegenIfStatement(
      ifCondition.str(),
      workCodeSnippet,
      codegenAssignment(outputVar, "std::nullopt"));
}

/// Codegen variable declaration, initialized by default constructor (For now)
inline std::string codegenTempVarDecl(
    const velox::Type& type,
    const std::string& variableName) {
  VELOX_CHECK(type.isPrimitiveType());

  auto args = std::string("std::in_place") +
      (type.isFixedWidth() ? "" : ", state.allocator");

  return fmt::format(
      "{0} {1} ({2});", // initialize optional
      codegenUtils::codegenNullableNativeType(type, TypePlacement::Temp),
      variableName,
      args);
}

/// Codegen variable declaration with initial value
inline std::string codegenVarDecl(
    [[maybe_unused]] const velox::Type& type,
    const std::string& variableName,
    const std::string& initialValue,
    bool constantVar = false) {
  return fmt::format(
      "{} {} {} = {};",
      constantVar ? "const" : "",
      "auto",
      variableName,
      initialValue);
}

/// Codegen variable declaration with initial value
inline std::string codegenRefVarDecl(
    [[maybe_unused]] const velox::Type& type,
    const std::string& variableName,
    const std::string& initialValue) {
  return fmt::format("auto {} = {};", variableName, initialValue);
}

/// Codegen tuple access
inline std::string codegenTupleAccess(
    const std::string& variableName,
    size_t index) {
  return fmt::format("std::get<{}>({})", index, variableName);
}

/// Codegen code to write data, and size in output string writer
inline std::string codegenStringWriteFromCharPtr(
    const std::string& stringWriterVariable,
    const std::string& charPtrVariable) {
  return fmt::format(
      "({0}).resize(strlen({1}));"
      "std::memcpy(({0}).data(), {1}, strlen({1}));",
      codegenOptionalAccess(stringWriterVariable),
      charPtrVariable);
}

} // namespace
} // namespace facebook::velox::codegen::codegenUtils
