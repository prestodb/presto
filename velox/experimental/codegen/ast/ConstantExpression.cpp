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

#include "velox/experimental/codegen/ast/ConstantExpressions.h"
namespace facebook::velox::codegen {

template <>
std::vector<std::string> ConstantExpression<std::string>::getHeaderFiles()
    const {
  return {"<cstring>"};
}

template <>
CodeSnippet ConstantExpression<std::string>::generateCode(
    CodegenCtx& exprCodegenCtx,
    const std::string& outputVarName) const {
  exprCodegenCtx.addHeaderPaths(getHeaderFiles());

  std::string code;
  if (isNull()) {
    code = codegenUtils::codegenAssignment(outputVarName, "std::nullopt");
  } else {
    // TODO: here constants strings are copied only when assigned to
    // Reader::PointerType.
    // An optimization for the future, is to allocate them on a buffer of the
    // same type as the one used by Velox and then we wont have to recopy them
    // per row!
    auto constVariable = exprCodegenCtx.addConstantStringDecl(
        codegenValue<std::string>(value_.value()));
    auto rhs = fmt::format(
        "{0} {{codegen::ConstantString{{{1}}} }}",
        codegenUtils::codegenNullableNativeType(
            TypeKind::VARCHAR, codegenUtils::TypePlacement::Constant),
        constVariable);
    code = codegenUtils::codegenAssignment(outputVarName, rhs);
  }

  return CodeSnippet(outputVarName, code);
}
} // namespace facebook::velox::codegen
