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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sstream>
#include <stdexcept>
#include "velox/experimental/codegen/ast/CodegenUtils.h"
#include "velox/experimental/codegen/code_generator/ExprCodeGenerator.h"
#include "velox/experimental/codegen/compiler_utils/CodeManager.h"
#include "velox/experimental/codegen/compiler_utils/CompilerOptions.h"
#include "velox/experimental/codegen/compiler_utils/tests/definitions.h"
#include "velox/experimental/codegen/utils/resources/ResourcePath.h"
#include "velox/experimental/codegen/vector_function/StringTypes.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::codegen {
namespace expressions::test {

template <TypeKind kind>
struct CodegenTestTypeTraits {
  using ReaderType = std::optional<typename TypeTraits<kind>::NativeType>;
  using WriterType = std::optional<typename TypeTraits<kind>::NativeType>;
  using ViewType = std::optional<typename TypeTraits<kind>::NativeType>;
  using VeloxType = typename TypeTraits<kind>::NativeType;
  static WriterType createWriter() {
    return WriterType{std::in_place};
  }
};

template <>
struct CodegenTestTypeTraits<TypeKind::VARCHAR> {
  // Simple object with string writer interface
  using WriterType = TempStringNullable<TempsAllocator>;
  using ReaderType = InputReferenceStringNullable;
  using ViewType = std::optional<StringView>;
  using VeloxType = StringView;
  static WriterType createWriter() {
    static TempsAllocator allocator;
    return WriterType{std::in_place, allocator};
  }
};

template <TypeKind... T>
struct RowTypeTrait {
  // The output tuple should be of this type
  using CodegenTupleWritersType =
      std::tuple<typename CodegenTestTypeTraits<T>::WriterType...>;

  // The input tuple should be of this type
  using CodegenTupleReadersType =
      std::tuple<typename CodegenTestTypeTraits<T>::ReaderType...>;

  // The compared types in the tests are from this
  using ViewTupleType =
      std::tuple<typename CodegenTestTypeTraits<T>::ViewType...>;

  // The types in velox flat vectors.
  using VeloxTypes =
      std::tuple<typename CodegenTestTypeTraits<T>::VeloxType...>;

  static constexpr TypeKind typeKinds[] = {T...};

  static std::string codegenReadersType() {
    std::stringstream ss;
    ss << "std::tuple<";

    for (int i = 0; i < rowWidth(); i++) {
      if (i != 0) {
        ss << ",";
      }
      ss << codegenUtils::codegenNullableNativeType(
          typeKinds[i], codegenUtils::TypePlacement::Input);
    }
    ss << ">";
    return ss.str();
  }

  static CodegenTupleWritersType createWritersTuple() {
    return std::tuple<typename CodegenTestTypeTraits<T>::WriterType...>{
        CodegenTestTypeTraits<T>::createWriter()...};
  }

  static std::string codegenWritersType() {
    std::stringstream ss;
    ss << "std::tuple<";
    for (int i = 0; i < rowWidth(); i++) {
      if (i != 0) {
        ss << ",";
      }

      ss << codegenUtils::codegenNullableNativeType(
          typeKinds[i], codegenUtils::TypePlacement::Temp);
    }
    ss << ">";
    return ss.str();
  }

  static CodegenTupleReadersType convertViewTupleToInput(
      const ViewTupleType& viewTuple) {
    CodegenTupleReadersType out;
    convertViewTupleToInput_<0>(viewTuple, out);
    return out;
  }

  template <int index>
  static void convertViewTupleToInput_(
      const ViewTupleType& viewTuple,
      CodegenTupleReadersType& out) {
    if constexpr (index == sizeof...(T)) {
      return;
    } else {
      if constexpr (std::is_same<
                        TempStringNullable<TempsAllocator>,
                        typename std::tuple_element<
                            index,
                            CodegenTupleWritersType>::type>::value) {
        if (std::get<index>(viewTuple).has_value()) {
          std::get<index>(out) = InputReferenceStringNullable{
              InputReferenceString{*std::get<index>(viewTuple)}};
        } else {
          std::get<index>(out) = std::nullopt;
        }
      } else {
        std::get<index>(out) = std::get<index>(viewTuple);
      }
      convertViewTupleToInput_<index + 1>(viewTuple, out);
    }
  }

  static ViewTupleType convertWritersTupleToViewTuple(
      const CodegenTupleWritersType& writers) {
    ViewTupleType out;
    convertWritersTupleToViewTuple_<0>(writers, out);
    return out;
  }

  template <int index>
  static void convertWritersTupleToViewTuple_(
      const CodegenTupleWritersType& writers,
      ViewTupleType& out) {
    if constexpr (index == sizeof...(T)) {
      return;

    } else {
      if constexpr (std::is_same<
                        TempStringNullable<TempsAllocator>,
                        typename std::tuple_element<
                            index,
                            CodegenTupleWritersType>::type>::value) {
        if (std::get<index>(writers).has_value()) {
          auto& tempString = *std::get<index>(writers);
          std::get<index>(out) = {
              StringView(tempString.data(), tempString.size())};

        } else {
          std::get<index>(out) = std::nullopt;
        }
      } else {
        std::get<index>(out) = std::get<index>(writers);
      }
      convertWritersTupleToViewTuple_<index + 1>(writers, out);
    }
  }

  static std::vector<std::shared_ptr<const Type>> veloxDynamicTypes() {
    return {(ScalarType<T>::create())...};
  }

  static size_t rowWidth() {
    return sizeof...(T);
  }

  // Convert array of codegen tuple rows to array of velox flat vectors
  static auto getViewTupleRowsAsFlatVectors(
      const std::vector<ViewTupleType>& tupleRows,
      core::ExecCtx* execCtx_) {
    auto vectors = std::vector<VectorPtr>{BaseVector::create(
        CppToType<typename TypeTraits<T>::NativeType>::create(),
        tupleRows.size(),
        execCtx_->pool())...};

    fillVectorsData<0>(tupleRows, vectors);
    return vectors;
  }

  // Convert array of velox flat vectors to array of tuple rows
  static std::vector<ViewTupleType> getVectorsAsViewTupleRows(
      const std::vector<VectorPtr>& columns) {
    if (columns.size() == 0) {
      return {};
    }

    std::vector<ViewTupleType> resultTuples;
    resultTuples.resize(columns[0]->size());
    fillTuplesData<0>(columns, resultTuples);
    return resultTuples;
  }

  // Convert a RowVector to array of tuples
  static std::vector<ViewTupleType> getBaseVectorsAsViewTupleRows(
      const BaseVector* vector,
      const SelectivityVector& rows) {
    // Regular Row vector
    if (auto rowVector = vector->as<RowVector>()) {
      return getVectorsAsViewTupleRows(rowVector->children());
    };

    // Constant vector of a complex (RowVector)
    if (auto constantVector = vector->as<ConstantVector<ComplexType>>();
        constantVector != nullptr) {
      if (auto rowVector = constantVector->valueVector()->as<RowVector>()) {
        auto columns = rowVector->children();
        if (columns.size() == 0) {
          return {};
        }

        std::vector<ViewTupleType> resultTuples;
        resultTuples.resize(rows.size());
        fillTuplesDataFromFoldedConstantInput<0>(
            columns, resultTuples, rows.size());
        return resultTuples;
      };
    };
    throw std::logic_error("Unsupported");
  }

 private:
  template <int index>
  static void fillVectorsData(
      const std::vector<ViewTupleType>& tupleRows,
      std::vector<VectorPtr>& columns) {
    if constexpr (index == sizeof...(T)) {
      return;
    } else {
      auto* vector =
          columns[index]
              ->template asFlatVector<typename std::tuple_element<
                  index,
                  VeloxTypes>::type>(); // view types matches FlatVector
                                        // native types of Velox for now

      for (int i = 0; i < tupleRows.size(); i++) {
        if (std::get<index>(tupleRows[i]).has_value()) {
          vector->set(i, std::get<index>(tupleRows[i]).value());
        } else {
          vector->setNull(i, true);
        }
      }
      fillVectorsData<index + 1>(tupleRows, columns);
    }
  }

  template <int index>
  static void fillTuplesData(
      const std::vector<VectorPtr>& columns,
      std::vector<ViewTupleType>& tupleRows) {
    if constexpr (index == sizeof...(T)) {
      return;
    } else {
      auto* vector = columns[index]
                         ->template as<SimpleVector<typename std::tuple_element<
                             index,
                             VeloxTypes>::type>>(); // view types matches
                                                    // SimpleVector native
                                                    // types of Velox for now

      for (auto i = 0; i < vector->size(); i++) {
        if (vector->isNullAt(i)) {
          std::get<index>(tupleRows[i]) = std::nullopt;
        } else {
          std::get<index>(tupleRows[i]) = vector->valueAt(i);
        }
      }
      fillTuplesData<index + 1>(columns, tupleRows);
    }
  }

  template <int index>
  static void fillTuplesDataFromFoldedConstantInput(
      const std::vector<VectorPtr>& columns,
      std::vector<ViewTupleType>& tupleRows,
      size_t size) {
    if constexpr (index != sizeof...(T)) {
      auto* vector = columns[index]
                         ->template as<SimpleVector<typename std::tuple_element<
                             index,
                             VeloxTypes>::type>>(); // view types matches
                                                    // SimpleVector native
                                                    // types of Velox for now

      for (auto i = 0; i < size; i++) {
        if (vector->isNullAt(0)) {
          std::get<index>(tupleRows[i]) = std::nullopt;
        } else {
          std::get<index>(tupleRows[i]) = vector->valueAt(0);
        }
      }
      fillTuplesDataFromFoldedConstantInput<index + 1>(
          columns, tupleRows, size);
    }
  }
};

// A class with utilities for testing expression codegen
class ExpressionCodegenTestBase : public testing::Test {
 public:
  // Given an expression, assert that the nullability of the elements of the
  // output row matches the expected nullability
  template <typename InputRowTypeTrait>
  void checkNullPropagation(
      const std::string& sqlExpression,
      std::vector<bool> inputRowNullability,
      std::vector<bool> expectedOutputNullability) {
    // Create velox row type
    auto inputRowType = makeRowType(InputRowTypeTrait::veloxDynamicTypes());

    // TODO convert to VELOX_ASSERT
    VELOX_CHECK(inputRowNullability.size() == InputRowTypeTrait::rowWidth());

    // Parse expression and convert it to velox typed expression
    auto typedConcatExpr = makeConcatTypedExpr(sqlExpression, inputRowType);

    auto codegenExprTree = std::dynamic_pointer_cast<MakeRowExpression>(
        generator_->convertVeloxExpressionToCodegenAST(
            typedConcatExpr, *inputRowType.get(), inputRowNullability));

    codegenExprTree->propagateNullability();
    auto idx = 0;
    for (auto child : codegenExprTree->children()) {
      ASSERT_EQ(child->maybeNull(), expectedOutputNullability[idx]);
      idx++;
    }
  }

  template <typename FuncType>
  FuncType compileAndGetFunction(
      const std::string& cppFileContent,
      const std::string& invokeFunctionName,
      bool printCode) {
    auto& compiler = codeManager_->compiler();
    auto compiledObjectPath = compiler.compileString(
        generator_->getContext().getLibsAsList(), cppFileContent);

    auto dynamicObjectPath = compiler.link(
        generator_->getContext().getLibsAsList(), {compiledObjectPath});

    if (printCode) {
      // TODO re-enable this option
    }

    auto& loader = codeManager_->loader();
    auto* dynamicLibPtr = loader.uncheckedLoadLibrary(dynamicObjectPath);
    auto* function = loader.uncheckedGetFunction<FuncType>(
        invokeFunctionName, dynamicLibPtr);
    return function;
  }

  /**
   * An interface that can be used to perform e2e testing for expression
   * codegen. InputRowTypeTrait and OutputRowTypeTrait are implemented using
   * RowTypeTrait.
   * @param sqlExpression input sql expression
   * @param inputTestRows vector of native input tuples
   * @param expectedOutputRows vector of native expected output tuples, if not
   * empty it will be used to verify the results of codegen.
   * @param printCode print the generated code to std::cerr
   * @param inputRowNullability provides information about the nullability of
   * the input schema.
   * @param compareWithVeloxOutput weather to compare with Velox
   * interpretation engine output
   **/
  template <typename InputRowTypeTrait, typename OutputRowTypeTrait>
  void evaluateAndCompare(
      const std::string& sqlExpression,
      const std::vector<typename InputRowTypeTrait::ViewTupleType>&
          inputTestRows,
      const std::vector<typename OutputRowTypeTrait::ViewTupleType>&
          expectedOutputRows = {},
      bool printCode = false,
      std::vector<bool> inputRowNullability =
          std::vector<bool>(InputRowTypeTrait::rowWidth(), true),
      bool compareWithVeloxOutput = true) {
    // Create velox row type
    auto inputRowType = makeRowType(InputRowTypeTrait::veloxDynamicTypes());

    // Parse expression and convert it to velox typed expression
    auto typedConcatExpr = makeConcatTypedExpr(sqlExpression, inputRowType);

    // Validate types of the parsed expression to match expected output types
    auto outputRowType =
        std::dynamic_pointer_cast<const RowType>(typedConcatExpr->type());
    auto expectedOutputTypes = OutputRowTypeTrait::veloxDynamicTypes();

    for (auto i = 0; i < expectedOutputTypes.size(); i++) {
      ASSERT_EQ(
          expectedOutputTypes[i]->kind(), outputRowType->children()[i]->kind());
    }

    // Codegen expression

    auto generatedExpression = generator_->codegenExpressionStructFromVeloxExpr(
        typedConcatExpr, *inputRowType.get(), inputRowNullability);

    // Codegen expression
    generator_->getContext().addHeaderPath(
        "\"velox/experimental/codegen/vector_function/StringTypes.h\"");
    generator_->getContext().addHeaderPath("\"velox/type/StringView.h\"");
    generator_->getContext().addHeaderPath("<tuple>");
    std::string cppFileContent;
    std::string invokeFunctionName;
    std::tie(invokeFunctionName, cppFileContent) =
        generatedExpression.wrapAsExternCallable(
            generator_->getContext(),
            InputRowTypeTrait::codegenReadersType(),
            OutputRowTypeTrait::codegenWritersType());

    using codegenFunctionType = void (*)(
        const typename InputRowTypeTrait::CodegenTupleReadersType&,
        typename OutputRowTypeTrait::CodegenTupleWritersType&);

    auto* function = compileAndGetFunction<codegenFunctionType>(
        cppFileContent, invokeFunctionName, printCode);

    // Compare with expected output if provided
    if (expectedOutputRows.size() != 0) {
      for (auto i = 0; i < inputTestRows.size(); i++) {
        auto output = OutputRowTypeTrait::createWritersTuple();
        auto input =
            InputRowTypeTrait::convertViewTupleToInput(inputTestRows[i]);
        function(input, output);
        auto outView =
            OutputRowTypeTrait::convertWritersTupleToViewTuple(output);
        ASSERT_EQ(outView, expectedOutputRows[i])
            << "failing test when compare with expected output case index is:"
            << i;
      }
    }

    // Compare with Velox output
    if (compareWithVeloxOutput) {
      // Execute Velox path
      facebook::velox::exec::ExprSet exprSet({typedConcatExpr}, execCtx_.get());
      auto rows = std::make_unique<SelectivityVector>(inputTestRows.size());

      auto columns = InputRowTypeTrait::getViewTupleRowsAsFlatVectors(
          inputTestRows, execCtx_.get());
      auto rowVector = makeRowVector(columns, inputTestRows.size());

      facebook::velox::exec::EvalCtx evalCtx(
          execCtx_.get(), &exprSet, rowVector.get());
      std::vector<VectorPtr> result(1);
      exprSet.eval(*rows, &evalCtx, &result);
      auto resultsRows = OutputRowTypeTrait::getBaseVectorsAsViewTupleRows(
          result[0].get(), *rows);

      // Compare
      for (auto i = 0; i < inputTestRows.size(); i++) {
        auto output = OutputRowTypeTrait::createWritersTuple();
        auto input =
            InputRowTypeTrait::convertViewTupleToInput(inputTestRows[i]);
        function(input, output);
        auto outView =
            OutputRowTypeTrait::convertWritersTupleToViewTuple(output);
        ASSERT_EQ(outView, resultsRows[i])
            << "failing test when compare with Velox case index is:" << i;
      }
    }
  }

  bool useBuiltInForArithmetic = false;

  UDFManager udfManager;

  virtual void SetUp() override {
    codeManager_ =
        std::make_unique<CodeManager>(getCompilerOptions(), eventSequence_);
    generator_ = std::make_unique<ExprCodeGenerator>(
        udfManager, useBuiltInForArithmetic, false);

    // Register velox functions since parsing expression depends on them
    functions::prestosql::registerArithmeticFunctions();

    // Register type resolver with the SQL parser.
    parse::registerTypeResolver();
  }

 protected:
  ExprCodeGenerator& getExprCodeGenerator() {
    return *generator_;
  }

  core::ExecCtx& getExecContext() {
    return *execCtx_;
  }

  // Create a row with names of cols C1, C2, C3 ..etc
  std::shared_ptr<const RowType> makeRowType(
      std::vector<std::shared_ptr<const Type>>&& types) {
    std::vector<std::string> names;
    for (int32_t i = 0; i < types.size(); ++i) {
      names.push_back(fmt::format("c{}", i));
    }
    return ROW(std::move(names), std::move(types));
  }

 private:
  compiler_utils::CompilerOptions getCompilerOptions() {
    std::string packageJson = ResourcePath::getResourcePath();

    LOG(INFO) << std::string(packageJson) << std::endl;
    auto defaultCompilerOptions =
        compiler_utils::CompilerOptions::fromJsonFile(packageJson);
    return defaultCompilerOptions;
  }

  RowVectorPtr makeRowVector(
      const std::vector<VectorPtr>& children,
      size_t size) {
    std::vector<std::shared_ptr<const Type>> childTypes;
    childTypes.resize(children.size());
    for (int i = 0; i < children.size(); i++) {
      childTypes[i] = children[i]->type();
    }
    auto rowType = makeRowType(std::move(childTypes));

    return std::make_shared<RowVector>(
        execCtx_->pool(), rowType, BufferPtr(nullptr), size, children);
  }

  // Takes a string expression as input and returns the ITypeExpression with
  // guaranteed top level concatTypedExpr.
  std::shared_ptr<const core::ConcatTypedExpr> makeConcatTypedExpr(
      const std::string& text,
      const std::shared_ptr<const RowType>& inputRowType) {
    auto untypedExpr = parse::parseExpr(text, options_);
    auto typedExpr = core::Expressions::inferTypes(
        untypedExpr, inputRowType, execCtx_->pool());

    if (auto callExpr =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(typedExpr)) {
      if (callExpr->name() == "row_constructor") {
        std::vector<std::string> names;
        for (auto i = 0; i < typedExpr->inputs().size(); i++) {
          names.push_back(fmt::format("c{}", names.size()));
        }
        return std::make_shared<core::ConcatTypedExpr>(
            core::ConcatTypedExpr(names, typedExpr->inputs()));
      }
    }

    if (auto concatTypedExpr =
            std::dynamic_pointer_cast<const core::ConcatTypedExpr>(typedExpr)) {
      return concatTypedExpr;
    }

    // add a concatTypedExpr on the top of the expression
    return std::make_shared<const core::ConcatTypedExpr>(
        core::ConcatTypedExpr({"c0"}, {typedExpr}));
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<facebook::velox::core::ExecCtx>(
          pool_.get(),
          queryCtx_.get())};
  std::unique_ptr<CodeManager> codeManager_;
  std::unique_ptr<ExprCodeGenerator> generator_;
  DefaultScopedTimer::EventSequence eventSequence_;
  parse::ParseOptions options_;
};

} // namespace expressions::test
} // namespace facebook::velox::codegen
