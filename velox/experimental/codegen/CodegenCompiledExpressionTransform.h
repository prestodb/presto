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

// This file defines the tranasformation that replaces velox expressions with
// codegen compiled expressions

#include <functional>
#include <optional>
#include "velox/core/PlanNode.h"
#include "velox/experimental/codegen/CompiledExpressionAnalysis.h"
#include "velox/experimental/codegen/code_generator/ExprCodeGenerator.h"
#include "velox/experimental/codegen/compiler_utils/CodeManager.h"
#include "velox/experimental/codegen/compiler_utils/ICompiledCall.h"
#include "velox/experimental/codegen/transform/PlanNodeTransform.h"
#include "velox/experimental/codegen/transform/utils/ranges_utils.h"
#include "velox/experimental/codegen/transform/utils/utils.h"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"
#include "velox/parse/Expressions.h"

namespace facebook {
namespace velox {
namespace codegen {
using namespace transform;

/// Code generator visitor
class CompiledExpressionTransformVisitor {
 public:
  CompiledExpressionTransformVisitor(
      const CompiledExpressionAnalysisResult& compiledExprAnalysisResult,
      const CompilerOptions& options,
      DefaultScopedTimer::EventSequence& eventSequence,
      bool compileFilter = true,
      bool mergeFilter = true)
      : codeManager_(options, eventSequence),
        compiledExprAnalysisResult_(compiledExprAnalysisResult),
        compileFilter_(compileFilter),
        mergeFilter_(mergeFilter) {}

  template <typename Children>
  std::shared_ptr<core::PlanNode> visit(
      const core::PlanNode& planNode,
      const Children& transformedChildren) {
    if (auto projectNode = dynamic_cast<const core::ProjectNode*>(&planNode)) {
      return visitProjection(*projectNode, transformedChildren);
    };

    if (auto filterNode = dynamic_cast<const core::FilterNode*>(&planNode)) {
      if (!compileFilter_ || mergeFilter_) {
        return utils::adapter::FilterCopy::copyWith(
            *filterNode,
            std::placeholders::_1,
            std::placeholders::_1,
            *ranges::begin(transformedChildren));
      }
      return visitFilter(*filterNode, transformedChildren);
    };

    if (auto valueNode = dynamic_cast<const core::ValuesNode*>(&planNode)) {
      return std::make_shared<core::ValuesNode>(
          valueNode->id(), valueNode->values());
    }

    if (auto tableWriteNode =
            dynamic_cast<const core::TableWriteNode*>(&planNode)) {
      // Return the same node with new children.
      return utils::adapter::TableWriteNodeCopy::copyWith(
          *tableWriteNode,
          std::placeholders::_1,
          std::placeholders::_1,
          std::placeholders::_1,
          std::placeholders::_1,
          std::placeholders::_1,
          std::placeholders::_1,
          *ranges::begin(transformedChildren));
    };

    if (auto tableScanNode =
            dynamic_cast<const core::TableScanNode*>(&planNode)) {
      // Return a copy.
      return utils::adapter::TableScanNodeCopy::copyWith(
          *tableScanNode,
          std::placeholders::_1,
          std::placeholders::_1,
          std::placeholders::_1,
          std::placeholders::_1);
    };

    throw std::logic_error("Unknown node type");
  }

 private:
  CodeManager codeManager_;

  const CompiledExpressionAnalysisResult& compiledExprAnalysisResult_;

  bool compileFilter_;
  bool mergeFilter_;

  std::optional<std::reference_wrapper<const GeneratedExpressionStruct>>
  getGeneratedCode(const std::shared_ptr<const ITypedExpr>& expression) {
    auto it = compiledExprAnalysisResult_.generatedCode_.find(expression);
    if (it == compiledExprAnalysisResult_.generatedCode_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  bool isDefaultNull(const core::PlanNodeId& planId) {
    auto it = compiledExprAnalysisResult_.isDefaultNull_.find(planId);
    if (it == compiledExprAnalysisResult_.isDefaultNull_.end()) {
      return false;
    }
    return it->second;
  }

  bool isDefaultNullStrict(const core::PlanNodeId& planId) {
    auto it = compiledExprAnalysisResult_.isDefaultNullStrict_.find(planId);
    if (it == compiledExprAnalysisResult_.isDefaultNullStrict_.end()) {
      return false;
    }
    return it->second;
  }

  const std::string& fileFormat() {
    // TODO: Move this into a file
    static const std::string fileFormat_ = R"(
    #include "velox/experimental/codegen/vector_function/VectorFunctionLoadUtils.h" // @manual
    {includes}

    using namespace facebook;
    using namespace facebook::velox;
    using namespace facebook::velox::codegen;

    {GeneratedCode}

    struct GeneratedVectorFunctionConfig {{
      using GeneratedCodeClass =  {GeneratedCodeClass};
      static constexpr bool isDefaultNull = {isDefaultNull};
      static constexpr bool isDefaultNullStrict = {isDefaultNullStrict};

    }};

    std::shared_ptr<FunctionTable> functionTable;

    std::unique_ptr<GeneratedVectorFunctionBase> newInstance() {{
          return std::make_unique<GeneratedVectorFunction<GeneratedVectorFunctionConfig>>();
    }};

    extern "C" {{
        bool init(void *) {{
          functionTable = std::make_shared<FunctionTable>();
          registerFunction("newInstance",&newInstance,*functionTable);
          return true;
        }}
        void release(void  * ) {{ }};
        std::shared_ptr<FunctionTable> getFunctionTable() {{return functionTable; }};
    }};
)";
    return fileFormat_;
  }

  /// Compute the union of a given set of RowTypes.
  /// eg ROW({'a','b,'c'}, {DOUBLETYPE,DOUBLETYPE,DOUBLETTYPE}) +
  ///  ROW({'a','d,}, {DOUBLETYPE,DOUBLETYPE})
  /// -> ROW({'a','b,'c','d'}, {DOUBLETYPE,DOUBLETYPE,DOUBLETYPE,DOUBLETTYPE})
  /// Throws when the union of type is inconsistent  (eg the same column exists
  /// with different types).
  /// TODO: use a range_view instead of a containers as arg
  /// \param types
  /// \return union type
  std::shared_ptr<RowType> computeUnionType(
      const std::vector<std::reference_wrapper<const RowType>>& rowTypes) {
    std::map<std::string, TypePtr> columnsTypes;

    for (auto& rowType_ : rowTypes) {
      auto& rowType = rowType_.get();
      for (size_t childId = 0; childId < rowType.size(); ++childId) {
        auto columnName = rowType.nameOf(childId);
        if (auto nameIterator = columnsTypes.find(columnName);
            nameIterator == columnsTypes.end()) {
          columnsTypes[columnName] = rowType.childAt(childId);
        } else {
          // If the column name has already been encountered, we check if the
          // type is the same we saw last time
          VELOX_CHECK_EQ(rowType.childAt(childId), (*nameIterator).second);
        }
      };
    }

    std::vector<std::string> names;
    std::vector<TypePtr> types;

    // TODO: We probably need here to have a better ordering of the columns
    for (auto& [name, type] : columnsTypes) {
      names.push_back(name);
      types.push_back(type);
    }
    return std::make_shared<RowType>(std::move(names), std::move(types));
  }

  /// Computes the input map of a given column give it's Rowtype
  /// eg if columnType = [ {"c",DOUBLE()}, {"a",DOUBLE()} ]
  /// and @concatanatedColumnType = [ {"a",DOUBLE()},
  /// {"b",DOUBLE()},{"c",DOUBLE()} ] return is "std::index_sequence<2,0>"
  std::string computeInputMap(
      const RowType& columnType,
      const RowType& concatanatedColumnType) {
    if (columnType.size() == 0) {
      return "std::index_sequence<>";
    }

    std::stringstream sstring;
    sstring << "std::index_sequence<"
            << concatanatedColumnType.getChildIdx(columnType.nameOf(0));

    for (size_t index = 1; index < columnType.size(); ++index) {
      sstring << ", "
              << concatanatedColumnType.getChildIdx(columnType.nameOf(index));
    };
    sstring << ">";
    return sstring.str();
  }

  /// Generate the cpp code for a given column.
  /// \param generatedCode
  /// \param unionType
  /// \param columnName
  /// \param outputIndex
  /// \param out
  /// \return the name of the final class
  std::string generateColumnCode(
      const GeneratedExpressionStruct& generatedCode,
      const RowType& unionType,
      const std::string& columnName,
      const size_t outputIndex,
      std::stringstream& out) {
    constexpr auto formatString = R"(
        {generatedStructCode}
        using {usingDeclName} = std::tuple<{className},{inputMap},
                  std::index_sequence<{outputIndex}>>;
    )";

    const std::string className = fmt::format("col_{}", columnName);
    const std::string usingDeclName = fmt::format("typeCol{}", columnName);

    const std::string inputMap =
        computeInputMap(generatedCode.inputRowType(), unionType);

    out << fmt::format(
               formatString,
               fmt::arg("className", className),
               fmt::arg("usingDeclName", usingDeclName),
               fmt::arg("inputMap", inputMap),
               fmt::arg(
                   "generatedStructCode", generatedCode.toStruct(className)),
               fmt::arg("outputIndex", outputIndex))
        << std::endl;

    return usingDeclName;
  }

  std::string generateConcatCode(
      const RowType& inputType,
      const RowType& outputType,
      const std::vector<std::string>& classNames,
      const std::string& mainClassName,
      const bool hasFilter) {
    const std::string inImplTypes =
        codegenUtils::codegenImplTypeName(inputType);
    const std::string outImplTypes =
        codegenUtils::codegenImplTypeName(outputType);

    std::stringstream decls;
    decls << classNames[0];

    for (size_t index = 1; index < classNames.size(); ++index) {
      decls << ", " << classNames[index];
    }

    return fmt::format(
        "using {} = facebook::velox::codegen::ConcatExpression<{},{},{},{}>;",
        mainClassName,
        hasFilter,
        inImplTypes,
        outImplTypes,
        decls.str());
  }

  /// Generate a templated instance of Concat Expression
  /// \param mainClassName
  /// \param outputType
  /// \param generatedColumns = [columnIndex,generatedCode] pairs
  /// \param newInputType
  /// \param newOutputType
  /// \return cpp code.
  std::string generateConcatExpr(
      const std::string& mainClassName,
      const RowType& outputType, // Used just for the column names
      std::optional<GeneratedExpressionStruct>& filterExpr,
      const std::vector<std::pair<size_t, GeneratedExpressionStruct>>&
          generatedColumns,
      std::shared_ptr<RowType>& newInputType,
      std::shared_ptr<RowType>& newOutputType) {
    VELOX_CHECK_GT(generatedColumns.size(), 0);

    std::stringstream sstring;

    std::vector<std::reference_wrapper<const RowType>> selectedInputTypes;

    if (filterExpr) {
      selectedInputTypes.push_back(std::ref(filterExpr->inputRowType()));
    }
    for (auto& [columnIndex, generatedCode] : generatedColumns) {
      selectedInputTypes.push_back(std::ref(generatedCode.inputRowType()));
    };

    /// RowType of the concatenated  expression
    newInputType = computeUnionType(selectedInputTypes);

    /// Generate class definitions for each supported columns
    size_t outputIndex = 0;
    std::vector<std::string> selectedOutputNames;
    std::vector<std::string> usingDeclNames;
    std::vector<TypePtr> selectedOutputTypes;

    if (filterExpr) {
      usingDeclNames.push_back(generateColumnCode(
          *filterExpr, *newInputType, "selected", outputIndex, sstring));
      sstring << std::endl;
    }
    for (const auto& [column, generatedCode] : generatedColumns) {
      const std::string columnName = outputType.nameOf(column);
      const std::string usingDeclName = generateColumnCode(
          generatedCode, *newInputType, columnName, outputIndex, sstring);
      sstring << std::endl;
      usingDeclNames.push_back(usingDeclName);
      outputIndex++;
      selectedOutputNames.push_back(columnName);
      selectedOutputTypes.push_back(outputType.childAt(column));
    };

    newOutputType = std::make_shared<RowType>(
        std::move(selectedOutputNames), std::move(selectedOutputTypes));

    // Concatenate all the expression into a single macro expression
    const std::string concatDeclaration = generateConcatCode(
        *newInputType,
        *newOutputType,
        usingDeclNames,
        mainClassName,
        filterExpr.has_value());

    sstring << std::endl << concatDeclaration << std::endl;
    return sstring.str();
  }

  /// Return FieldAccess expression for each Column in rowType
  /// eg [ROW({'a','b'}, {DOUBLETYPE,DOUBLETYPE} ->
  /// [FieldAcess(a),FieldAccess(b)])
  /// @param rowType
  /// @param input input expression to link the generated FieldAccess
  /// @return vector of FieldAccess, one for each column in rowType
  std::vector<std::shared_ptr<const core::ITypedExpr>> buildFieldAccessor(
      const RowType& rowType,
      std::shared_ptr<const core::ITypedExpr> input = nullptr) {
    std::vector<std::shared_ptr<const core::ITypedExpr>> fieldAccessVector;

    for (size_t childIdx = 0; childIdx < rowType.size(); ++childIdx) {
      auto name = rowType.nameOf(childIdx);
      auto type = rowType.childAt(childIdx);
      if (input) {
        auto fieldAccess = std::make_shared<const core::FieldAccessTypedExpr>(
            type, input, std::move(name));
        fieldAccessVector.push_back(fieldAccess);
      } else {
        auto fieldAccess = std::make_shared<const core::FieldAccessTypedExpr>(
            type, std::move(name));
        fieldAccessVector.push_back(fieldAccess);
      }
    };

    return fieldAccessVector;
  }

  /// Build the expressions trees using the generated code.
  /// In general, we would replace the set of expressions {a + b, a -b} :
  /// [ IcallExpr(+) -> FieldsAccess(b) -> InputExpr({a,b},{DOUBLE,DOUBLE})
  ///                -> FieldsAccess(a) /
  ///  IcallExpr(-) -> FieldsAccess(b) -> InputExpr({a,b},{DOUBLE,DOUBLE})
  ///               -> FieldsAccess(a) /
  /// ]
  /// into                    {PlusExpr,MinusExpr}
  /// [   -> FieldsAccess(c) -> CompiledEpr{c,d}  -> FieldsAccess(b) ->
  /// InputExpr({a,b},{DOUBLE,DOUBLE}) [   -> FieldsAccess(d) / \
  /// FieldsAccess(a) / \param dynamicObjectPath  compiled code path \param
  /// callOutputType compiled expression output row type \param callInputType
  /// compiled expression input  row type \param projectionInputType input type
  /// of the original projection \return
  std::vector<std::shared_ptr<const ITypedExpr>> buildCompiledCallExpr(
      const std::filesystem::path& dynamicObjectPath,
      const std::shared_ptr<const RowType>& callOutputType,
      const std::shared_ptr<const RowType>& callInputType,
      const std::shared_ptr<const RowType>& projectionInputType) {
    // Create the input FieldAccess expression node to the read input data
    // Note we could reuse the one already existing in the current expressions.
    auto inputFieldAccessVector = buildFieldAccessor(*callInputType);

    // Create ICompiledExpression
    auto compiledExpression = std::make_shared<codegen::ICompiledCall>(
        dynamicObjectPath, inputFieldAccessVector, callOutputType);

    // Create the field accessor to read the output of the compiled call
    auto outputFieldAccessVector =
        buildFieldAccessor(*callOutputType, compiledExpression);

    return outputFieldAccessVector;
  }

  template <typename Children>
  std::shared_ptr<core::PlanNode> visitFilter(
      const core::FilterNode& filter,
      const Children& children) {
    std::vector<std::pair<size_t, GeneratedExpressionStruct>> generatedColumns;
    generatedColumns.push_back({0, getGeneratedCode(filter.filter()).value()});
    if (generatedColumns[0].second.inputRowType().size() == 0) {
      // it's a constant expression, don't compile it
      return utils::adapter::FilterCopy::copyWith(
          filter,
          std::placeholders::_1,
          std::placeholders::_1,
          *ranges::begin(children));
    }

    VELOX_CHECK_EQ(filter.sources().size(), 1);

    const auto outputType = //*filter.outputType().get();
        *ROW({"selected"}, std::vector<TypePtr>{BOOLEAN()});

    std::shared_ptr<RowType> concatInputType;
    std::shared_ptr<RowType> concatOutputType;

    std::optional<GeneratedExpressionStruct> noFilter(std::nullopt);

    // when filter is compiled separately, it's treated similar to projection on
    // one boolean expr, so here no filter argument but instead has projection
    // argument
    const std::string genCode = generateConcatExpr(
        "FilterExpr",
        outputType,
        noFilter,
        generatedColumns,
        concatInputType,
        concatOutputType);

    std::stringstream includes;
    std::unordered_set<std::string> includeSet;
    for (const auto& [columnIndex, expressionStruct] : generatedColumns) {
      includeSet.insert(
          expressionStruct.headers().begin(), expressionStruct.headers().end());
    };

    for (const auto& includePath : includeSet) {
      includes << fmt::format("#include {}\n", includePath);
    };
    const std::string fileString = fmt::vformat(
        fileFormat(),
        fmt::make_format_args(
            fmt::arg("includes", includes.str()),
            fmt::arg("GeneratedCode", genCode),
            fmt::arg("GeneratedCodeClass", "FilterExpr"),
            fmt::arg(
                "isDefaultNull", isDefaultNull(filter.id()) ? "true" : "false"),
            fmt::arg(
                "isDefaultNullStrict",
                isDefaultNullStrict(filter.id()) ? "true" : "false")));
    auto compiledObject = codeManager_.compiler().compileString({}, fileString);
    auto dynamicObject = codeManager_.compiler().link({}, {compiledObject});

    // Extract the row input expression from the current filter
    const auto inputType = filter.sources()[0]->outputType();

    std::shared_ptr<const ITypedExpr> newFilter = buildCompiledCallExpr(
        dynamicObject, concatOutputType, concatInputType, inputType)[0];

    // Build new filter node with newly generated expressions
    return utils::adapter::FilterCopy::copyWith(
        filter, std::placeholders::_1, newFilter, *ranges::begin(children));
  }

  template <typename Children>
  std::shared_ptr<core::PlanNode> visitProjection(
      const core::ProjectNode& projection,
      const Children& children) {
    std::vector<std::pair<size_t, GeneratedExpressionStruct>> generatedColumns;

    std::shared_ptr<const core::PlanNode> source = *ranges::begin(children);

    // Check for filter
    std::optional<GeneratedExpressionStruct> filterExpr = std::nullopt;
    auto filterNode = std::dynamic_pointer_cast<const core::FilterNode>(source);
    if (compileFilter_ && mergeFilter_ && filterNode) {
      if (auto filterCode = getGeneratedCode(filterNode->filter())) {
        source = filterNode->sources()[0]; // change source to filter's source
        filterExpr.emplace(filterCode.value());
      }
    }

    // Collect all the columns with generated code.
    for (size_t outputColumn = 0;
         outputColumn < projection.projections().size();
         ++outputColumn) {
      // TODO: We should exclude here direct access columns.
      if (auto generatedCode =
              getGeneratedCode(projection.projections()[outputColumn])) {
        generatedColumns.push_back({outputColumn, generatedCode.value()});
      }
    }

    VELOX_CHECK_EQ(projection.sources().size(), 1);

    const auto& outputType = *projection.outputType().get();

    std::shared_ptr<RowType> concatInputType;
    std::shared_ptr<RowType> concatOutputType;

    const std::string genCode = generateConcatExpr(
        "ProjectExpr",
        outputType,
        filterExpr,
        generatedColumns,
        concatInputType,
        concatOutputType);

    if (concatInputType->size() == 0) {
      // the whole expression doesn't take input, don't compile it
      return utils::adapter::ProjectCopy::copyWith(
          projection,
          std::placeholders::_1,
          std::placeholders::_1,
          std::placeholders::_1,
          *ranges::begin(children));
    }

    std::stringstream includes;
    std::unordered_set<std::string> includeSet;
    for (const auto& [columnIndex, expressionStruct] : generatedColumns) {
      includeSet.insert(
          expressionStruct.headers().begin(), expressionStruct.headers().end());
    };

    for (const auto& includePath : includeSet) {
      includes << fmt::format("#include {}\n", includePath);
    };

    bool isDefaultNull, isDefaultNullStrict;
    if (filterExpr) {
      isDefaultNull = this->isDefaultNull(filterNode->id());
      isDefaultNullStrict = this->isDefaultNullStrict(filterNode->id());
    } else {
      isDefaultNull = this->isDefaultNull(projection.id());
      isDefaultNullStrict = this->isDefaultNullStrict(projection.id());
    }

    const std::string fileString = fmt::vformat(
        fileFormat(),
        fmt::make_format_args(
            fmt::arg("includes", includes.str()),
            fmt::arg("GeneratedCode", genCode),
            fmt::arg("GeneratedCodeClass", "ProjectExpr"),
            fmt::arg("isDefaultNull", isDefaultNull ? "true" : "false"),
            fmt::arg(
                "isDefaultNullStrict",
                isDefaultNullStrict ? "true" : "false")));

    auto compiledObject = codeManager_.compiler().compileString({}, fileString);
    auto dynamicObject = codeManager_.compiler().link({}, {compiledObject});
    std::vector<std::shared_ptr<const ITypedExpr>> newProjections;

    // Extract the row input expression from the current projection
    const auto inputType = projection.sources()[0]->outputType();

    std::vector<std::shared_ptr<const ITypedExpr>> newExpressions =
        buildCompiledCallExpr(
            dynamicObject, concatOutputType, concatInputType, inputType);

    // oldToNewExpressionColumnMap[Index] in the new projection list maps to
    // projection.projections()[Index] in the old;
    std::map<size_t, std::shared_ptr<const ITypedExpr>>
        oldToNewExpressionColumnMap;

    for (size_t newExpressionIdx = 0; newExpressionIdx < newExpressions.size();
         ++newExpressionIdx) {
      auto oldColumnIndex = generatedColumns[newExpressionIdx].first;
      oldToNewExpressionColumnMap[oldColumnIndex] =
          newExpressions[newExpressionIdx];
    }

    // Replace the selected columns with new generated expressions
    for (size_t outputColumn = 0;
         outputColumn < projection.projections().size();
         ++outputColumn) {
      if (auto expr = oldToNewExpressionColumnMap.find(outputColumn);
          expr != oldToNewExpressionColumnMap.end()) {
        newProjections.push_back(
            std::dynamic_pointer_cast<const ITypedExpr>(expr->second));
      } else {
        newProjections.push_back(projection.projections()[outputColumn]);
      }
    }
    // Build new projection node with newly generated expressions
    return utils::adapter::ProjectCopy::copyWith(
        projection,
        std::placeholders::_1,
        std::placeholders::_1,
        std::move(newProjections),
        source);
  }
};

union TransformFlags {
  struct {
    bool compileFilter : 1; // use codegen for filter expr

    // merge filter into projection
    // invalid if compileFilter not set
    bool mergeFilter : 1;

    bool enableDefaultNullOpt : 1; // enable default null optimization

    // use extended default null definition for filter
    // invalid if enableDefaultNullOpt not set
    bool enableFilterDefaultNull : 1;

    // up for more flags in the future
  };

  // make it easily comparable, may need to increase size as more flags added
  uint8_t flagVal;
};

class CodegenCompiledExpressionTransform final : PlanNodeTransform {
 public:
  constexpr static TransformFlags defaultFlags = {{1, 1, 1, 1}};
  CodegenCompiledExpressionTransform(
      const CompilerOptions& options,
      const UDFManager& udfManager,
      bool useSymbolsForArithmetic,
      NamedSteadyClockEventSequence& eventSequence,
      const TransformFlags& flags = defaultFlags)
      : compilerOptions_(options),
        udfManager_(udfManager),
        useSymbolsForArithmetic_(useSymbolsForArithmetic),
        eventSequence_(eventSequence),
        flags_(flags) {}

  std::shared_ptr<core::PlanNode> transform(
      const core::PlanNode& plan) override {
    DefaultScopedTimer timer(
        "CodegenCompiledExpressionTransform", eventSequence_);

    CompiledExpressionAnalysis expressionAnalysis(
        udfManager_,
        useSymbolsForArithmetic_,
        eventSequence_,
        flags_.enableDefaultNullOpt,
        flags_.enableFilterDefaultNull);

    expressionAnalysis.run(plan);

    CompiledExpressionTransformVisitor visitor(
        expressionAnalysis.results(),
        compilerOptions_,
        eventSequence_,
        flags_.compileFilter,
        flags_.mergeFilter);

    auto nodeTransformer = [&visitor](
                               auto& node, const auto& transformedChildren) {
      return visitor.visit(node, transformedChildren);
    };

    auto [treeRoot, nodeMap] =
        transform::utils::isomorphicTreeTransform(plan, nodeTransformer);
    return treeRoot;
  }

  void setTransformFlags(const TransformFlags& flags) {
    flags_ = flags;
  }

 private:
  CompilerOptions compilerOptions_;
  const UDFManager& udfManager_;
  bool useSymbolsForArithmetic_;
  NamedSteadyClockEventSequence& eventSequence_;
  TransformFlags flags_;
};

} // namespace codegen
} // namespace velox
} // namespace facebook
