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

#include <cstdint>
#include <exception>
#include <fstream>
#include <stdexcept>
#include <vector>
#include "glog/logging.h"
#include "gtest/gtest.h"

#include "velox/expression/Expr.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/CoalesceExpr.h"
#include "velox/expression/ConjunctExpr.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/SwitchExpr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/tests/TestingAlwaysThrowsFunction.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {
namespace {
class ExprTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType,
      const VectorPtr& complexConstants = nullptr) {
    auto untyped = parse::parseExpr(text, options_);
    return core::Expressions::inferTypes(
        untyped, rowType, execCtx_->pool(), complexConstants);
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType,
      const std::vector<TypePtr>& lambdaInputTypes) {
    auto untyped = parse::parseExpr(text, options_);
    return core::Expressions::inferTypes(
        untyped, rowType, lambdaInputTypes, execCtx_->pool(), nullptr);
  }

  std::vector<core::TypedExprPtr> parseMultipleExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::parseMultipleExpressions(text, options_);
    std::vector<core::TypedExprPtr> parsed;
    for (auto& iExpr : untyped) {
      parsed.push_back(
          core::Expressions::inferTypes(iExpr, rowType, execCtx_->pool()));
    }
    return parsed;
  }

  // T can be ExprSet or ExprSetSimplified.
  template <typename T = exec::ExprSet>
  std::unique_ptr<T> compileExpression(const core::TypedExprPtr& expr) {
    std::vector<core::TypedExprPtr> expressions = {expr};
    return std::make_unique<T>(std::move(expressions), execCtx_.get());
  }

  // T can be ExprSet or ExprSetSimplified.
  template <typename T = exec::ExprSet>
  std::unique_ptr<T> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType,
      const VectorPtr& complexConstants = nullptr) {
    auto parsedExpression = parseExpression(expr, rowType, complexConstants);
    return compileExpression<T>(parsedExpression);
  }

  std::unique_ptr<exec::ExprSet> compileNoConstantFolding(
      const std::string& sql,
      const RowTypePtr& rowType,
      const VectorPtr& complexConstants = nullptr) {
    auto expression = parseExpression(sql, rowType, complexConstants);
    return compileNoConstantFolding(expression);
  }

  std::unique_ptr<exec::ExprSet> compileNoConstantFolding(
      const core::TypedExprPtr& expression) {
    std::vector<core::TypedExprPtr> expressions = {expression};
    return std::make_unique<exec::ExprSet>(
        std::move(expressions),
        execCtx_.get(),
        false /*enableConstantFolding*/);
  }

  std::unique_ptr<exec::ExprSet> compileMultiple(
      const std::vector<std::string>& texts,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions;
    expressions.reserve(texts.size());
    for (const auto& text : texts) {
      expressions.emplace_back(parseExpression(text, rowType));
    }
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  // Utility method to compile multiple expressions expected in a single sql
  // text.
  std::unique_ptr<exec::ExprSet> compileMultipleExprs(
      const std::string& text,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions =
        parseMultipleExpression(text, rowType);
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  std::vector<VectorPtr> evaluateMultiple(
      const std::vector<std::string>& texts,
      const RowVectorPtr& input,
      const std::optional<SelectivityVector>& rows = std::nullopt,
      core::ExecCtx* execCtx = nullptr) {
    auto exprSet = compileMultiple(texts, asRowType(input->type()));

    exec::EvalCtx context(
        execCtx ? execCtx : execCtx_.get(), exprSet.get(), input.get());
    std::vector<VectorPtr> result(texts.size());
    if (rows.has_value()) {
      exprSet->eval(*rows, context, result);
    } else {
      exprSet->eval(SelectivityVector{input->size()}, context, result);
    }
    return result;
  }

  std::pair<
      std::vector<VectorPtr>,
      std::unordered_map<std::string, exec::ExprStats>>
  evaluateMultipleWithStats(
      const std::vector<std::string>& texts,
      const RowVectorPtr& input,
      std::vector<VectorPtr> resultToReuse = {}) {
    auto exprSet = compileMultiple(texts, asRowType(input->type()));

    exec::EvalCtx context(execCtx_.get(), exprSet.get(), input.get());

    SelectivityVector rows(input->size());
    if (resultToReuse.empty()) {
      resultToReuse.resize(texts.size());
    }
    exprSet->eval(rows, context, resultToReuse);
    return {resultToReuse, exprSet->stats()};
  }

  VectorPtr evaluate(const std::string& text, const RowVectorPtr& input) {
    return evaluateMultiple({text}, input)[0];
  }

  std::pair<VectorPtr, std::unordered_map<std::string, exec::ExprStats>>
  evaluateWithStats(const std::string& expression, const RowVectorPtr& input) {
    auto exprSet = compileExpression(expression, asRowType(input->type()));
    return evaluateWithStats(exprSet.get(), input);
  }

  std::pair<VectorPtr, std::unordered_map<std::string, exec::ExprStats>>
  evaluateWithStats(exec::ExprSet* exprSetPtr, const RowVectorPtr& input) {
    SelectivityVector rows(input->size());
    std::vector<VectorPtr> results(1);

    exec::EvalCtx context(execCtx_.get(), exprSetPtr, input.get());
    exprSetPtr->eval(rows, context, results);

    return {results[0], exprSetPtr->stats()};
  }

  template <
      typename T = exec::ExprSet,
      typename = std::enable_if_t<
          std::is_same_v<T, exec::ExprSet> ||
              std::is_same_v<T, exec::ExprSetSimplified>,
          bool>>
  VectorPtr evaluate(T* exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(execCtx_.get(), exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet->eval(rows, context, result);
    return result[0];
  }

  template <typename T = exec::ExprSet>
  void evalWithEmptyRows(
      const std::string& expr,
      const RowVectorPtr& input,
      VectorPtr& result,
      const VectorPtr& expected) {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(expr, options);
    auto typedExpr = core::Expressions::inferTypes(
        untyped, asRowType(input->type()), pool());

    SelectivityVector rows{input->size(), false};
    T exprSet({typedExpr}, execCtx_.get());
    exec::EvalCtx evalCtx(execCtx_.get(), &exprSet, input.get());
    std::vector<VectorPtr> results{result};
    exprSet.eval(rows, evalCtx, results);
    assertEqualVectors(result, expected);
  }

  template <typename T = ComplexType>
  std::shared_ptr<core::ConstantTypedExpr> makeConstantExpr(
      const VectorPtr& base,
      vector_size_t index) {
    return std::make_shared<core::ConstantTypedExpr>(
        BaseVector::wrapInConstant(1, index, base));
  }

  /// Create constant expression from a variant of primitive type.
  std::shared_ptr<core::ConstantTypedExpr> makeConstantExpr(
      variant value,
      const TypePtr& type = nullptr) {
    auto valueType = type != nullptr ? type : value.inferType();
    return std::make_shared<core::ConstantTypedExpr>(
        valueType, std::move(value));
  }

  /// Remove ". Input data: .*" from the 'context'.
  std::string trimInputPath(const std::string& context) {
    auto pos = context.find(". Input data: ");
    if (pos == std::string::npos) {
      return context;
    }
    return context.substr(0, pos);
  }

  std::string extractFromErrorContext(
      const std::string& context,
      const char* key) {
    auto startPos = context.find(key);
    VELOX_CHECK(startPos != std::string::npos);
    startPos += strlen(key);
    auto endPos = context.find(".", startPos);
    VELOX_CHECK(endPos != std::string::npos, context);
    return context.substr(startPos, endPos - startPos);
  }

  /// Extract input path from the 'context':
  ///     "<expression>. Input data: <input path>. ..."
  std::string extractInputPath(const std::string& context) {
    return extractFromErrorContext(context, ". Input data: ");
  }

  /// Extract expression sql's path from the 'context':
  ///     "... <input path>. SQL expression: <sql path>"
  std::string extractSqlPath(const std::string& context) {
    return extractFromErrorContext(context, ". SQL expression: ");
  }

  /// Extract all expressions sqls' path from the 'context':
  ///     "... <sql path>.  All SQL expressions: <all sql path>"
  std::string extractAllExprSqlPath(const std::string& context) {
    return extractFromErrorContext(context, ". All SQL expressions: ");
  }

  VectorPtr restoreVector(const std::string& path) {
    std::ifstream inputFile(path, std::ifstream::binary);
    VELOX_CHECK(!inputFile.fail(), "Cannot open file: {}", path);
    auto copy = facebook::velox::restoreVector(inputFile, pool());
    inputFile.close();
    return copy;
  }

  void verifyDataAndSqlPaths(const VeloxException& e, const VectorPtr& data) {
    auto inputPath = extractInputPath(e.additionalContext());
    auto copy = restoreVector(inputPath);
    assertEqualVectors(data, copy);

    auto sqlPath = extractSqlPath(e.additionalContext());
    auto sql = readSqlFromFile(sqlPath);
    ASSERT_NO_THROW(compileExpression(sql, asRowType(data->type())));

    LOG(ERROR) << e.additionalContext();
    auto allSqlsPath = extractAllExprSqlPath(e.additionalContext());
    auto allSqls = readSqlFromFile(allSqlsPath);
    ASSERT_NO_THROW(compileMultipleExprs(allSqls, asRowType(data->type())));
  }

  std::string readSqlFromFile(const std::string& path) {
    std::ifstream inputFile(path, std::ifstream::binary);

    // Find out file size.
    auto begin = inputFile.tellg();
    inputFile.seekg(0, std::ios::end);
    auto end = inputFile.tellg();

    auto fileSize = end - begin;
    if (fileSize == 0) {
      return "";
    }

    // Read the file.
    std::string sql;
    sql.resize(fileSize);

    inputFile.seekg(begin);
    inputFile.read(sql.data(), fileSize);
    inputFile.close();
    return sql;
  }

  std::exception_ptr assertError(
      const std::string& expression,
      const VectorPtr& input,
      const std::string& context,
      const std::string& additionalContext,
      const std::string& message) {
    try {
      evaluate(expression, makeRowVector({input}));
      EXPECT_TRUE(false) << "Expected an error";
    } catch (VeloxException& e) {
      EXPECT_EQ(context, trimInputPath(e.context()));
      EXPECT_EQ(additionalContext, trimInputPath(e.additionalContext()));
      EXPECT_EQ(message, e.message());
      return e.wrappedException();
    }
    return nullptr;
  }

  void assertErrorSimplified(
      const std::string& expression,
      const VectorPtr& input,
      const std::string& message) {
    try {
      auto inputVector = makeRowVector({input});
      auto exprSetSimplified = compileExpression<exec::ExprSetSimplified>(
          expression, asRowType(inputVector->type()));
      evaluate<exec::ExprSetSimplified>(exprSetSimplified.get(), inputVector);
      ASSERT_TRUE(false) << "Expected an error";
    } catch (VeloxException& e) {
      ASSERT_EQ(message, e.message());
    }
  }

  std::exception_ptr assertWrappedException(
      const std::string& expression,
      const VectorPtr& input,
      const std::string& context,
      const std::string& additionalContext,
      const std::string& message) {
    try {
      evaluate(expression, makeRowVector({input}));
      EXPECT_TRUE(false) << "Expected an error";
    } catch (VeloxException& e) {
      EXPECT_EQ(context, trimInputPath(e.context()));
      EXPECT_EQ(additionalContext, trimInputPath(e.additionalContext()));
      EXPECT_EQ(message, e.message());
      return e.wrappedException();
    }

    return nullptr;
  }

  void testToSql(const std::string& expression, const RowTypePtr& rowType) {
    auto exprSet = compileExpression(expression, rowType);
    auto sql = exprSet->expr(0)->toSql();
    auto copy = compileExpression(sql, rowType);
    ASSERT_EQ(
        exprSet->toString(false /*compact*/), copy->toString(false /*compact*/))
        << sql;
  }

  bool propagatesNulls(const core::TypedExprPtr& typedExpr) {
    exec::ExprSet exprSet({typedExpr}, execCtx_.get(), true);
    return exprSet.exprs().front()->propagatesNulls();
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{velox::core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  parse::ParseOptions options_;
};

class ParameterizedExprTest : public ExprTest,
                              public testing::WithParamInterface<bool> {
 public:
  ParameterizedExprTest() {
    std::unordered_map<std::string, std::string> configData(
        {{core::QueryConfig::kEnableExpressionEvaluationCache,
          GetParam() ? "true" : "false"}});
    queryCtx_ = velox::core::QueryCtx::create(
        nullptr, core::QueryConfig(std::move(configData)));
    execCtx_ = std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get());
  }
};

TEST_P(ParameterizedExprTest, moreEncodings) {
  const vector_size_t size = 1'000;
  std::vector<std::string> fruits = {"apple", "pear", "grapes", "pineapple"};
  VectorPtr a = makeFlatVector<int64_t>(size, [](auto row) { return row; });
  VectorPtr b = makeFlatVector(fruits);

  // Wrap b in a dictionary.
  auto indices =
      makeIndices(size, [&fruits](auto row) { return row % fruits.size(); });
  b = wrapInDictionary(indices, size, b);

  // Wrap both a and b in another dictionary.
  auto evenIndices = makeIndices(size / 2, [](auto row) { return row * 2; });

  a = wrapInDictionary(evenIndices, size / 2, a);
  b = wrapInDictionary(evenIndices, size / 2, b);

  auto result =
      evaluate("if(c1 = 'grapes', c0 + 10, c0)", makeRowVector({a, b}));
  ASSERT_EQ(VectorEncoding::Simple::DICTIONARY, result->encoding());
  ASSERT_EQ(size / 2, result->size());

  auto expected = makeFlatVector<int64_t>(size / 2, [&fruits](auto row) {
    return (fruits[row * 2 % 4] == "grapes") ? row * 2 + 10 : row * 2;
  });
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, reorder) {
  constexpr int32_t kTestSize = 20'000;

  auto data = makeRowVector(
      {makeFlatVector<int64_t>(kTestSize, [](auto row) { return row; })});
  auto exprSet = compileExpression(
      "if (c0 % 409 < 300 and c0 % 103 < 30, 1, 2)", asRowType(data->type()));
  auto result = evaluate(exprSet.get(), data);

  auto expectedResult = makeFlatVector<int64_t>(kTestSize, [](auto row) {
    return (row % 409) < 300 && (row % 103) < 30 ? 1 : 2;
  });

  auto condition = std::dynamic_pointer_cast<exec::ConjunctExpr>(
      exprSet->expr(0)->inputs()[0]);
  EXPECT_TRUE(condition != nullptr);

  // Verify that more efficient filter is first.
  for (auto i = 1; i < condition->inputs().size(); ++i) {
    std::cout << condition->selectivityAt(i - 1).timeToDropValue() << std::endl;
    EXPECT_LE(
        condition->selectivityAt(i - 1).timeToDropValue(),
        condition->selectivityAt(i).timeToDropValue());
  }
}

TEST_P(ParameterizedExprTest, constant) {
  auto exprSet = compileExpression("1 + 2 + 3 + 4", ROW({}));
  auto constExpr = dynamic_cast<exec::ConstantExpr*>(exprSet->expr(0).get());
  ASSERT_NE(constExpr, nullptr);
  auto constant = constExpr->value()->as<ConstantVector<int64_t>>()->valueAt(0);
  EXPECT_EQ(10, constant);

  exprSet = compileExpression("a * (1 + 2 + 3)", ROW({"a"}, {BIGINT()}));
  ASSERT_EQ(2, exprSet->expr(0)->inputs().size());
  constExpr =
      dynamic_cast<exec::ConstantExpr*>(exprSet->expr(0)->inputs()[1].get());
  ASSERT_NE(constExpr, nullptr);
  constant = constExpr->value()->as<ConstantVector<int64_t>>()->valueAt(0);
  EXPECT_EQ(6, constant);
}

// Tests that the eval does the right thing when it receives a NULL
// ConstantVector.
TEST_P(ParameterizedExprTest, constantNull) {
  // Need to manually build the expression since our eval doesn't support type
  // promotion, to upgrade the UNKOWN type generated by the NULL constant.
  auto inputExpr =
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0");
  auto nullConstant = std::make_shared<core::ConstantTypedExpr>(
      INTEGER(), variant::null(TypeKind::INTEGER));

  // Builds the following expression: "plus(c0, plus(c0, null))"
  auto expression = std::make_shared<core::CallTypedExpr>(
      INTEGER(),
      std::vector<core::TypedExprPtr>{
          inputExpr,
          std::make_shared<core::CallTypedExpr>(
              INTEGER(),
              std::vector<core::TypedExprPtr>{inputExpr, nullConstant},
              "plus"),
      },
      "plus");

  // Execute it and check it returns all null results.
  auto vector = makeNullableFlatVector<int32_t>({1, std::nullopt, 3});
  auto rowVector = makeRowVector({vector});
  SelectivityVector rows(rowVector->size());
  std::vector<VectorPtr> result(1);

  exec::ExprSet exprSet({expression}, execCtx_.get());
  exec::EvalCtx context(execCtx_.get(), &exprSet, rowVector.get());
  exprSet.eval(rows, context, result);

  auto expected = makeNullableFlatVector<int32_t>(
      {std::nullopt, std::nullopt, std::nullopt});
  assertEqualVectors(expected, result.front());
}

// Tests that exprCompiler throws if there's a return type mismatch between what
// the user specific in ConstantTypedExpr, and the available signatures.
TEST_P(ParameterizedExprTest, validateReturnType) {
  auto inputExpr =
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0");

  // Builds a "eq(c0, c0)" expression.
  auto expression = std::make_shared<core::CallTypedExpr>(
      INTEGER(), std::vector<core::TypedExprPtr>{inputExpr, inputExpr}, "eq");

  // Execute it and check it returns all null results.
  auto vector = makeNullableFlatVector<int32_t>({1, 2, 3});
  auto rowVector = makeRowVector({vector});
  SelectivityVector rows(rowVector->size());
  std::vector<VectorPtr> result(1);

  EXPECT_THROW(
      {
        exec::ExprSet exprSet({expression}, execCtx_.get());
        exec::EvalCtx context(execCtx_.get(), &exprSet, rowVector.get());
        exprSet.eval(rows, context, result);
      },
      VeloxUserError);
}

namespace {

template <typename T>
struct NoArgFunction {
  void call(int64_t& out) {
    out = 10;
  }
};
} // namespace

TEST_P(ParameterizedExprTest, constantFolding) {
  auto typedExpr = parseExpression("1 + 2", ROW({}));

  auto extractConstant = [](exec::Expr* expr) {
    auto constExpr = dynamic_cast<exec::ConstantExpr*>(expr);
    EXPECT_TRUE(constExpr);
    return constExpr->value()->as<ConstantVector<int64_t>>()->valueAt(0);
  };

  // Check that the constants have been folded.
  {
    exec::ExprSet exprSetFolded({typedExpr}, execCtx_.get(), true);

    auto expr = exprSetFolded.exprs().front();
    auto constExpr = dynamic_cast<exec::ConstantExpr*>(expr.get());

    ASSERT_TRUE(constExpr != nullptr);
    EXPECT_TRUE(constExpr->inputs().empty());
    EXPECT_EQ(3, extractConstant(expr.get()));
  }

  // Check that the constants have NOT been folded.
  {
    exec::ExprSet exprSetUnfolded({typedExpr}, execCtx_.get(), false);
    auto expr = exprSetUnfolded.exprs().front();

    ASSERT_EQ(2, expr->inputs().size());
    EXPECT_EQ(1, extractConstant(expr->inputs()[0].get()));
    EXPECT_EQ(2, extractConstant(expr->inputs()[1].get()));
  }

  {
    // codepoint() takes a single character, so this expression
    // deterministically throws; however, we should never throw at constant
    // folding time. Ensure compiling this expression does not throw..
    auto typedExpr = parseExpression("codepoint('abcdef')", ROW({}));
    EXPECT_NO_THROW(exec::ExprSet exprSet({typedExpr}, execCtx_.get(), true));
  }

  {
    registerFunction<NoArgFunction, int64_t>({"no_arg_func"});
    auto expression = parseExpression("no_arg_func()", ROW({}));
    exec::ExprSet exprSetFolded({expression}, execCtx_.get(), true);
    EXPECT_EQ(10, extractConstant(exprSetFolded.exprs().front().get()));
  }
}

TEST_P(ParameterizedExprTest, constantArray) {
  auto a = makeArrayVector<int32_t>(
      10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; });
  auto b = makeArrayVector<int64_t>(
      10, [](auto /*row*/) { return 7; }, [](auto row) { return row; });

  std::vector<core::TypedExprPtr> expressions = {
      makeConstantExpr(a, 3), makeConstantExpr(b, 5)};

  auto exprSet =
      std::make_unique<exec::ExprSet>(std::move(expressions), execCtx_.get());

  const vector_size_t size = 1'000;
  auto input = makeRowVector(ROW({}), size);
  exec::EvalCtx context(execCtx_.get(), exprSet.get(), input.get());

  SelectivityVector rows(input->size());
  std::vector<VectorPtr> result(2);
  exprSet->eval(rows, context, result);

  ASSERT_TRUE(a->equalValueAt(result[0].get(), 3, 0));
  ASSERT_TRUE(b->equalValueAt(result[1].get(), 5, 0));
}

TEST_P(ParameterizedExprTest, constantComplexNull) {
  std::vector<core::TypedExprPtr> expressions = {
      std::make_shared<const core::ConstantTypedExpr>(
          ARRAY(BIGINT()), variant::null(TypeKind::ARRAY)),
      std::make_shared<const core::ConstantTypedExpr>(
          MAP(VARCHAR(), BIGINT()), variant::null(TypeKind::MAP)),
      std::make_shared<const core::ConstantTypedExpr>(
          ROW({SMALLINT(), BIGINT()}), variant::null(TypeKind::ROW))};
  auto exprSet =
      std::make_unique<exec::ExprSet>(std::move(expressions), execCtx_.get());

  const vector_size_t size = 10;
  auto input = makeRowVector(ROW({}), size);
  exec::EvalCtx context(execCtx_.get(), exprSet.get(), input.get());

  SelectivityVector rows(size);
  std::vector<VectorPtr> result(3);
  exprSet->eval(rows, context, result);

  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, result[0]->encoding());
  ASSERT_EQ(TypeKind::ARRAY, result[0]->typeKind());
  ASSERT_TRUE(result[0]->as<ConstantVector<ComplexType>>()->isNullAt(0));

  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, result[1]->encoding());
  ASSERT_EQ(TypeKind::MAP, result[1]->typeKind());
  ASSERT_TRUE(result[1]->as<ConstantVector<ComplexType>>()->isNullAt(0));

  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, result[2]->encoding());
  ASSERT_EQ(TypeKind::ROW, result[2]->typeKind());
  ASSERT_TRUE(result[2]->as<ConstantVector<ComplexType>>()->isNullAt(0));
}

TEST_P(ParameterizedExprTest, constantScalarEquals) {
  auto a = makeFlatVector<int32_t>(10, [](auto row) { return row; });
  auto b = makeFlatVector<int32_t>(10, [](auto row) { return row; });
  auto c = makeFlatVector<int64_t>(10, [](auto row) { return row; });

  ASSERT_EQ(*makeConstantExpr<int32_t>(a, 3), *makeConstantExpr<int32_t>(b, 3));
  // The types differ, so not equal
  ASSERT_FALSE(
      *makeConstantExpr<int32_t>(a, 3) == *makeConstantExpr<int64_t>(c, 3));
  // The values differ, so not equal
  ASSERT_FALSE(
      *makeConstantExpr<int32_t>(a, 3) == *makeConstantExpr<int32_t>(b, 4));
}

TEST_P(ParameterizedExprTest, constantComplexEquals) {
  auto testConstantEquals =
      // a and b should be equal but distinct vectors.
      // a and c should be vectors with equal values but different types (e.g.
      // int32_t and int64_t).
      [&](const VectorPtr& a, const VectorPtr& b, const VectorPtr& c) {
        ASSERT_EQ(*makeConstantExpr(a, 3), *makeConstantExpr(b, 3));
        // The types differ, so not equal
        ASSERT_FALSE(*makeConstantExpr(a, 3) == *makeConstantExpr(c, 3));
        // The values differ, so not equal
        ASSERT_FALSE(*makeConstantExpr(a, 3) == *makeConstantExpr(b, 4));
      };

  testConstantEquals(
      makeArrayVector<int32_t>(
          10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; }),
      makeArrayVector<int32_t>(
          10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; }),
      makeArrayVector<int64_t>(
          10,
          [](auto /*row*/) { return 5; },
          [](auto row) { return row * 3; }));

  testConstantEquals(
      makeMapVector<int32_t, int32_t>(
          10,
          [](auto /*row*/) { return 5; },
          [](auto row) { return row; },
          [](auto row) { return row * 3; }),
      makeMapVector<int32_t, int32_t>(
          10,
          [](auto /*row*/) { return 5; },
          [](auto row) { return row; },
          [](auto row) { return row * 3; }),
      makeMapVector<int32_t, int64_t>(
          10,
          [](auto /*row*/) { return 5; },
          [](auto row) { return row; },
          [](auto row) { return row * 3; }));

  auto a = makeFlatVector<int32_t>(10, [](auto row) { return row; });
  auto b = makeFlatVector<int64_t>(10, [](auto row) { return row; });

  testConstantEquals(
      makeRowVector({a}), makeRowVector({a}), makeRowVector({b}));
}

namespace {
class PlusConstantFunction : public exec::VectorFunction {
 public:
  explicit PlusConstantFunction(int32_t addition) : addition_(addition) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);

    auto& arg = args[0];

    // The argument may be flat or constant.
    VELOX_CHECK(arg->isFlatEncoding() || arg->isConstantEncoding());

    BaseVector::ensureWritable(rows, INTEGER(), context.pool(), result);

    auto* flatResult = result->asFlatVector<int32_t>();
    auto* rawResult = flatResult->mutableRawValues();

    flatResult->clearNulls(rows);

    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<int32_t>>()->valueAt(0);
      rows.applyToSelected(
          [&](auto row) { rawResult[row] = value + addition_; });
    } else {
      auto* rawInput = arg->as<FlatVector<int32_t>>()->rawValues();

      rows.applyToSelected(
          [&](auto row) { rawResult[row] = rawInput[row] + addition_; });
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // integer -> integer
    return {exec::FunctionSignatureBuilder()
                .returnType("integer")
                .argumentType("integer")
                .build()};
  }

 private:
  const int32_t addition_;
};

} // namespace

TEST_P(ParameterizedExprTest, dictionaryAndConstantOverLazy) {
  exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(5));

  const vector_size_t size = 1'000;

  // Make LazyVector with nulls.
  auto valueAt = [](vector_size_t row) { return row; };
  auto isNullAt = [](vector_size_t row) { return row % 5 == 0; };

  const auto lazyVector =
      vectorMaker_.lazyFlatVector<int32_t>(size, valueAt, isNullAt);
  auto row = makeRowVector({lazyVector});
  auto result = evaluate("plus5(c0)", row);

  auto expected =
      makeFlatVector<int32_t>(size, [](auto row) { return row + 5; }, isNullAt);
  assertEqualVectors(expected, result);

  // Wrap LazyVector in a dictionary (select only even rows).
  auto evenIndices = makeIndices(size / 2, [](auto row) { return row * 2; });

  auto vector = wrapInDictionary(evenIndices, size / 2, lazyVector);
  row = makeRowVector({vector});
  result = evaluate("plus5(c0)", row);

  expected = makeFlatVector<int32_t>(
      size / 2, [](auto row) { return row * 2 + 5; }, isNullAt);
  assertEqualVectors(expected, result);

  // non-null constant
  vector = BaseVector::wrapInConstant(size, 3, lazyVector);
  row = makeRowVector({vector});
  result = evaluate("plus5(c0)", row);

  expected = makeFlatVector<int32_t>(size, [](auto /*row*/) { return 3 + 5; });
  assertEqualVectors(expected, result);

  // null constant
  vector = BaseVector::wrapInConstant(size, 5, lazyVector);
  row = makeRowVector({vector});
  result = evaluate("plus5(c0)", row);

  expected = makeAllNullFlatVector<int32_t>(size);
  assertEqualVectors(expected, result);
}

// Test evaluating single-argument vector function on a non-zero row of
// constant vector.
TEST_P(ParameterizedExprTest, vectorFunctionOnConstantInput) {
  exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(5));
  const vector_size_t size = 1'000;

  auto row = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeConstant(3, size)});

  VectorPtr expected = makeFlatVector<int32_t>(
      size, [](auto row) { return row > 5 ? 3 + 5 : 0; });
  auto result = evaluate("if (c0 > 5, plus5(c1), cast(0 as integer))", row);
  assertEqualVectors(expected, result);

  result = evaluate("is_null(c1)", row);
  expected = makeConstant(false, size);
  assertEqualVectors(expected, result);
}

namespace {
// f(n) = n + rand() - non-deterministict function with a single argument
class PlusRandomIntegerFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    VELOX_CHECK_EQ(args[0]->typeKind(), facebook::velox::TypeKind::INTEGER);

    BaseVector::ensureWritable(rows, INTEGER(), context.pool(), result);
    auto flatResult = result->asFlatVector<int32_t>();

    DecodedVector decoded(*args[0], rows);
    std::srand(1);
    rows.applyToSelected([&](auto row) {
      if (decoded.isNullAt(row)) {
        flatResult->setNull(row, true);
      } else {
        flatResult->set(row, decoded.valueAt<int32_t>(row) + std::rand());
      }
      return true;
    });
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // integer -> integer
    return {exec::FunctionSignatureBuilder()
                .returnType("integer")
                .argumentType("integer")
                .build()};
  }
};

void registerPlusRandomIntegerFunction() {
  exec::registerVectorFunction(
      "plus_random",
      PlusRandomIntegerFunction::signatures(),
      std::make_unique<PlusRandomIntegerFunction>(),
      exec::VectorFunctionMetadataBuilder().deterministic(false).build());
}
} // namespace

// Test evaluating single-argument non-deterministic vector function on
// constant vector. The function must be called on each row, not just one.
TEST_P(ParameterizedExprTest, nonDeterministicVectorFunctionOnConstantInput) {
  registerPlusRandomIntegerFunction();

  const vector_size_t size = 1'000;
  auto row = makeRowVector({makeConstant(10, size)});

  auto result = evaluate("plus_random(c0)", row);

  std::srand(1);
  auto expected = makeFlatVector<int32_t>(
      size, [](auto /*row*/) { return 10 + std::rand(); });
  assertEqualVectors(expected, result);
}

// Verify constant folding doesn't apply to non-deterministic functions.
TEST_P(ParameterizedExprTest, nonDeterministicConstantFolding) {
  registerPlusRandomIntegerFunction();

  const vector_size_t size = 1'000;
  auto emptyRow = makeRowVector(ROW({}), size);

  auto result = evaluate("plus_random(cast(23 as integer))", emptyRow);

  std::srand(1);
  auto expected = makeFlatVector<int32_t>(
      size, [](auto /*row*/) { return 23 + std::rand(); });
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, shortCircuit) {
  vector_size_t size = 4;

  auto a = makeConstant(10, size);
  auto b = makeFlatVector<int32_t>({-1, -2, -3, -4});

  auto result = evaluate("c0 > 0 OR c1 > 0", makeRowVector({a, b}));
  auto expectedResult = makeConstant(true, size);

  assertEqualVectors(expectedResult, result);

  result = evaluate("c0 < 0 AND c1 < 0", makeRowVector({a, b}));
  expectedResult = makeConstant(false, size);

  assertEqualVectors(expectedResult, result);
}

// Test common sub-expression (CSE) optimization with encodings.
// CSE evaluation may happen in different contexts, e.g. original input rows
// on first evaluation and base vectors uncovered through peeling of encodings
// on second. In this case, the row numbers from first evaluation and row
// numbers in the second evaluation are non-comparable.
//
// Consider two projections:
//  if (a > 0 AND c = 'apple')
//  if (b > 0 AND c = 'apple')
//
// c = 'apple' is CSE. Let a be flat vector, b and c be dictionaries with
// shared indices. On first evaluation, 'a' and 'c' don't share any encodings,
// no peeling happens and context contains the original vectors and rows. On
// second evaluation, 'b' and 'c' share dictionary encoding and it gets
// peeled. Context now contains base vectors and inner rows.
//
// Currently, this case doesn't work properly, hence, peeling disables CSE
// optimizations.
TEST_P(ParameterizedExprTest, cseEncodings) {
  auto a = makeFlatVector<int32_t>({1, 2, 3, 4, 5});

  auto indices = makeIndices({0, 0, 0, 1, 1});
  auto b = wrapInDictionary(indices, 5, makeFlatVector<int32_t>({11, 15}));
  auto c = wrapInDictionary(
      indices, 5, makeFlatVector<std::string>({"apple", "banana"}));

  auto results = evaluateMultiple(
      {"if (c0 > 0 AND c2 = 'apple', 10, 3)",
       "if (c1 > 0 AND c2 = 'apple', 20, 5)"},
      makeRowVector({a, b, c}));

  auto expected = makeFlatVector<int64_t>({10, 10, 10, 3, 3});
  assertEqualVectors(expected, results[0]);

  expected = makeFlatVector<int64_t>({20, 20, 20, 5, 5});
  assertEqualVectors(expected, results[1]);
}

namespace {
class AddSuffixFunction : public exec::VectorFunction {
 public:
  explicit AddSuffixFunction(const std::string& suffix) : suffix_{suffix} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto input = args[0]->asFlatVector<StringView>();
    auto localResult = std::dynamic_pointer_cast<FlatVector<StringView>>(
        BaseVector::create(VARCHAR(), rows.end(), context.pool()));
    rows.applyToSelected([&](auto row) {
      auto value = fmt::format("{}{}", input->valueAt(row).str(), suffix_);
      localResult->set(row, StringView(value));
      return true;
    });

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .build()};
  }

 private:
  const std::string suffix_;
};
} // namespace

// Test CSE evaluation where first evaluation applies to fewer rows then
// second. Make sure values calculated on first evaluation are preserved when
// calculating additional rows on second evaluation. This could happen if CSE
// is a function that uses EvalCtx::moveOrCopyResult which relies on
// isFinalSelection flag.
TEST_P(ParameterizedExprTest, csePartialEvaluation) {
  exec::registerVectorFunction(
      "add_suffix",
      AddSuffixFunction::signatures(),
      std::make_unique<AddSuffixFunction>("_xx"));

  auto a = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  auto b = makeFlatVector<std::string>({"a", "b", "c", "d", "e"});

  auto [results, stats] = evaluateMultipleWithStats(
      {
          "if (c0 >= 3, add_suffix(c1), 'n/a')",
          "add_suffix(c1)",
      },
      makeRowVector({a, b}));

  auto expected =
      makeFlatVector<std::string>({"n/a", "n/a", "c_xx", "d_xx", "e_xx"});
  assertEqualVectors(expected, results[0]);

  expected =
      makeFlatVector<std::string>({"a_xx", "b_xx", "c_xx", "d_xx", "e_xx"});
  assertEqualVectors(expected, results[1]);
  EXPECT_EQ(5, stats.at("add_suffix").numProcessedRows);

  std::tie(results, stats) = evaluateMultipleWithStats(
      {
          "if (c0 >= 3, add_suffix(c1), 'n/a')",
          "if (c0 < 2, 'n/a', add_suffix(c1))",
      },
      makeRowVector({a, b}));

  expected =
      makeFlatVector<std::string>({"n/a", "n/a", "c_xx", "d_xx", "e_xx"});
  assertEqualVectors(expected, results[0]);

  expected =
      makeFlatVector<std::string>({"n/a", "b_xx", "c_xx", "d_xx", "e_xx"});
  assertEqualVectors(expected, results[1]);
  EXPECT_EQ(4, stats.at("add_suffix").numProcessedRows);
}

TEST_P(ParameterizedExprTest, csePartialEvaluationWithEncodings) {
  auto data = makeRowVector(
      {wrapInDictionary(
           makeIndicesInReverse(5),
           wrapInDictionary(
               makeIndicesInReverse(5),
               makeFlatVector<int64_t>({0, 10, 20, 30, 40}))),
       makeFlatVector<int64_t>({3, 33, 333, 3333, 33333})});

  // Compile the expressions once, then execute two times. First time, evaluate
  // on 2 rows (0, 1). Seconds time, one 4 rows (0, 1, 2, 3).
  auto exprSet = compileMultiple(
      {
          "concat(concat(cast(c0 as varchar), ',', cast(c1 as varchar)), 'xxx')",
          "concat(concat(cast(c0 as varchar), ',', cast(c1 as varchar)), 'yyy')",
      },
      asRowType(data->type()));

  std::vector<VectorPtr> results(2);
  {
    SelectivityVector rows(2);
    exec::EvalCtx context(execCtx_.get(), exprSet.get(), data.get());
    exprSet->eval(rows, context, results);

    std::vector<VectorPtr> expectedResults = {
        makeFlatVector<StringView>({"0,3xxx", "10,33xxx"}),
        makeFlatVector<StringView>({"0,3yyy", "10,33yyy"}),
    };

    assertEqualVectors(expectedResults[0], results[0]);
    assertEqualVectors(expectedResults[1], results[1]);
  }

  {
    SelectivityVector rows(4);
    exec::EvalCtx context(execCtx_.get(), exprSet.get(), data.get());
    exprSet->eval(rows, context, results);

    std::vector<VectorPtr> expectedResults = {
        makeFlatVector<StringView>(
            {"0,3xxx", "10,33xxx", "20,333xxx", "30,3333xxx"}),
        makeFlatVector<StringView>(
            {"0,3yyy", "10,33yyy", "20,333yyy", "30,3333yyy"}),
    };

    assertEqualVectors(expectedResults[0], results[0]);
    assertEqualVectors(expectedResults[1], results[1]);
  }
}

// Checks that vector function registry overwrites if multiple registry
// attempts are made for the same functions.
TEST_P(ParameterizedExprTest, overwriteInRegistry) {
  exec::VectorFunctionMetadata metadata;
  auto inserted = exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(500),
      metadata,
      true);
  ASSERT_TRUE(inserted);
  core::QueryConfig config({});
  auto vectorFunction =
      exec::getVectorFunction("plus5", {INTEGER()}, {}, config);
  ASSERT_TRUE(vectorFunction != nullptr);

  inserted = exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(5),
      metadata,
      true);
  ASSERT_TRUE(inserted);

  auto vectorFunction2 =
      exec::getVectorFunction("plus5", {INTEGER()}, {}, config);

  ASSERT_TRUE(vectorFunction2 != nullptr);
  ASSERT_TRUE(vectorFunction != vectorFunction2);

  ASSERT_TRUE(inserted);
}

// Check non overwriting path in the function registry

TEST_F(ExprTest, keepInRegistry) {
  // Adding a new function, overwrite = false;

  exec::VectorFunctionMetadata metadata;
  bool inserted = exec::registerVectorFunction(
      "NonExistingFunction",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(500),
      metadata,
      false);

  ASSERT_TRUE(inserted);

  core::QueryConfig config({});
  auto vectorFunction =
      exec::getVectorFunction("NonExistingFunction", {}, {}, config);

  inserted = exec::registerVectorFunction(
      "NonExistingFunction",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(400),
      metadata,
      false);
  ASSERT_FALSE(inserted);
  ASSERT_EQ(
      vectorFunction,
      exec::getVectorFunction("NonExistingFunction", {}, {}, config));
}

TEST_P(ParameterizedExprTest, lazyVectors) {
  vector_size_t size = 1'000;

  // Make LazyVector with no nulls
  auto valueAt = [](auto row) { return row; };
  auto vector = vectorMaker_.lazyFlatVector<int64_t>(size, valueAt);
  auto row = makeRowVector({vector});

  auto result = evaluate("c0 + coalesce(c0, 1)", row);

  auto expected =
      makeFlatVector<int64_t>(size, [](auto row) { return row * 2; }, nullptr);
  assertEqualVectors(expected, result);

  // Make LazyVector with nulls
  auto isNullAt = [](auto row) { return row % 5 == 0; };
  vector = vectorMaker_.lazyFlatVector<int64_t>(size, valueAt, isNullAt);
  row = makeRowVector({vector});

  result = evaluate("c0 + coalesce(c0, 1)", row);

  expected =
      makeFlatVector<int64_t>(size, [](auto row) { return row * 2; }, isNullAt);
  assertEqualVectors(expected, result);
}

// Tests that lazy vectors are not loaded unnecessarily.
TEST_P(ParameterizedExprTest, lazyLoading) {
  const vector_size_t size = 1'000;
  VectorPtr vector =
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5; });
  VectorPtr lazyVector = std::make_shared<LazyVector>(
      execCtx_->pool(),
      BIGINT(),
      size,
      std::make_unique<test::SimpleVectorLoader>([&](RowSet /*rows*/) {
        VELOX_FAIL("This lazy vector is not expected to be loaded");
        return nullptr;
      }));

  auto result = evaluate(
      "if(c0 = 10, c1 + 5, c0 - 5)", makeRowVector({vector, lazyVector}));
  auto expected =
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5 - 5; });
  assertEqualVectors(expected, result);

  vector = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 5; }, nullEvery(7));

  result = evaluate(
      "if(c0 = 10, c1 + 5, c0 - 5)", makeRowVector({vector, lazyVector}));
  expected = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 5 - 5; }, nullEvery(7));
  assertEqualVectors(expected, result);

  // Wrap non-lazy vector in a dictionary (repeat each row twice).
  auto evenIndices = makeIndices(size, [](auto row) { return row / 2; });
  vector = wrapInDictionary(evenIndices, size, vector);

  result = evaluate(
      "if(c0 = 10, c1 + 5, c0 - 5)", makeRowVector({vector, lazyVector}));
  expected = makeFlatVector<int64_t>(
      size,
      [](auto row) { return (row / 2) % 5 - 5; },
      [](auto row) { return (row / 2) % 7 == 0; });
  assertEqualVectors(expected, result);

  // Wrap both vectors in the same dictionary.
  lazyVector = wrapInDictionary(evenIndices, size, lazyVector);

  result = evaluate(
      "if(c0 = 10, c1 + 5, c0 - 5)", makeRowVector({vector, lazyVector}));
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, selectiveLazyLoadingAnd) {
  const vector_size_t size = 1'000;

  // Evaluate AND expression on 3 lazy vectors and verify that each
  // subsequent vector is loaded for fewer rows than the one before.
  // Create 3 identical vectors with values set to row numbers. Use conditions
  // that pass on half of the rows for the first vector, a third for the
  // second, and a fifth for the third: a % 2 = 0 AND b % 3 = 0 AND c % 5 = 0.
  // Verify that all rows are loaded for the first vector, half for the second
  // and only 1/6 for the third.
  auto valueAt = [](auto row) { return row; };
  auto a = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto b = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, ceil(size / 2.0), [](auto row) {
        return row * 2;
      });
  auto c = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, ceil(size / 2.0 / 3.0), [](auto row) {
        return row * 2 * 3;
      });

  auto result = evaluate(
      "c0 % 2 = 0 AND c1 % 3 = 0 AND c2 % 5 = 0", makeRowVector({a, b, c}));
  auto expected = makeFlatVector<bool>(
      size, [](auto row) { return row % (2 * 3 * 5) == 0; });
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, selectiveLazyLoadingOr) {
  const vector_size_t size = 1'000;

  // Evaluate OR expression. Columns under OR must be loaded for "all" rows
  // because the engine currently doesn't know whether a column is used
  // elsewhere or not.
  auto valueAt = [](auto row) { return row; };
  auto a = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto b = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto c = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });

  auto result = evaluate(
      "c0 % 2 <> 0 OR c1 % 4 <> 0 OR c2 % 8 <> 0", makeRowVector({a, b, c}));
  auto expected = makeFlatVector<bool>(size, [](auto row) {
    return row % 2 != 0 || row % 4 != 0 || row % 8 != 0;
  });
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, lazyVectorAccessTwiceWithDifferentRows) {
  const vector_size_t size = 4;

  auto c0 = makeNullableFlatVector<int64_t>({1, 1, 1, std::nullopt});
  // [0, 1, 2, 3] if fully loaded
  std::vector<vector_size_t> loadedRows;
  auto valueAt = [](auto row) { return row; };
  VectorPtr c1 = std::make_shared<LazyVector>(
      pool_.get(),
      BIGINT(),
      size,
      std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
        for (auto row : rows) {
          loadedRows.push_back(row);
        }
        return makeFlatVector<int64_t>(rows.back() + 1, valueAt);
      }));

  auto result = evaluate(
      "row_constructor(c0 + c1, if (c1 >= 0, c1, 0))", makeRowVector({c0, c1}));

  auto expected = makeRowVector(
      {makeNullableFlatVector<int64_t>({1, 2, 3, std::nullopt}),
       makeNullableFlatVector<int64_t>({0, 1, 2, 3})});

  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, lazyVectorAccessTwiceInDifferentExpressions) {
  const vector_size_t size = 1'000;

  // Fields referenced by multiple expressions will load lazy vector
  // immediately in ExprSet::eval().
  auto isNullAtColA = [](auto row) { return row % 4 == 0; };
  auto isNullAtColC = [](auto row) { return row % 2 == 0; };

  auto a = makeLazyFlatVector<int64_t>(
      size,
      [](auto row) { return row; },
      isNullAtColA,
      size,
      [](auto row) { return row; });
  auto b = makeLazyFlatVector<int64_t>(
      size,
      [](auto row) { return row * 2; },
      nullptr,
      size,
      [](auto row) { return row; });
  auto c = makeLazyFlatVector<int64_t>(
      size,
      [](auto row) { return row; },
      isNullAtColC,
      size,
      [](auto row) { return row; });

  auto result = evaluateMultiple(
      {"if(c0 is not null, c0, c1)", "if (c2 is not null, c2, c1)"},
      makeRowVector({a, b, c}));

  auto expected = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 4 == 0 ? row * 2 : row; });
  assertEqualVectors(expected, result[0]);

  expected = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 2 == 0 ? row * 2 : row; });
  assertEqualVectors(expected, result[1]);
}

TEST_P(ParameterizedExprTest, selectiveLazyLoadingIf) {
  const vector_size_t size = 1'000;

  // Evaluate IF expression. Columns under IF must be loaded for "all" rows
  // because the engine currently doesn't know whether a column is used in a
  // single branch (then or else) or in both.
  auto valueAt = [](auto row) { return row; };

  auto a = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto b = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto c = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });

  auto result =
      evaluate("if (c0 % 2 = 0, c1 + c2, c2 / 3)", makeRowVector({a, b, c}));
  auto expected = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 2 == 0 ? row + row : row / 3; });
  assertEqualVectors(expected, result);
}

namespace {
class StatefulVectorFunction : public exec::VectorFunction {
 public:
  explicit StatefulVectorFunction(
      const std::string& /*name*/,
      const std::vector<exec::VectorFunctionArg>& inputs)
      : numInputs_(inputs.size()) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), numInputs_);
    auto numInputs = BaseVector::createConstant(
        INTEGER(), numInputs_, rows.size(), context.pool());
    if (!result) {
      result = numInputs;
    } else {
      BaseVector::ensureWritable(rows, INTEGER(), context.pool(), result);
      result->copy(numInputs.get(), rows, nullptr);
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T... -> integer
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("integer")
                .argumentType("T")
                .variableArity()
                .build()};
  }

 private:
  const int32_t numInputs_;
};
} // namespace

TEST_P(ParameterizedExprTest, statefulVectorFunctions) {
  exec::registerStatefulVectorFunction(
      "TEST_Function",
      StatefulVectorFunction::signatures(),
      exec::makeVectorFunctionFactory<StatefulVectorFunction>());

  vector_size_t size = 1'000;

  auto a = makeFlatVector<int64_t>(size, [](auto row) { return row; });
  auto b = makeFlatVector<int64_t>(size, [](auto row) { return row * 2; });
  auto row = makeRowVector({a, b});

  {
    auto result = evaluate("TEST_Function(c0)", row);

    auto expected =
        makeFlatVector<int32_t>(size, [](auto /*row*/) { return 1; });
    assertEqualVectors(expected, result);
  }

  {
    auto result = evaluate("TEST_Function(c0, c1)", row);

    auto expected =
        makeFlatVector<int32_t>(size, [](auto /*row*/) { return 2; });
    assertEqualVectors(expected, result);
  }
}

struct OpaqueState {
  static int constructed;
  static int destructed;

  static void clearStats() {
    constructed = 0;
    destructed = 0;
  }

  explicit OpaqueState(int x) : x(x) {
    ++constructed;
  }

  ~OpaqueState() {
    ++destructed;
  }

  int x;
};

int OpaqueState::constructed = 0;
int OpaqueState::destructed = 0;

template <typename T>
struct TestOpaqueCreateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<std::shared_ptr<OpaqueState>>& out,
      const arg_type<int64_t>& x) {
    out = std::make_shared<OpaqueState>(x);
    return true;
  }
};

template <typename T>
struct TestOpaqueAddFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& out,
      const arg_type<std::shared_ptr<OpaqueState>>& state,
      const arg_type<int64_t>& y) {
    out = state->x + y;
    return true;
  }
};

bool registerTestUDFs() {
  static bool once = [] {
    registerFunction<
        TestOpaqueCreateFunction,
        std::shared_ptr<OpaqueState>,
        int64_t>({"test_opaque_create"});
    registerFunction<
        TestOpaqueAddFunction,
        int64_t,
        std::shared_ptr<OpaqueState>,
        int64_t>({"test_opaque_add"});
    return true;
  }();
  return once;
}

TEST_P(ParameterizedExprTest, opaque) {
  registerTestUDFs();

  static constexpr vector_size_t kRows = 100;

  OpaqueState::clearStats();

  auto data = makeRowVector({
      makeFlatVector<int64_t>(
          kRows, [](auto row) { return row; }, nullEvery(7)),
      makeFlatVector<int64_t>(
          kRows, [](auto row) { return row * 2; }, nullEvery(11)),
      BaseVector::wrapInConstant(
          kRows,
          0,
          makeFlatVector<std::shared_ptr<void>>(
              1,
              [](auto row) {
                return std::static_pointer_cast<void>(
                    std::make_shared<OpaqueState>(123));
              })),
  });

  EXPECT_EQ(1, OpaqueState::constructed);

  int nonNulls = 0;
  for (auto i = 0; i < kRows; ++i) {
    if (i % 7 != 0 && i % 11 != 0) {
      ++nonNulls;
    }
  }

  // Opaque value created each time.
  OpaqueState::clearStats();
  auto result = evaluate("test_opaque_add(test_opaque_create(c0), c1)", data);
  auto expectedResult = makeFlatVector<int64_t>(
      kRows,
      [](auto row) { return row + row * 2; },
      [](auto row) { return row % 7 == 0 || row % 11 == 0; });
  assertEqualVectors(expectedResult, result);

  EXPECT_EQ(OpaqueState::constructed, nonNulls);
  EXPECT_EQ(OpaqueState::destructed, nonNulls);

  // Opaque value passed in as a constant explicitly.
  OpaqueState::clearStats();
  result = evaluate("test_opaque_add(c2, c1)", data);
  expectedResult = makeFlatVector<int64_t>(
      kRows, [](auto row) { return 123 + row * 2; }, nullEvery(11));
  assertEqualVectors(expectedResult, result);

  // Nothing got created!
  EXPECT_EQ(OpaqueState::constructed, 0);
  EXPECT_EQ(OpaqueState::destructed, 0);

  // Opaque value created by a function taking a literal. Should be
  // constant-folded.
  OpaqueState::clearStats();
  result = evaluate("test_opaque_add(test_opaque_create(123), c1)", data);
  expectedResult = makeFlatVector<int64_t>(
      kRows, [](auto row) { return 123 + row * 2; }, nullEvery(11));
  assertEqualVectors(expectedResult, result);

  EXPECT_EQ(OpaqueState::constructed, 1);
  EXPECT_EQ(OpaqueState::destructed, 1);
}

TEST_P(ParameterizedExprTest, switchExpr) {
  vector_size_t size = 1'000;
  auto vector = makeRowVector(
      {makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       makeFlatVector<int32_t>(
           size, [](auto row) { return row; }, nullEvery(5)),
       makeConstant<int32_t>(0, size)});

  auto result =
      evaluate("case c0 when 7 then 1 when 11 then 2 else 0 end", vector);
  std::function<int32_t(vector_size_t)> expectedValueAt = [](auto row) {
    switch (row) {
      case 7:
        return 1;
      case 11:
        return 2;
      default:
        return 0;
    }
  };
  auto expected = makeFlatVector<int64_t>(size, expectedValueAt);
  assertEqualVectors(expected, result);

  // c1 has nulls
  result = evaluate("case c1 when 7 then 1 when 11 then 2 else 0 end", vector);
  assertEqualVectors(expected, result);

  // no "else" clause
  result = evaluate("case c0 when 7 then 1 when 11 then 2 end", vector);
  expected = makeFlatVector<int64_t>(
      size, expectedValueAt, [](auto row) { return row != 7 && row != 11; });
  assertEqualVectors(expected, result);

  result = evaluate("case c1 when 7 then 1 when 11 then 2 end", vector);
  assertEqualVectors(expected, result);

  // No "else" clause and no match.
  result = evaluate("case 0 when 100 then 1 when 200 then 2 end", vector);
  expected = makeAllNullFlatVector<int64_t>(size);
  assertEqualVectors(expected, result);

  result = evaluate("case c2 when 100 then 1 when 200 then 2 end", vector);
  assertEqualVectors(expected, result);

  // non-equality case expression
  result = evaluate(
      "case when c0 < 7 then 1 when c0 < 11 then 2 else 0 end", vector);
  expectedValueAt = [](auto row) {
    if (row < 7) {
      return 1;
    }
    if (row < 11) {
      return 2;
    }
    return 0;
  };
  expected = makeFlatVector<int64_t>(size, expectedValueAt);
  assertEqualVectors(expected, result);

  result = evaluate(
      "case when c1 < 7 then 1 when c1 < 11 then 2 else 0 end", vector);
  expected = makeFlatVector<int64_t>(size, [](auto row) {
    if (row % 5 == 0) {
      return 0;
    }
    if (row < 7) {
      return 1;
    }
    if (row < 11) {
      return 2;
    }
    return 0;
  });
  assertEqualVectors(expected, result);

  // non-equality case expression, no else clause
  result = evaluate("case when c0 < 7 then 1 when c0 < 11 then 2 end", vector);
  expected = makeFlatVector<int64_t>(
      size, expectedValueAt, [](auto row) { return row >= 11; });
  assertEqualVectors(expected, result);

  result = evaluate("case when c1 < 7 then 1 when c1 < 11 then 2 end", vector);
  expected = makeFlatVector<int64_t>(size, expectedValueAt, [](auto row) {
    return row >= 11 || row % 5 == 0;
  });
  assertEqualVectors(expected, result);

  // non-constant then expression
  result = evaluate(
      "case when c0 < 7 then c0 + 5 when c0 < 11 then c0 - 11 "
      "else c0::BIGINT end",
      vector);
  expectedValueAt = [](auto row) {
    if (row < 7) {
      return row + 5;
    }
    if (row < 11) {
      return row - 11;
    }
    return row;
  };
  expected = makeFlatVector<int64_t>(size, expectedValueAt);
  assertEqualVectors(expected, result);

  result = evaluate(
      "case when c1 < 7 then c1 + 5 when c1 < 11 then c1 - 11 "
      "else c1::BIGINT end",
      vector);
  expected = makeFlatVector<int64_t>(size, expectedValueAt, nullEvery(5));
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, swithExprSanityChecks) {
  auto vector = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});

  // Then clauses have different types.
  VELOX_ASSERT_THROW(
      evaluate(
          "case c0 when 7 then 1 when 11 then 'welcome' else 0 end", vector),
      "All then clauses of a SWITCH statement must have the same type. "
      "Expected BIGINT, but got VARCHAR.");

  // Else clause has different type.
  VELOX_ASSERT_THROW(
      evaluate("case c0 when 7 then 1 when 11 then 2 else 'hello' end", vector),
      "Else clause of a SWITCH statement must have the same type as 'then' clauses. "
      "Expected BIGINT, but got VARCHAR.");

  // Unknown is not implicitly casted.
  VELOX_ASSERT_THROW(
      evaluate("case c0 when 7 then 1 when 11 then null else 3 end", vector),
      "All then clauses of a SWITCH statement must have the same type. "
      "Expected BIGINT, but got UNKNOWN.");

  // Unknown is not implicitly casted.
  VELOX_ASSERT_THROW(
      evaluate(
          "case c0 when 7 then  row_constructor(null, 1) when 11 then  row_constructor(1, null) end",
          vector),
      "All then clauses of a SWITCH statement must have the same type. "
      "Expected ROW<c1:UNKNOWN,c2:BIGINT>, but got ROW<c1:BIGINT,c2:UNKNOWN>.");
}

TEST_P(ParameterizedExprTest, switchExprWithNull) {
  vector_size_t size = 1'000;
  // Build an input with c0 column having nulls at odd row index.
  auto vector = makeRowVector(
      {makeFlatVector<int32_t>(
           size,
           [](auto /*unused*/) { return 7; },
           [](auto row) { return row % 2; }),
       makeFlatVector<int32_t>(
           size, [](auto row) { return row; }, nullEvery(5)),
       makeConstant<int32_t>(0, size)});

  auto result = evaluate("case c0 when 7 then 1 else 0 end", vector);
  // If 'c0' is null, then we shall get 0 from else branch.
  auto expected = makeFlatVector<int64_t>(size, [](auto row) {
    if (row % 2 == 0) {
      return 1;
    } else {
      return 0;
    }
  });
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, ifWithConstant) {
  vector_size_t size = 4;

  auto a = makeFlatVector<int32_t>({-1, -2, -3, -4});
  auto b = makeNullConstant(TypeKind::INTEGER, size); // 4 nulls
  auto result = evaluate("is_null(if(c0 > 0, c0, c1))", makeRowVector({a, b}));
  EXPECT_EQ(VectorEncoding::Simple::CONSTANT, result->encoding());
  EXPECT_EQ(true, result->as<ConstantVector<bool>>()->valueAt(0));
}

// Make sure that switch do set nulls for rows that are not evaluated by the
// switch due to a throw.
TEST_P(ParameterizedExprTest, switchSetNullsForThrowIndices) {
  // Build an input with c0 column having nulls at odd row index.
  registerFunction<TestingAlwaysThrowsFunction, bool, int64_t>(
      {"always_throws"});
  registerFunction<TestingThrowsAtOddFunction, bool, int64_t>({"throw_at_odd"});

  auto eval = [&](const std::string& input, auto inputRow) {
    // Evaluate an expression in a no throw context without using try
    // expression to verify that the switch sets the nulls. If we use try, try
    // will add the nulls and we won't be able to check that the switch adds
    // nulls.
    auto exprSet = compileMultiple({input}, asRowType(inputRow->type()));
    exec::EvalCtx context(execCtx_.get(), exprSet.get(), inputRow.get());
    *context.mutableThrowOnError() = false;
    std::vector<VectorPtr> results;
    SelectivityVector rows(inputRow->size());
    exprSet->eval(rows, context, results);
    return results[0];
  };

  // All null.
  {
    auto input = vectorMaker_.flatVector<int64_t>({1, 2, 3, 4, 5});
    auto result =
        eval("if (always_throws(c0), 7, 1)", vectorMaker_.rowVector({input}));
    for (int i = 0; i < input->size(); i++) {
      EXPECT_TRUE(result->isNullAt(i));
    }
  }
  {
    auto input = vectorMaker_.flatVector<int64_t>({1, 1, 1, 4, 4});
    auto result = eval(
        "if(always_throws(c0), row_constructor(1,2), row_constructor(1,2))",
        vectorMaker_.rowVector({input}));
    EXPECT_EQ(result->size(), input->size());
    for (int i = 0; i < input->size(); i++) {
      EXPECT_TRUE(result->isNullAt(i)) << "index: " << i;
    }
  }

  // Null at odd.
  {
    auto input = vectorMaker_.flatVector<int64_t>({1, 2, 3, 4, 5});
    auto result =
        eval("if(throw_at_odd(c0), 7, 1)", vectorMaker_.rowVector({input}));
    EXPECT_EQ(result->size(), input->size());
    for (int i = 0; i < input->size(); i++) {
      EXPECT_EQ(result->isNullAt(i), input->valueAt(i) % 2);
    }
  }

  {
    auto input = vectorMaker_.flatVector<int64_t>({1, 1, 1, 4, 4});
    auto result = eval(
        "if(throw_at_odd(c0), array_constructor(1,2), array_constructor(1,2))",
        vectorMaker_.rowVector({input}));
    EXPECT_EQ(result->size(), input->size());
    for (int i = 0; i < input->size(); i++) {
      EXPECT_EQ(result->isNullAt(i), input->valueAt(i) % 2);
    }
  }
}

namespace {
// Testing functions for generating intermediate results in different
// encodings. The test case passes vectors to these and these
// functions make constant/dictionary vectors from their arguments

// Returns the first value of the argument vector wrapped as a constant.
class TestingConstantFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& /*context*/,
      VectorPtr& result) const override {
    VELOX_CHECK(rows.isAllSelected());
    result = BaseVector::wrapInConstant(rows.size(), 0, args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};

// Returns a dictionary vector with values from the first argument
// vector and indices from the second.
class TestingDictionaryFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& /*context*/,
      VectorPtr& result) const override {
    VELOX_CHECK(rows.isAllSelected());
    auto& indices = args[1]->as<FlatVector<int32_t>>()->values();
    result = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, rows.size(), args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T, integer -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .argumentType("integer")
                .build()};
  }
};

// Single-argument deterministic functions always receive their argument
// vector as flat or constant.
class TestingSingleArgDeterministicFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto& arg = args[0];
    VELOX_CHECK(arg->isFlatEncoding() || arg->isConstantEncoding());
    BaseVector::ensureWritable(rows, outputType, context.pool(), result);
    result->copy(arg.get(), rows, nullptr);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_testing_constant,
    TestingConstantFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<TestingConstantFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_testing_dictionary,
    TestingDictionaryFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<TestingDictionaryFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_testing_single_arg_deterministic,
    TestingSingleArgDeterministicFunction::signatures(),
    std::make_unique<TestingSingleArgDeterministicFunction>());

TEST_P(ParameterizedExprTest, peelArgs) {
  constexpr int32_t kSize = 100;
  constexpr int32_t kDistinct = 10;
  VELOX_REGISTER_VECTOR_FUNCTION(udf_testing_constant, "testing_constant");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_testing_dictionary, "testing_dictionary");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_testing_single_arg_deterministic, "testing_single_arg_deterministic");

  std::vector<int32_t> onesSource(kSize, 1);
  std::vector<int32_t> distinctSource(kDistinct);
  std::iota(distinctSource.begin(), distinctSource.end(), 11);
  std::vector<vector_size_t> indicesSource(kSize);
  for (auto i = 0; i < indicesSource.size(); ++i) {
    indicesSource[i] = i % kDistinct;
  }
  std::vector lengthSource(kDistinct, kSize / kDistinct);
  auto allOnes = makeFlatVector<int32_t>(onesSource);

  // constant
  auto result = evaluate("1 + testing_constant(c0)", makeRowVector({allOnes}));
  auto expected64 =
      makeFlatVector<int64_t>(kSize, [](int32_t /*i*/) { return 2; });
  assertEqualVectors(expected64, result);
  result = evaluate(
      "testing_constant(c0) + testing_constant(c1)",
      makeRowVector({allOnes, allOnes}));
  auto expected32 =
      makeFlatVector<int32_t>(kSize, [](int32_t /*i*/) { return 2; });
  assertEqualVectors(expected32, result);

  // Constant and dictionary
  auto distincts = makeFlatVector<int32_t>(distinctSource);
  auto indices = makeFlatVector<int32_t>(indicesSource);
  result = evaluate(
      "testing_constant(c0) + testing_dictionary(c1, c2)",
      makeRowVector({allOnes, distincts, indices}));
  expected32 = makeFlatVector<int32_t>(kSize, [&](int32_t row) {
    return 1 + distinctSource[indicesSource[row]];
  });
  assertEqualVectors(expected32, result);

  // dictionary and single-argument deterministic
  indices = makeFlatVector<int32_t>(kSize, [](auto) {
    // having all indices to be the same makes DictionaryVector::isConstant()
    // returns true
    return 0;
  });
  result = evaluate(
      "testing_single_arg_deterministic(testing_dictionary(c1, c0))",
      makeRowVector({indices, distincts}));
  expected32 = makeFlatVector<int32_t>(kSize, [](int32_t /*i*/) { return 11; });
  assertEqualVectors(expected32, result);
}

class NullArrayFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // This function returns a vector of all nulls
    BaseVector::ensureWritable(rows, ARRAY(VARCHAR()), context.pool(), result);
    result->addNulls(rows);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T... -> array(varchar)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("array(varchar)")
                .argumentType("T")
                .variableArity()
                .build()};
  }
};

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_null_array,
    NullArrayFunction::signatures(),
    std::make_unique<NullArrayFunction>());

TEST_P(ParameterizedExprTest, complexNullOutput) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_null_array, "null_array");

  auto row = makeRowVector({makeAllNullFlatVector<int64_t>(1)});

  auto expectedResults =
      BaseVector::createNullConstant(ARRAY(VARCHAR()), 1, execCtx_->pool());
  auto resultForNulls = evaluate("null_array(NULL, NULL)", row);

  // Making sure the output of the function is the same when returning all
  // null or called on NULL constants
  assertEqualVectors(expectedResults, resultForNulls);
}

TEST_P(ParameterizedExprTest, rewriteInputs) {
  // rewrite one field
  {
    auto alpha =
        std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "alpha");
    auto expr = parseExpression(
        "(a + b) * 2.1", ROW({"a", "b"}, {INTEGER(), DOUBLE()}));
    expr = expr->rewriteInputNames({{"a", alpha}});

    auto expectedExpr = parseExpression(
        "(alpha + b) * 2.1", ROW({"alpha", "b"}, {INTEGER(), DOUBLE()}));
    ASSERT_EQ(*expectedExpr, *expr);
  }

  // rewrite 2 fields
  {
    auto alpha =
        std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "alpha");
    auto beta = std::make_shared<core::FieldAccessTypedExpr>(DOUBLE(), "beta");
    auto expr = parseExpression(
        "a + b * c", ROW({"a", "b", "c"}, {INTEGER(), DOUBLE(), DOUBLE()}));
    expr = expr->rewriteInputNames({{"a", alpha}, {"b", beta}});

    auto expectedExpr = parseExpression(
        "alpha + beta * c",
        ROW({"alpha", "beta", "c"}, {INTEGER(), DOUBLE(), DOUBLE()}));
    ASSERT_EQ(*expectedExpr, *expr);
  }

  // rewrite with lambda
  {
    auto alpha =
        std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "alpha");
    auto expr =
        parseExpression("i -> i * a", ROW({"a"}, {INTEGER()}), {INTEGER()});
    expr = expr->rewriteInputNames({{"a", alpha}});

    auto expectedExpr = parseExpression(
        "i -> i * alpha", ROW({"alpha"}, {INTEGER()}), {INTEGER()});
    ASSERT_EQ(*expectedExpr, *expr);
  }

  // no rewrite with dereference
  {
    auto alpha =
        std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "alpha");
    auto expr = parseExpression(
        "i -> i * b.a", ROW({"b"}, {ROW({"a"}, {INTEGER()})}), {INTEGER()});
    expr = expr->rewriteInputNames({{"a", alpha}});

    auto expectedExpr = parseExpression(
        "i -> i * b.a", ROW({"b"}, {ROW({"a"}, {INTEGER()})}), {INTEGER()});
    ASSERT_EQ(*expectedExpr, *expr);
  }
}

TEST_F(ExprTest, memo) {
  // Verify that dictionary memoization
  // 1. correctly evaluates the unevaluated rows on subsequent runs
  // 2. Only caches results if it encounters the same base twice
  auto base = makeArrayVector<int64_t>(
      500,
      [](auto row) { return row % 5 + 1; },
      [](auto row, auto index) { return (row % 3) + index; });

  auto evenIndices = makeIndices(100, [](auto row) { return 8 + row * 2; });
  auto oddIndices = makeIndices(100, [](auto row) { return 9 + row * 2; });

  auto rowType = ROW({"c0"}, {base->type()});
  auto exprSet = compileExpression("c0[1] = 1", rowType);

  auto [result, stats] = evaluateWithStats(
      exprSet.get(), makeRowVector({wrapInDictionary(evenIndices, 100, base)}));
  auto expectedResult = makeFlatVector<bool>(
      100, [](auto row) { return (8 + row * 2) % 3 == 1; });
  assertEqualVectors(expectedResult, result);
  VELOX_CHECK_EQ(stats["eq"].numProcessedRows, 100);
  VELOX_CHECK(base.unique());

  // After this results would be cached
  std::tie(result, stats) = evaluateWithStats(
      exprSet.get(), makeRowVector({wrapInDictionary(evenIndices, 100, base)}));
  assertEqualVectors(expectedResult, result);
  VELOX_CHECK_EQ(stats["eq"].numProcessedRows, 200);
  VELOX_CHECK(!base.unique());

  // Unevaluated rows are processed
  std::tie(result, stats) = evaluateWithStats(
      exprSet.get(), makeRowVector({wrapInDictionary(oddIndices, 100, base)}));
  expectedResult = makeFlatVector<bool>(
      100, [](auto row) { return (9 + row * 2) % 3 == 1; });
  assertEqualVectors(expectedResult, result);
  VELOX_CHECK_EQ(stats["eq"].numProcessedRows, 300);
  VELOX_CHECK(!base.unique());

  auto everyFifth = makeIndices(100, [](auto row) { return row * 5; });
  std::tie(result, stats) = evaluateWithStats(
      exprSet.get(), makeRowVector({wrapInDictionary(everyFifth, 100, base)}));
  expectedResult =
      makeFlatVector<bool>(100, [](auto row) { return (row * 5) % 3 == 1; });
  assertEqualVectors(expectedResult, result);
  VELOX_CHECK_EQ(
      stats["eq"].numProcessedRows,
      360,
      "Fewer rows expected as memoization should have kicked in.");
  VELOX_CHECK(!base.unique());

  // Create a new base
  base = makeArrayVector<int64_t>(
      500,
      [](auto row) { return row % 5 + 1; },
      [](auto row, auto index) { return (row % 3) + index; });

  std::tie(result, stats) = evaluateWithStats(
      exprSet.get(), makeRowVector({wrapInDictionary(oddIndices, 100, base)}));
  expectedResult = makeFlatVector<bool>(
      100, [](auto row) { return (9 + row * 2) % 3 == 1; });
  assertEqualVectors(expectedResult, result);
  VELOX_CHECK_EQ(stats["eq"].numProcessedRows, 460);
  VELOX_CHECK(base.unique());
}

// This test triggers the situation when peelEncodings() produces an empty
// selectivity vector, which if passed to evalWithMemo() causes the latter to
// produce null Expr::dictionaryCache_, which leads to a crash in evaluation
// of subsequent rows. We have fixed that issue with condition and this test
// is for that.
TEST_P(ParameterizedExprTest, memoNulls) {
  // Generate 5 rows with null string and 5 with a string.
  auto base = makeFlatVector<StringView>(
      10, [](vector_size_t /*row*/) { return StringView("abcdefg"); });

  // Two batches by 5 rows each.
  auto first5Indices = makeIndices(5, [](auto row) { return row; });
  auto last5Indices = makeIndices(5, [](auto row) { return row + 5; });
  // Nulls for the 1st batch.
  BufferPtr nulls =
      AlignedBuffer::allocate<bool>(5, execCtx_->pool(), bits::kNull);

  auto rowType = ROW({"c0"}, {base->type()});
  auto exprSet = compileExpression("STRPOS(c0, 'abc') >= 0", rowType);

  auto result = evaluate(
      exprSet.get(),
      makeRowVector(
          {BaseVector::wrapInDictionary(nulls, first5Indices, 5, base)}));
  // Expecting 5 nulls.
  auto expectedResult =
      BaseVector::createNullConstant(BOOLEAN(), 5, execCtx_->pool());
  assertEqualVectors(expectedResult, result);

  result = evaluate(
      exprSet.get(), makeRowVector({wrapInDictionary(last5Indices, 5, base)}));
  // Expecting 5 trues.
  expectedResult = makeConstant(true, 5);
  assertEqualVectors(expectedResult, result);
}

// This test is carefully constructed to exercise calling
// applyFunctionWithPeeling in a situation where inputValues_ can be peeled
// and applyRows and rows are distinct SelectivityVectors.  This test ensures
// we're using applyRows and rows in the right places, if not we should see a
// SIGSEGV.
TEST_P(ParameterizedExprTest, peelNulls) {
  // Generate 5 distinct values for the c0 column.
  auto c0 = makeFlatVector<StringView>(5, [](vector_size_t row) {
    std::string val = "abcdefg";
    val.append(2, 'a' + row);
    return StringView(val);
  });
  // Generate 5 values for the c1 column.
  auto c1 = makeFlatVector<StringView>(
      5, [](vector_size_t /*row*/) { return StringView("xyz"); });

  // One batch of 5 rows.
  auto c0Indices = makeIndices(5, [](auto row) { return row; });
  auto c1Indices = makeIndices(5, [](auto row) { return row; });

  auto rowType = ROW({"c0", "c1"}, {c0->type(), c1->type()});
  // This expression is very deliberately written this way.
  // REGEXP_EXTRACT will return null for all but row 2, it is important we
  // get nulls and non-nulls so applyRows and rows will be distinct.
  // The result of REVERSE will be collapsed into a constant vector, which
  // is necessary so that the inputValues_ can be peeled.
  // REGEXP_LIKE is the function for which applyFunctionWithPeeling will be
  // called.
  auto exprSet = compileExpression(
      "REGEXP_LIKE(REGEXP_EXTRACT(c0, 'cc'), REVERSE(c1))", rowType);

  // It is important that both columns be wrapped in DictionaryVectors so
  // that they are not peeled until REGEXP_LIKE's children have been
  // evaluated.
  auto result = evaluate(
      exprSet.get(),
      makeRowVector(
          {BaseVector::wrapInDictionary(nullptr, c0Indices, 5, c0),
           BaseVector::wrapInDictionary(nullptr, c1Indices, 5, c1)}));

  // Since c0 only has 'cc' as a substring in row 2, all other rows should be
  // null.
  auto expectedResult = makeFlatVector<bool>(
      5,
      [](vector_size_t /*row*/) { return false; },
      [](vector_size_t row) { return row != 2; });
  assertEqualVectors(expectedResult, result);
}

TEST_P(ParameterizedExprTest, peelLazyDictionaryOverConstant) {
  auto c0 = makeFlatVector<int64_t>(5, [](vector_size_t row) { return row; });
  auto c0Indices = makeIndices(5, [](auto row) { return row; });
  auto c1 = makeFlatVector<int64_t>(5, [](auto row) { return row; });

  auto result = evaluate(
      "if (not(is_null(if (c0 >= 0, c1, cast (null as bigint)))), coalesce(c0, 22), cast (null as bigint))",
      makeRowVector(
          {BaseVector::wrapInDictionary(
               nullptr, c0Indices, 5, wrapInLazyDictionary(c0)),
           BaseVector::wrapInDictionary(
               nullptr, c0Indices, 5, wrapInLazyDictionary(c1))}));
  assertEqualVectors(c0, result);
}

TEST_P(ParameterizedExprTest, accessNested) {
  // Construct row(row(row(integer))) vector.
  auto base = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  auto level1 = makeRowVector({base});
  auto level2 = makeRowVector({level1});
  auto level3 = makeRowVector({level2});

  // Access level3->level2->level1->base.
  // TODO: Expression "c0.c0.c0" currently not supported by DuckDB
  // So we wrap with parentheses to force parsing as struct extract
  // Track https://github.com/duckdb/duckdb/issues/2568
  auto result = evaluate("(c0).c0.c0.c0", makeRowVector({level3}));

  assertEqualVectors(base, result);
}

TEST_P(ParameterizedExprTest, accessNestedNull) {
  // Construct row(row(row(integer))) vector.
  auto base = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  auto level1 = makeRowVector({base});

  // Construct level 2 row with nulls.
  auto level2 = makeRowVector({level1}, nullEvery(2));
  auto level3 = makeRowVector({level2});

  auto result = evaluate("(c0).c0.c0.c0", makeRowVector({level3}));
  auto expected = makeNullableFlatVector<int32_t>(
      {std::nullopt, 2, std::nullopt, 4, std::nullopt});
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, accessNestedDictionaryEncoding) {
  // Construct row(row(row(integer))) vector.
  auto base = makeFlatVector<int32_t>({1, 2, 3, 4, 5});

  // Reverse order in dictionary encoding.
  auto indices = makeIndicesInReverse(5);

  auto level1 = makeRowVector({base});
  auto level2 = makeRowVector({wrapInDictionary(indices, 5, level1)});
  auto level3 = makeRowVector({level2});

  auto result = evaluate("(c0).c0.c0.c0", makeRowVector({level3}));

  assertEqualVectors(makeFlatVector<int32_t>({5, 4, 3, 2, 1}), result);
}

TEST_P(ParameterizedExprTest, accessNestedConstantEncoding) {
  // Construct row(row(row(integer))) vector.
  VectorPtr base = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  // Wrap base in constant.
  base = BaseVector::wrapInConstant(5, 2, base);

  auto level1 = makeRowVector({base});
  auto level2 = makeRowVector({level1});
  auto level3 = makeRowVector({level2});

  auto result = evaluate("(c0).c0.c0.c0", makeRowVector({level3}));

  assertEqualVectors(makeConstant(3, 5), result);
}

TEST_P(ParameterizedExprTest, testEmptyVectors) {
  auto a = makeFlatVector<int32_t>({});
  auto result = evaluate("c0 + c0", makeRowVector({a, a}));
  assertEqualVectors(a, result);
}

TEST_P(ParameterizedExprTest, subsetOfDictOverLazy) {
  // We have dictionaries over LazyVector. We load for some indices in
  // the top dictionary. The intermediate dictionaries refer to
  // non-loaded items in the base of the LazyVector, including indices
  // past its end. We check that end result is a valid vector.

  auto makeLoaded = [&]() {
    return makeFlatVector<int32_t>(100, [](auto row) { return row; });
  };

  auto lazy = std::make_shared<LazyVector>(
      execCtx_->pool(),
      INTEGER(),
      1000,
      std::make_unique<test::SimpleVectorLoader>(
          [&](auto /*size*/) { return makeLoaded(); }));
  auto row = makeRowVector({BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(100, [](auto row) { return row; }),
      100,
      BaseVector::wrapInDictionary(
          nullptr,
          makeIndices(1000, [](auto row) { return row; }),
          1000,
          lazy))});

  auto result = evaluate("c0", row);
  assertEqualVectors(result, makeLoaded());
  // The loader will ensure that the loaded vector size is 1000.
  auto base =
      result->as<DictionaryVector<int32_t>>()->valueVector()->loadedVector();
  EXPECT_EQ(base->size(), 1000);
}

TEST_P(ParameterizedExprTest, peeledConstant) {
  constexpr int32_t kSubsetSize = 80;
  constexpr int32_t kBaseSize = 160;
  auto indices = makeIndices(kSubsetSize, [](auto row) { return row * 2; });
  auto numbers =
      makeFlatVector<int32_t>(kBaseSize, [](auto row) { return row; });
  auto row = makeRowVector({
      wrapInDictionary(indices, kSubsetSize, numbers),
      makeConstant("Hans Pfaal", kBaseSize),
  });
  auto result = std::dynamic_pointer_cast<SimpleVector<StringView>>(
      evaluate("if (c0 % 4 = 0, c1, cast (null as VARCHAR))", row));
  EXPECT_EQ(kSubsetSize, result->size());
  for (auto i = 0; i < kSubsetSize; ++i) {
    if (result->isNullAt(i)) {
      continue;
    }
    EXPECT_LE(1, result->valueAt(i).size());
    // Check that the data is readable.
    EXPECT_NO_THROW(result->toString(i));
  }
}

namespace {
// In general simple functions should not throw runtime errors but only user
// errors, since runtime errors are not suppressed with try. This is needed for
// the unit test.
template <typename T>
struct ThrowRuntimeError {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    // Throw runtime error,
    VELOX_FAIL();
  }
};
} // namespace

TEST_P(ParameterizedExprTest, exceptionContext) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<int32_t>({1, 2, 3}),
  });

  registerFunction<ThrowRuntimeError, int32_t, int32_t>({"runtime_error"});
  registerFunction<TestingAlwaysThrowsFunction, int32_t, int32_t>(
      {"always_throws"});

  // Disable saving vector and expression SQL on error.
  FLAGS_velox_save_input_on_expression_any_failure_path = "";
  FLAGS_velox_save_input_on_expression_system_failure_path = "";

  try {
    evaluate("always_throws(c0) + c1", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("always_throws(c0)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(always_throws(c0), c1)",
        e.additionalContext());
  }

  try {
    evaluate("c0 + (c0 + c1) % 0", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(cast((c0) as BIGINT), mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT))",
        e.additionalContext());
  }

  try {
    evaluate("c0 + (c1 % 0)", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((c1) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(cast((c0) as BIGINT), mod(cast((c1) as BIGINT), 0:BIGINT))",
        e.additionalContext());
  }

  // Enable saving vector and expression SQL for system errors only.
  auto tempDirectory = exec::test::TempDirectoryPath::create();
  FLAGS_velox_save_input_on_expression_system_failure_path =
      tempDirectory->getPath();

  try {
    evaluate("runtime_error(c0) + c1", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("runtime_error(c0)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(runtime_error(c0), c1)",
        trimInputPath(e.additionalContext()));
    verifyDataAndSqlPaths(e, data);
  }

  try {
    evaluate("c0 + (c0 + c1) % 0", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(cast((c0) as BIGINT), mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT))",
        e.additionalContext())
        << e.errorSource();
  }

  try {
    evaluate("c0 + (c0 + c1) % 0", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(cast((c0) as BIGINT), mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT))",
        e.additionalContext());
  }

  // Enable saving vector and expression SQL for all errors.
  FLAGS_velox_save_input_on_expression_any_failure_path =
      tempDirectory->getPath();
  FLAGS_velox_save_input_on_expression_system_failure_path = "";

  try {
    evaluate("always_throws(c0) + c1", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("always_throws(c0)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(always_throws(c0), c1)",
        trimInputPath(e.additionalContext()));
    verifyDataAndSqlPaths(e, data);
  }

  try {
    evaluate("c0 + (c0 + c1) % 0", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(cast((c0) as BIGINT), mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT))",
        trimInputPath(e.additionalContext()));
    verifyDataAndSqlPaths(e, data);
  }

  try {
    evaluate("c0 + (c1 % 0)", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((c1) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(cast((c0) as BIGINT), mod(cast((c1) as BIGINT), 0:BIGINT))",
        trimInputPath(e.additionalContext()));
    verifyDataAndSqlPaths(e, data);
  }

  try {
    evaluateMultiple({"c0 + (c1 % 0)", "c0 + c1"}, data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((c1) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "Top-level Expression: plus(cast((c0) as BIGINT), mod(cast((c1) as BIGINT), 0:BIGINT))",
        trimInputPath(e.additionalContext()));
    verifyDataAndSqlPaths(e, data);
  }
}

namespace {

template <typename T>
struct AlwaysThrowsStdExceptionFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    throw std::invalid_argument("This is a test");
  }
};
} // namespace

/// Verify exception context for the case when function throws std::exception.
TEST_P(ParameterizedExprTest, stdExceptionContext) {
  auto data = makeFlatVector<int64_t>({1, 2, 3});

  registerFunction<AlwaysThrowsStdExceptionFunction, int64_t, int64_t>(
      {"throw_invalid_argument"});

  auto wrappedEx = assertError(
      "throw_invalid_argument(c0) + 5",
      data,
      "throw_invalid_argument(c0)",
      "Top-level Expression: plus(throw_invalid_argument(c0), 5:BIGINT)",
      "This is a test");
  ASSERT_THROW(std::rethrow_exception(wrappedEx), std::invalid_argument);

  wrappedEx = assertError(
      "throw_invalid_argument(c0 + 5)",
      data,
      "Top-level Expression: throw_invalid_argument(plus(c0, 5:BIGINT))",
      "",
      "This is a test");
  ASSERT_THROW(std::rethrow_exception(wrappedEx), std::invalid_argument);
}

/// Verify the output of ConstantExpr::toString().
TEST_P(ParameterizedExprTest, constantToString) {
  auto arrayVector =
      makeNullableArrayVector<float>({{1.2, 3.4, std::nullopt, 5.6}});

  exec::ExprSet exprSet(
      {std::make_shared<core::ConstantTypedExpr>(INTEGER(), 23),
       std::make_shared<core::ConstantTypedExpr>(
           DOUBLE(), variant::null(TypeKind::DOUBLE)),
       makeConstantExpr(arrayVector, 0)},
      execCtx_.get());

  ASSERT_EQ("23:INTEGER", exprSet.exprs()[0]->toString());
  ASSERT_EQ("null:DOUBLE", exprSet.exprs()[1]->toString());
  ASSERT_EQ(
      "4 elements starting at 0 {1.2000000476837158, 3.4000000953674316, null, 5.599999904632568}:ARRAY<REAL>",
      exprSet.exprs()[2]->toString());
}

TEST_P(ParameterizedExprTest, fieldAccessToString) {
  auto rowType =
      ROW({"a", "b"},
          {BIGINT(), ROW({"c", "d"}, {DOUBLE(), ROW({"e"}, {VARCHAR()})})});

  exec::ExprSet exprSet(
      {std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "a"),
       std::make_shared<core::FieldAccessTypedExpr>(
           DOUBLE(),
           std::make_shared<core::FieldAccessTypedExpr>(
               ROW({"c", "d"}, {DOUBLE(), ROW({"e"}, {VARCHAR()})}), "b"),
           "c"),
       std::make_shared<core::FieldAccessTypedExpr>(
           VARCHAR(),
           std::make_shared<core::FieldAccessTypedExpr>(
               ROW({"e"}, {VARCHAR()}),
               std::make_shared<core::FieldAccessTypedExpr>(
                   ROW({"c", "d"}, {DOUBLE(), ROW({"e"}, {VARCHAR()})}), "b"),
               "d"),
           "e")},
      execCtx_.get());

  ASSERT_EQ("a", exprSet.exprs()[0]->toString());
  ASSERT_EQ("a", exprSet.exprs()[0]->toString(/*recursive*/ false));
  ASSERT_EQ("(b).c", exprSet.exprs()[1]->toString());
  ASSERT_EQ("c", exprSet.exprs()[1]->toString(/*recursive*/ false));
  ASSERT_EQ("((b).d).e", exprSet.exprs()[2]->toString());
  ASSERT_EQ("e", exprSet.exprs()[2]->toString(/*recursive*/ false));
}

TEST_P(ParameterizedExprTest, constantToSql) {
  auto toSql = [&](const variant& value, const TypePtr& type = nullptr) {
    exec::ExprSet exprSet({makeConstantExpr(value, type)}, execCtx_.get());
    auto sql = exprSet.expr(0)->toSql();

    auto input = makeRowVector(ROW({}), 1);
    auto a = evaluate(&exprSet, input);
    auto b = evaluate(sql, input);

    if (a->type()->containsUnknown()) {
      EXPECT_TRUE(a->isNullAt(0));
      EXPECT_TRUE(b->isNullAt(0));
    } else {
      assertEqualVectors(a, b);
    }

    return sql;
  };

  ASSERT_EQ(toSql(true), "TRUE");
  ASSERT_EQ(toSql(false), "FALSE");
  ASSERT_EQ(toSql(variant::null(TypeKind::BOOLEAN)), "NULL::BOOLEAN");

  ASSERT_EQ(toSql((int8_t)23), "'23'::TINYINT");
  ASSERT_EQ(toSql(variant::null(TypeKind::TINYINT)), "NULL::TINYINT");

  ASSERT_EQ(toSql((int16_t)23), "'23'::SMALLINT");
  ASSERT_EQ(toSql(variant::null(TypeKind::SMALLINT)), "NULL::SMALLINT");

  ASSERT_EQ(toSql(23), "'23'::INTEGER");
  ASSERT_EQ(toSql(variant::null(TypeKind::INTEGER)), "NULL::INTEGER");

  ASSERT_EQ(toSql(2134456LL), "'2134456'::BIGINT");
  ASSERT_EQ(toSql(variant::null(TypeKind::BIGINT)), "NULL::BIGINT");

  ASSERT_EQ(toSql(18'506, DATE()), "'2020-09-01'::DATE");
  ASSERT_EQ(toSql(variant::null(TypeKind::INTEGER), DATE()), "NULL::DATE");

  ASSERT_EQ(toSql(2134456LL, DECIMAL(18, 2)), "'21344.56'::DECIMAL(18, 2)");
  ASSERT_EQ(
      toSql(variant::null(TypeKind::BIGINT), DECIMAL(18, 2)),
      "NULL::DECIMAL(18, 2)");
  ASSERT_EQ(
      toSql((int128_t)1'000'000'000'000'000'000, DECIMAL(38, 2)),
      "'10000000000000000.00'::DECIMAL(38, 2)");
  ASSERT_EQ(
      toSql(variant::null(TypeKind::HUGEINT), DECIMAL(38, 2)),
      "NULL::DECIMAL(38, 2)");

  ASSERT_EQ(
      toSql(Timestamp(123'456, 123'000)),
      "'1970-01-02 10:17:36.000123000'::TIMESTAMP");
  ASSERT_EQ(toSql(variant::null(TypeKind::TIMESTAMP)), "NULL::TIMESTAMP");

  ASSERT_EQ(
      toSql(123'456LL, INTERVAL_DAY_TIME()), "INTERVAL 123456 MILLISECOND");
  ASSERT_EQ(
      toSql(variant::null(TypeKind::BIGINT), INTERVAL_DAY_TIME()),
      "NULL::INTERVAL DAY TO SECOND");

  ASSERT_EQ(toSql(1.5f), "'1.5'::REAL");
  ASSERT_EQ(toSql(variant::null(TypeKind::REAL)), "NULL::REAL");

  ASSERT_EQ(toSql(-78.456), "'-78.456'::DOUBLE");
  ASSERT_EQ(toSql(variant::null(TypeKind::DOUBLE)), "NULL::DOUBLE");

  ASSERT_EQ(toSql("This is a test."), "'This is a test.'");
  ASSERT_EQ(
      toSql("This is a \'test\' with single quotes."),
      "'This is a \'\'test\'\' with single quotes.'");
  ASSERT_EQ(toSql(variant::null(TypeKind::VARCHAR)), "NULL::VARCHAR");

  auto toSqlComplex = [&](const VectorPtr& vector, vector_size_t index = 0) {
    exec::ExprSet exprSet({makeConstantExpr(vector, index)}, execCtx_.get());
    auto sql = exprSet.expr(0)->toSql();

    auto input = makeRowVector(ROW({}), 1);
    auto a = evaluate(&exprSet, input);
    auto b = evaluate(sql, input);

    if (a->type()->containsUnknown()) {
      EXPECT_TRUE(a->isNullAt(0));
      EXPECT_TRUE(b->isNullAt(0));
    } else {
      assertEqualVectors(a, b);
    }

    return sql;
  };

  ASSERT_EQ(
      toSqlComplex(makeArrayVector<int32_t>({{1, 2, 3}})),
      "ARRAY['1'::INTEGER, '2'::INTEGER, '3'::INTEGER]");
  ASSERT_EQ(
      toSqlComplex(makeArrayVector<int32_t>({{1, 2, 3}, {4, 5, 6}}), 1),
      "ARRAY['4'::INTEGER, '5'::INTEGER, '6'::INTEGER]");
  ASSERT_EQ(toSql(variant::null(TypeKind::ARRAY)), "NULL");

  ASSERT_EQ(
      toSqlComplex(makeMapVector<int32_t, int32_t>({
          {{1, 10}, {2, 20}, {3, 30}},
      })),
      "map(ARRAY['1'::INTEGER, '2'::INTEGER, '3'::INTEGER], ARRAY['10'::INTEGER, '20'::INTEGER, '30'::INTEGER])");
  ASSERT_EQ(
      toSqlComplex(
          makeMapVector<int32_t, int32_t>({
              {{1, 11}, {2, 12}},
              {{1, 10}, {2, 20}, {3, 30}},
          }),
          1),
      "map(ARRAY['1'::INTEGER, '2'::INTEGER, '3'::INTEGER], ARRAY['10'::INTEGER, '20'::INTEGER, '30'::INTEGER])");
  ASSERT_EQ(
      toSqlComplex(BaseVector::createNullConstant(
          MAP(INTEGER(), VARCHAR()), 10, pool())),
      "NULL::MAP(INTEGER, VARCHAR)");

  ASSERT_EQ(
      toSqlComplex(makeRowVector({
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<bool>({true, false, true}),
      })),
      "row_constructor('1'::INTEGER, TRUE)");
  ASSERT_EQ(
      toSqlComplex(BaseVector::createNullConstant(
          ROW({"a", "b"}, {BOOLEAN(), DOUBLE()}), 10, pool())),
      "NULL::STRUCT(a BOOLEAN, b DOUBLE)");
  ASSERT_EQ(
      toSqlComplex(BaseVector::createNullConstant(
          ROW({"a", "b"}, {BOOLEAN(), ROW({"c", "d"}, {DOUBLE(), VARCHAR()})}),
          10,
          pool())),
      "NULL::STRUCT(a BOOLEAN, b STRUCT(c DOUBLE, d VARCHAR))");
}

TEST_P(ParameterizedExprTest, constantJsonToSql) {
  core::TypedExprPtr expression = std::make_shared<const core::CallTypedExpr>(
      VARCHAR(),
      std::vector<core::TypedExprPtr>{makeConstantExpr("[1, 2, 3]", JSON())},
      "json_format");

  auto exprSet = compileNoConstantFolding(expression);

  std::vector<VectorPtr> complexConstants;
  auto sql = exprSet->expr(0)->toSql(&complexConstants);

  auto copy =
      compileNoConstantFolding(sql, ROW({}), makeRowVector(complexConstants));
  ASSERT_EQ(
      exprSet->toString(false /*compact*/), copy->toString(false /*compact*/))
      << sql;
}

TEST_P(ParameterizedExprTest, lambdaToSql) {
  auto exprSet = compileNoConstantFolding(
      "transform(array[1, 2, 3], x -> (x + cardinality(array[10, 20, 30])))",
      ROW({}));

  std::vector<VectorPtr> complexConstants;
  auto sql = exprSet->expr(0)->toSql(&complexConstants);

  auto copy =
      compileNoConstantFolding(sql, ROW({}), makeRowVector(complexConstants));
  ASSERT_EQ(
      exprSet->toString(false /*compact*/), copy->toString(false /*compact*/))
      << sql;
}

TEST_P(ParameterizedExprTest, toSql) {
  auto rowType =
      ROW({"a", "b", "c.d", "e", "f"},
          {INTEGER(),
           BIGINT(),
           VARCHAR(),
           ARRAY(BIGINT()),
           MAP(VARCHAR(), DOUBLE())});

  // CAST.
  testToSql("a + 3", rowType);
  testToSql("a * b", rowType);
  testToSql("a * 1.5", rowType);
  testToSql("cast(e as varchar[])", rowType);
  testToSql("cast(f as map(bigint, varchar))", rowType);
  testToSql(
      "cast(row_constructor(a, b) as struct(x bigint, y double))", rowType);

  // SWITCH.
  testToSql("if(a > 0, 1, 10)", rowType);
  testToSql("if(a = 10, true, false)", rowType);
  testToSql("case a when 7 then 1 when 11 then 2 else 0 end", rowType);
  testToSql("case a when 7 then 1 when 11 then 2 when 17 then 3 end", rowType);
  testToSql(
      "case a when b + 3 then 1 when b * 11 then 2 when b - 17 then a + b end",
      rowType);

  // AND / OR.
  testToSql("a > 0 AND b < 100", rowType);
  testToSql("a > 0 AND b / a < 100", rowType);
  testToSql("is_null(a) OR is_null(b)", rowType);
  testToSql("a > 10 AND (b > 100 OR a < 0) AND b < 3", rowType);

  // COALESCE.
  testToSql("coalesce(a::bigint, b, 123)", rowType);

  // TRY.
  testToSql("try(a / b)", rowType);

  // String literals.
  testToSql("length(\"c.d\")", rowType);
  testToSql("concat(a::varchar, ',', b::varchar, '\'\'')", rowType);

  // Array, map and row literals.
  testToSql("contains(array[1, 2, 3], a)", rowType);
  testToSql("map(array[a, b, 5], array[10, 20, 30])", rowType);
  testToSql(
      "element_at(map(array[1, 2, 3], array['a', 'b', 'c']), a)", rowType);
  testToSql("row_constructor(a, b, 'test')", rowType);
  testToSql("row_constructor(true, 1.5, 'abc', array[1, 2, 3])", rowType);

  // Lambda functions.
  testToSql("filter(e, x -> (x > 10))", rowType);
  testToSql("transform(e, x -> x + b)", rowType);
  testToSql("map_filter(f, (k, v) -> (v > 10::double))", rowType);
  testToSql("reduce(e, b, (s, x) -> s + x, s -> s * 10)", rowType);

  // Function without inputs.
  testToSql("pi()", rowType);

  // Field dereference.
  rowType = ROW({"o"}, {ROW({"i"}, {std::move(rowType)})});
  testToSql("o.i", rowType);
  testToSql("(o).i.e", rowType);
}

namespace {
// A naive function that wraps the input in a dictionary vector.
class WrapInDictionaryFunc : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(rows.end(), context.pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    rows.applyToSelected([&](int row) { rawIndices[row] = row; });

    result =
        BaseVector::wrapInDictionary(nullptr, indices, rows.end(), args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("bigint")
                .build()};
  }
};

class LastRowNullFunc : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, BIGINT(), result);
    auto* flatOutput = result->asFlatVector<int64_t>();
    auto* flatInput = args[0]->asFlatVector<int64_t>();
    rows.applyToSelected(
        [&](int row) { flatOutput->set(row, flatInput->valueAt(row)); });
    flatOutput->setNull(rows.end() - 1, true);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("bigint")
                .build()};
  }
};
} // namespace

TEST_P(ParameterizedExprTest, dictionaryResizedInAddNulls) {
  exec::registerVectorFunction(
      "dict_wrap",
      WrapInDictionaryFunc::signatures(),
      std::make_unique<WrapInDictionaryFunc>(),
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());

  exec::registerVectorFunction(
      "last_row_null",
      LastRowNullFunc::signatures(),
      std::make_unique<LastRowNullFunc>(),
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build());

  // This test verifies an edge case where applyFunctionWithPeeling may produce
  // a result vector which is dictionary encoded and has fewer values than
  // are rows.
  // The expression bellow make sure that we call a resize on a dictonary
  // vector during addNulls after function `dict_wrap` is evaluated.

  // Making the last rows NULL, so we call addNulls in eval.
  auto c0 = makeFlatVector<int64_t>({1, 2, 3, 4, 5});

  auto evalResult =
      evaluate("dict_wrap(last_row_null(c0))", makeRowVector({c0}));
  auto expected = c0;
  expected->setNull(4, true);
  assertEqualVectors(expected, evalResult);
}

TEST_P(ParameterizedExprTest, tryWithConstantFailure) {
  // This test verifies the behavior of constant peeling on a function wrapped
  // in a TRY.  Specifically the case when the UDF executed on the peeled
  // vector throws an exception on the constant value.

  // When wrapping a peeled ConstantVector, the result is wrapped in a
  // ConstantVector.  ConstantVector has special handling logic to copy the
  // underlying string when the type is Varchar.  When an exception is thrown
  // and the StringView isn't initialized, without special handling logic in
  // EvalCtx this results in reading uninitialized memory triggering ASAN
  // errors.
  registerFunction<TestingAlwaysThrowsFunction, Varchar, Varchar>(
      {"always_throws"});
  auto c0 = makeConstant("test", 5);
  auto c1 = makeFlatVector<int64_t>(5, [](vector_size_t row) { return row; });
  auto rowVector = makeRowVector({c0, c1});

  // We use strpos and c1 to ensure that the constant is peeled before calling
  // always_throws, not before the try.
  auto evalResult =
      evaluate("try(strpos(always_throws(\"c0\"), 't', c1))", rowVector);

  auto expectedResult = makeFlatVector<int64_t>(
      5, [](vector_size_t) { return 0; }, [](vector_size_t) { return true; });
  assertEqualVectors(expectedResult, evalResult);
}

TEST_P(ParameterizedExprTest, castExceptionContext) {
  assertError(
      "cast(c0 as bigint)",
      makeFlatVector<std::string>({"1a"}),
      "Top-level Expression: cast((c0) as BIGINT)",
      "",
      "Cannot cast VARCHAR '1a' to BIGINT. Non-whitespace character found after end of conversion: \"\"");

  assertError(
      "cast(c0 as timestamp)",
      makeFlatVector(std::vector<int8_t>{1}),
      "Top-level Expression: cast((c0) as TIMESTAMP)",
      "",
      "Cannot cast TINYINT '1' to TIMESTAMP. Conversion to Timestamp is not supported");
}

TEST_P(ParameterizedExprTest, switchExceptionContext) {
  assertError(
      "case c0 when 7 then c0 / 0 else 0 end",
      makeFlatVector(std::vector<int64_t>{7}),
      "divide(c0, 0:BIGINT)",
      "Top-level Expression: switch(eq(c0, 7:BIGINT), divide(c0, 0:BIGINT), 0:BIGINT)",
      "division by zero");
}

TEST_P(ParameterizedExprTest, conjunctExceptionContext) {
  auto data = makeFlatVector<int64_t>(20, [](auto row) { return row; });

  assertError(
      "if (c0 % 409 < 300 and c0 / 0 < 30, 1, 2)",
      data,
      "divide(c0, 0:BIGINT)",
      "Top-level Expression: switch(and(lt(mod(c0, 409:BIGINT), 300:BIGINT), lt(divide(c0, 0:BIGINT), 30:BIGINT)), 1:BIGINT, 2:BIGINT)",
      "division by zero");
}

TEST_P(ParameterizedExprTest, lambdaExceptionContext) {
  auto array = makeArrayVector<int64_t>(
      10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; });

  assertError(
      "filter(c0, x -> (x / 0 > 1))",
      array,
      "divide(x, 0:BIGINT)",
      "Top-level Expression: filter(c0, (x) -> gt(divide(x, 0:BIGINT), 1:BIGINT))",
      "division by zero");
}

/// Verify that null inputs result in exceptions, not crashes.
TEST_P(ParameterizedExprTest, invalidInputs) {
  auto rowType = ROW({"a"}, {BIGINT()});
  auto exprSet = compileExpression("a + 5", rowType);

  // Try null top-level vector.
  RowVectorPtr input;
  ASSERT_THROW(
      exec::EvalCtx(execCtx_.get(), exprSet.get(), input.get()),
      VeloxRuntimeError);

  // Try non-null vector with null children.
  input = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, 1024, std::vector<VectorPtr>{nullptr});
  ASSERT_THROW(
      exec::EvalCtx(execCtx_.get(), exprSet.get(), input.get()),
      VeloxRuntimeError);
}

TEST_P(ParameterizedExprTest, lambdaWithRowField) {
  auto array = makeArrayVector<int64_t>(
      10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; });
  auto row = makeRowVector(
      {"val"},
      {makeFlatVector<int64_t>(10, [](vector_size_t row) { return row; })});

  auto rowVector = makeRowVector({"c0", "c1"}, {row, array});

  // We use strpos and c1 to ensure that the constant is peeled before calling
  // always_throws, not before the try.
  auto evalResult = evaluate("filter(c1, x -> (x + c0.val >= 0))", rowVector);

  assertEqualVectors(array, evalResult);
}

TEST_P(ParameterizedExprTest, flatNoNullsFastPath) {
  auto data = makeRowVector(
      {"a", "b", "c", "d"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int32_t>({10, 20, 30}),
          makeFlatVector<float>({0.1, 0.2, 0.3}),
          makeFlatVector<float>({-1.2, 0.0, 10.67}),
      });
  auto rowType = asRowType(data->type());

  // Basic math expressions.

  auto exprSet = compileExpression("a + b", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  auto expectedResult = makeFlatVector<int32_t>({11, 22, 33});
  auto result = evaluate(exprSet.get(), data);
  assertEqualVectors(expectedResult, result);

  exprSet = compileExpression("a + b * 5::integer", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  exprSet = compileExpression("floor(c * 1.34::real) / d", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Switch expressions.

  exprSet = compileExpression("if (a > 10::integer, 0::integer, b)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // If statement with 'then' or 'else' branch that can return null does not
  // support fast path.
  exprSet = compileExpression(
      "if (a > 10::integer, 0, cast (null as bigint))", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  exprSet = compileExpression(
      "case when a > 10::integer then 1 when b > 10::integer then 2 else 3 end",
      rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Switch without an else clause doesn't support fast path.
  exprSet = compileExpression(
      "case when a > 10::integer then 1 when b > 10::integer then 2 end",
      rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // AND / OR expressions.

  exprSet = compileExpression("a > 10::integer AND b < 0::integer", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  exprSet = compileExpression(
      "a > 10::integer OR (b % 7::integer == 4::integer)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Coalesce expression.

  exprSet = compileExpression("coalesce(a, b)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Multiplying an integer by a double requires a cast, but cast doesn't
  // support fast path.
  exprSet = compileExpression("a * 0.1 + b", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Try expression doesn't support fast path.
  exprSet = compileExpression("try(a / b)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Field dereference.
  exprSet = compileExpression("a", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath());

  exprSet = compileExpression("a.c0", ROW({"a"}, {ROW({"c0"}, {INTEGER()})}));
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath());
}

TEST_P(ParameterizedExprTest, commonSubExpressionWithEncodedInput) {
  // This test case does a sanity check of the code path that re-uses
  // precomputed results for common sub-expressions.
  auto data = makeRowVector(
      {makeFlatVector<int64_t>({1, 1, 2, 2}),
       makeFlatVector<int64_t>({10, 10, 20, 20}),
       wrapInDictionary(
           makeIndices({0, 1, 2, 3}),
           4,
           wrapInDictionary(
               makeIndices({0, 1, 1, 0}),
               4,
               makeFlatVector<int64_t>({1, 2, 3, 4}))),
       makeConstant<int64_t>(1, 4)});

  // Case 1: When the input to the common sub-expression is a dictionary.
  // c2 > 1 is a common sub-expression. It is used in 3 top-level expressions.
  // In the first expression, c2 > 1 is evaluated for rows 2, 3.
  // In the second expression, c2 > 1 is evaluated for rows 0, 1.
  // In the third expression. c2 > 1 returns pre-computed results for rows 2, 3
  auto results = makeRowVector(evaluateMultiple(
      {"c0 = 2 AND c2 > 1", "c0 = 1 AND c2 > 1", "c1 = 20 AND c2 > 1"}, data));
  auto expectedResults = makeRowVector(
      {makeFlatVector<bool>({false, false, true, false}),
       makeFlatVector<bool>({false, true, false, false}),
       makeFlatVector<bool>({false, false, true, false})});
  assertEqualVectors(expectedResults, results);

  // Case 2: When the input to the common sub-expression is a constant.
  results = makeRowVector(evaluateMultiple(
      {"c0 = 2 AND c3 > 3", "c0 = 1 AND c3 > 3", "c1 = 20 AND c3 > 3"}, data));
  expectedResults = makeRowVector(
      {makeFlatVector<bool>({false, false, false, false}),
       makeFlatVector<bool>({false, false, false, false}),
       makeFlatVector<bool>({false, false, false, false})});
  assertEqualVectors(expectedResults, results);

  // Case 3: When cached rows in sub-expression are not present in final
  // selection.
  // In the first expression, c2 > 1 is evaluated for rows 2, 3.
  // In the second expression, c0 = 1 filters out row 2, 3 and the OR
  // expression sets the final selection to rows 0, 1. Finally, c2 > 1 is
  // evaluated for rows 0, 1. If finalSelection was not updated to the union of
  // cached rows and the existing finalSelection then the vector containing
  // cached values would have been override.
  // In the third expression. c2 > 1 returns pre-computed results for rows 3, 4
  // verifying that the vector containing cached values was not overridden.
  results = makeRowVector(evaluateMultiple(
      {"c0 = 2 AND c2 > 1",
       "c0 = 1 AND ( c1 = 20 OR c2 > 1 )",
       "c1 = 20 AND c2 > 1"},
      data));
  expectedResults = makeRowVector(
      {makeFlatVector<bool>({false, false, true, false}),
       makeFlatVector<bool>({false, true, false, false}),
       makeFlatVector<bool>({false, false, true, false})});
  assertEqualVectors(expectedResults, results);
}

TEST_P(ParameterizedExprTest, preservePartialResultsWithEncodedInput) {
  // This test verifies that partially populated results are preserved when the
  // input contains an encoded vector. We do this by using an if statement where
  // partial results are passed between its children expressions based on the
  // condition.
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6}),
      wrapInDictionary(
          makeIndices({0, 1, 2, 0, 1, 2}),
          6,
          makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6})),
  });

  // Create an expression which divides the input to be processed equally
  // between two different expressions.
  auto result = evaluate("if(c0 > 3, 7, c1 + 100)", data);
  assertEqualVectors(makeFlatVector<int64_t>({101, 102, 103, 7, 7, 7}), result);
}

// Verify code paths in Expr::applyFunctionWithPeeling for the case when one
// input is a constant of size N, while another input is a dictionary of size N
// over base vector with size > N. After peeling encodings, first input has size
// N, while second input has size > N (the size of the base vector). The
// translated set of rows now contains row numbers > N, hence, constant input
// needs to be resized, otherwise, accessing rows numbers > N will cause an
// error.
TEST_P(ParameterizedExprTest, peelIntermediateResults) {
  auto data = makeRowVector({makeArrayVector<int32_t>({
      {0, 1, 2, 3, 4, 5, 6, 7},
      {0, 1, 2, 33, 4, 5, 6, 7, 8},
  })});

  // element_at(c0, 200) returns a constant null of size 2.
  // element_at(c0, 4) returns a dictionary vector with base vector of size 17
  // (the size of the array's elements vector) and indices [0, 8].
  auto result = evaluate(
      "array_constructor(element_at(c0, 200), element_at(c0, 4))", data);
  auto expected = makeNullableArrayVector<int32_t>({
      {std::nullopt, 3},
      {std::nullopt, 33},
  });
  assertEqualVectors(expected, result);

  // Change the order of arguments.
  result = evaluate(
      "array_constructor(element_at(c0, 4), element_at(c0, 200))", data);

  expected = makeNullableArrayVector<int32_t>({
      {3, std::nullopt},
      {33, std::nullopt},
  });
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, peelWithDefaultNull) {
  // dict vector is [null, "b", null, "a", null, null].
  auto base =
      makeNullableFlatVector<StringView>({"a"_sv, "b"_sv, std::nullopt});
  auto indices = makeIndices({0, 1, 2, 0, 1, 2});
  auto nulls = makeNulls(6, nullEvery(2));
  auto dict = BaseVector::wrapInDictionary(nulls, indices, 6, base);
  auto data = makeRowVector({dict});

  // After peeling, to_utf8 is only evaluated on rows 0 and 1 in base vector.
  // The result is then wrapped with the dictionary encoding. Unevaluated rows
  // should be filled with nulls.
  auto result =
      evaluate("distinct_from(to_utf8('xB60ChtE03'),to_utf8(c0))", data);
  auto expected =
      makeNullableFlatVector<bool>({true, true, true, true, true, true});
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, addNulls) {
  const vector_size_t kSize = 6;
  SelectivityVector rows{kSize + 1};
  rows.setValid(kSize, false);
  rows.updateBounds();

  auto nulls = allocateNulls(kSize, pool());
  auto* rawNulls = nulls->asMutable<uint64_t>();
  bits::setNull(rawNulls, kSize - 1);

  exec::EvalCtx context(execCtx_.get());

  auto checkConstantResult = [&](const VectorPtr& vector) {
    ASSERT_TRUE(vector->isConstantEncoding());
    ASSERT_EQ(vector->size(), kSize);
    ASSERT_TRUE(vector->isNullAt(0));
  };

  // Test vector that is nullptr.
  {
    VectorPtr vector;
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), vector);
    ASSERT_NE(vector, nullptr);
    checkConstantResult(vector);
  }

  // Test vector that is already a constant null vector and is uniquely
  // referenced.
  {
    auto vector = makeNullConstant(TypeKind::BIGINT, kSize - 1);
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), vector);
    checkConstantResult(vector);
  }

  // Test vector that is already a constant null vector and is not uniquely
  // referenced.
  {
    auto vector = makeNullConstant(TypeKind::BIGINT, kSize - 1);
    auto another = vector;
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), vector);
    ASSERT_EQ(another->size(), kSize - 1);
    checkConstantResult(vector);
  }

  // Test vector that is a non-null constant vector.
  {
    auto vector = makeConstant<int64_t>(100, kSize - 1);
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), vector);
    ASSERT_TRUE(vector->isFlatEncoding());
    ASSERT_EQ(vector->size(), kSize);
    for (auto i = 0; i < kSize - 1; ++i) {
      ASSERT_FALSE(vector->isNullAt(i));
      ASSERT_EQ(vector->asFlatVector<int64_t>()->valueAt(i), 100);
    }
    ASSERT_TRUE(vector->isNullAt(kSize - 1));
  }

  auto checkResult = [&](const VectorPtr& vector) {
    ASSERT_EQ(vector->size(), kSize);
    for (auto i = 0; i < kSize - 1; ++i) {
      ASSERT_FALSE(vector->isNullAt(i));
      ASSERT_EQ(vector->asFlatVector<int64_t>()->valueAt(i), i);
    }
    ASSERT_TRUE(vector->isNullAt(kSize - 1));
  };

  // Test vector that is not uniquely referenced.
  {
    VectorPtr vector =
        makeFlatVector<int64_t>(kSize - 1, [](auto row) { return row; });
    auto another = vector;
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), vector);

    ASSERT_EQ(another->size(), kSize - 1);
    checkResult(vector);
  }

  // Test vector that is uniquely referenced.
  {
    VectorPtr vector =
        makeFlatVector<int64_t>(kSize - 1, [](auto row) { return row; });
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), vector);

    checkResult(vector);
  }

  // Test flat vector which has a shared values buffer. This is done by first
  // slicing the vector which creates buffer views of its nulls and values
  // buffer which are immutable.
  {
    VectorPtr vector =
        makeFlatVector<int64_t>(kSize, [](auto row) { return row; });
    auto slicedVector = vector->slice(0, kSize - 1);
    ASSERT_FALSE(slicedVector->values()->isMutable());
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), slicedVector);

    checkResult(slicedVector);
  }

  // Lazy reading sometimes generates row vector that has child with length
  // shorter than the parent.  The extra rows in parent are all marked as nulls
  // so it is valid.  We need to handle this situation when propagating nulls
  // from parent to child.
  {
    auto a = makeFlatVector<int64_t>(kSize - 1, folly::identity);
    auto b = makeArrayVector<int64_t>(
        kSize - 1, [](auto) { return 1; }, [](auto i) { return i; });
    auto row = std::make_shared<RowVector>(
        pool_.get(),
        ROW({{"a", a->type()}, {"b", b->type()}}),
        nullptr,
        kSize,
        std::vector<VectorPtr>({a, b}));
    row->setNull(kSize - 1, true);
    VectorPtr result = row;
    exec::EvalCtx::addNulls(rows, rawNulls, context, row->type(), result);
    ASSERT_NE(result.get(), row.get());
    ASSERT_EQ(result->size(), kSize);
    for (int i = 0; i < kSize - 1; ++i) {
      ASSERT_FALSE(result->isNullAt(i));
      ASSERT_TRUE(result->equalValueAt(row.get(), i, i));
    }
    ASSERT_TRUE(result->isNullAt(kSize - 1));
  }

  // Make sure addNulls does not overwrite a shared dictionary vector indices.
  {
    BufferPtr sharedIndices =
        AlignedBuffer::allocate<vector_size_t>(6, context.pool());
    auto* mutableIndices = sharedIndices->asMutable<vector_size_t>();
    mutableIndices[0] = 1;
    mutableIndices[1] = 1;
    mutableIndices[2] = 1;

    // Vector of size 2 using only the first two indices of sharedIndices.
    auto wrappedVectorSmaller = BaseVector::wrapInDictionary(
        nullptr, sharedIndices, 2, makeFlatVector<int64_t>({1, 2, 3}));
    exec::EvalCtx::addNulls(
        SelectivityVector(3),
        rawNulls,
        context,
        BIGINT(),
        wrappedVectorSmaller);
    EXPECT_EQ(mutableIndices[2], 1);
  }

  // Make sure we dont overwrite the shared values_ buffer of a flatVector
  {
    // When the buffer needs to be reduced in size.
    auto otherVector =
        makeFlatVector<int64_t>(2 * kSize, [](auto row) { return row; });
    auto valuesBufferCurrentSize = otherVector->values()->size();
    VectorPtr input = std::make_shared<FlatVector<int64_t>>(
        context.pool(),
        BIGINT(),
        nullptr,
        kSize - 1,
        otherVector->values(),
        std::vector<BufferPtr>{});
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), input);
    checkResult(input);
    ASSERT_EQ(valuesBufferCurrentSize, otherVector->values()->size());
    ASSERT_NE(input->values(), otherVector->values());
  }

  {
    // When the buffer needs to be increased in size.
    auto otherVector =
        makeFlatVector<int64_t>(kSize - 1, [](auto row) { return row; });
    auto valuesBufferCurrentSize = otherVector->values()->size();
    VectorPtr input = std::make_shared<FlatVector<int64_t>>(
        context.pool(),
        BIGINT(),
        nullptr,
        kSize - 1,
        otherVector->values(),
        std::vector<BufferPtr>{});
    exec::EvalCtx::addNulls(rows, rawNulls, context, BIGINT(), input);
    checkResult(input);
    ASSERT_EQ(valuesBufferCurrentSize, otherVector->values()->size());
    ASSERT_NE(input->values(), otherVector->values());
  }

  // Verify that when adding nulls to a RowVector outside of its initial size,
  // we ensure that newly added rows outside of the initial size that are not
  // marked as null are still accessible.
  // The function expects an input vector of size 3 and adds a null at idx 5.
  auto testRowWithPartialSelection = [&](VectorPtr& rowVector) {
    ASSERT_EQ(rowVector->size(), 3);
    SelectivityVector localRows(5, false);

    // We do not set null to row three.
    localRows.setValid(4, true);
    localRows.updateBounds();
    auto nulls = allocateNulls(5, pool());
    auto* rawNulls = nulls->asMutable<uint64_t>();
    bits::setNull(rawNulls, 4, true);
    exec::EvalCtx::addNulls(
        localRows, rawNulls, context, rowVector->type(), rowVector);
    ASSERT_EQ(rowVector->size(), 5);

    rowVector->validate();
    // Unselected row does not need to be marked null, just has to be valid.
    ASSERT_FALSE(rowVector->isNullAt(3));
  };

  {
    // Test row vector with partial selection.
    VectorPtr rowVector = makeRowVector({makeFlatVector<int32_t>({0, 1, 2})});
    testRowWithPartialSelection(rowVector);
  }

  {
    // Test constant row vector with partial selection.
    VectorPtr rowVector =
        makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>{0})});
    auto constant = BaseVector::wrapInConstant(3, 0, rowVector);
    testRowWithPartialSelection(constant);
  }

  {
    // Test dictionary row vector with partial selection.
    VectorPtr rowVector =
        makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>{0})});
    auto dictionary = BaseVector::wrapInDictionary(
        nullptr, makeIndices({0, 0, 0}), 3, rowVector);
    testRowWithPartialSelection(dictionary);
  }
}

namespace {
class NoOpVectorFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& /* rows */,
      std::vector<VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      exec::EvalCtx& /* context */,
      VectorPtr& /* result */) const override {}

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("boolean")
                .argumentType("integer")
                .build()};
  }
};
} // namespace

TEST_P(ParameterizedExprTest, applyFunctionNoResult) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
  });

  exec::registerVectorFunction(
      "always_throws_vector_function",
      TestingAlwaysThrowsVectorFunction::signatures(),
      std::make_unique<TestingAlwaysThrowsVectorFunction>(true));

  // At various places in the code, we don't check if result has been set or
  // not.  Conjuncts have the nice property that they set throwOnError to
  // false and don't check if the result VectorPtr is nullptr.
  assertError(
      "always_throws_vector_function(c0) AND true",
      makeFlatVector<int32_t>({1, 2, 3}),
      "always_throws_vector_function(c0)",
      "Top-level Expression: and(always_throws_vector_function(c0), true:BOOLEAN)",
      TestingAlwaysThrowsVectorFunction::kVeloxErrorMessage);

  exec::registerVectorFunction(
      "no_op",
      NoOpVectorFunction::signatures(),
      std::make_unique<NoOpVectorFunction>());

  assertError(
      "no_op(c0) AND true",
      makeFlatVector<int32_t>({1, 2, 3}),
      "no_op(c0)",
      "Top-level Expression: and(no_op(c0), true:BOOLEAN)",
      "Function neither returned results nor threw exception.");
}

TEST_P(ParameterizedExprTest, mapKeysAndValues) {
  // Verify that the right size of maps and keys arrays are created. This is
  // done by executing eval with a selectivity vector larger than the size of
  // the input map but with the extra trailing rows marked invalid. Finally, if
  // map_keys/_values tried to create a larger result array (equivalent to
  // rows.size()) this will throw.
  vector_size_t vectorSize = 100;
  VectorPtr mapVector = std::make_shared<MapVector>(
      pool_.get(),
      MAP(BIGINT(), BIGINT()),
      makeNulls(vectorSize, nullEvery(3)),
      vectorSize,
      makeIndices(vectorSize, [](auto /* row */) { return 0; }),
      makeIndices(vectorSize, [](auto /* row */) { return 1; }),
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<int64_t>({10, 20, 30}));
  auto input = makeRowVector({mapVector});
  auto exprSet = compileMultiple(
      {"map_keys(c0)", "map_values(c0)"}, asRowType(input->type()));
  exec::EvalCtx context(execCtx_.get(), exprSet.get(), input.get());

  SelectivityVector rows(vectorSize + 1);
  rows.setValid(vectorSize, false);
  rows.updateBounds();
  std::vector<VectorPtr> result(2);
  ASSERT_NO_THROW(exprSet->eval(rows, context, result));
}

TEST_P(ParameterizedExprTest, maskErrorByNull) {
  // Checks that errors in arguments of null-propagating functions are ignored
  // for rows with at least one null.
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
       makeFlatVector<int32_t>({1, 0, 3, 0, 5, 6}),
       makeNullableFlatVector<int32_t>(
           {std::nullopt, 10, std::nullopt, 10, std::nullopt, 10})});

  auto resultAB =
      evaluate("if (c2 is null, 10, CAST(null as BIGINT))  + (c0 / c1)", data);
  auto resultBA =
      evaluate("(c0 / c1) + if (c2 is null, 10, CAST(null as BIGINT))", data);

  assertEqualVectors(resultAB, resultBA);
  VELOX_ASSERT_THROW(evaluate("(c0 / c1) + 10", data), "division by zero");
  VELOX_ASSERT_THROW(
      evaluate("(c0 / c1) + (c0 + if(c1 = 0, 10, CAST(null as BIGINT)))", data),
      "division by zero");

  // Make non null flat input to invoke flat no null path for a subtree.
  data = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
       makeFlatVector<int32_t>({1, 0, 3, 0, 5, 6})});
  // Register a function that does not support flat no nulls fast path.
  exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(5));

  auto result = evaluate("plus5(c0 + c1)", data);
  assertEqualVectors(
      result, makeNullableFlatVector<int32_t>({7, 7, 11, 9, 15, 17}));

  // try permutations of nulls and errors. c1 is 0 every 3rd. c1 is null every
  // 5th. c2 is 0 every 7th.
  data = makeRowVector(
      {makeFlatVector<int32_t>(100, [](auto row) { return row % 3; }),
       makeFlatVector<int32_t>(
           100,
           [](auto row) { return 1 + (row % 5); },
           [](auto row) { return row % 5 == 0; }),
       makeFlatVector<int32_t>(100, [](auto row) { return row % 7; })});
  // All 6 permutations of 0, 1, 2.
  std::vector<std::vector<int32_t>> permutations = {
      {0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}};
  VectorPtr firstResult;
  VectorPtr firstCoalesceResult;
  for (auto& permutation : permutations) {
    result = evaluate(
        fmt::format(
            "try((100 / c{}) + (100 / c{}) + (100 / c{}))",
            permutation[0],
            permutation[1],
            permutation[2]),
        data);
    if (!firstResult) {
      firstResult = result;
    } else {
      assertEqualVectors(firstResult, result);
    }
    auto coalesceResult = evaluate(
        fmt::format(
            "try(coalesce((100 / c{}) + (100 / c{}) + (100 / c{}), 9999))",
            permutation[0],
            permutation[1],
            permutation[2]),
        data);
    if (!firstCoalesceResult) {
      firstCoalesceResult = coalesceResult;
    } else {
      assertEqualVectors(firstCoalesceResult, coalesceResult);
    }
  }
}

/// Test recursive constant peeling: in general expression evaluation first,
/// then in cast.
TEST_P(ParameterizedExprTest, constantWrap) {
  auto data = makeRowVector({
      makeNullableFlatVector<int64_t>({std::nullopt, 1, 25, 3}),
      makeConstant("5", 4),
  });

  auto result = evaluate("c0 < (cast(c1 as bigint) + 10)", {data});
  assertEqualVectors(
      makeNullableFlatVector<bool>({std::nullopt, true, false, true}), result);
}

TEST_P(ParameterizedExprTest, stdExceptionInVectorFunction) {
  exec::registerVectorFunction(
      "always_throws_vector_function",
      TestingAlwaysThrowsVectorFunction::signatures(),
      std::make_unique<TestingAlwaysThrowsVectorFunction>(false));

  assertError(
      "always_throws_vector_function(c0)",
      makeFlatVector<int32_t>({1, 2, 3}),
      "Top-level Expression: always_throws_vector_function(c0)",
      "",
      TestingAlwaysThrowsVectorFunction::kStdErrorMessage);

  assertErrorSimplified(
      "always_throws_vector_function(c0)",
      makeFlatVector<int32_t>({1, 2, 3}),
      TestingAlwaysThrowsVectorFunction::kStdErrorMessage);
}

TEST_P(ParameterizedExprTest, cseUnderTry) {
  auto input = makeRowVector({
      makeNullableFlatVector<int8_t>({31, 3, 31, 31, 2, std::nullopt}),
  });

  // All rows trigger overflow.
  VELOX_ASSERT_THROW(
      evaluate(
          "72::tinyint * 31::tinyint <> 4 or 72::tinyint * 31::tinyint <> 5",
          input),
      "integer overflow: 72 * 31");

  auto result = evaluate(
      "try(72::tinyint * 31::tinyint <> 4 or 72::tinyint * 31::tinyint <> 5)",
      input);
  assertEqualVectors(makeNullConstant(TypeKind::BOOLEAN, 6), result);

  // Only some rows trigger overflow.
  VELOX_ASSERT_THROW(
      evaluate(
          "36::tinyint * c0 <> 4 or 36::tinyint * c0 <> 5 or 36::tinyint * c0 <> 6",
          input),
      "integer overflow: 36 * 31");

  result = evaluate(
      "try(36::tinyint * c0 <> 4 or 36::tinyint * c0 <> 5 or 36::tinyint * c0 <> 6)",
      input);

  assertEqualVectors(
      makeNullableFlatVector<bool>({
          std::nullopt,
          true,
          std::nullopt,
          std::nullopt,
          true,
          std::nullopt,
      }),
      result);
}

namespace {
// This UDF throws an exception for any argument equal to 31, otherwise it's the
// identity function. Important for the test, it sizes the result only large
// enough to hold all rows that didn't throw an error.
class TestingShrinkForErrorsFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK(args[0]->isFlatEncoding());

    const auto* flatArg = args[0]->as<FlatVector<int8_t>>();

    exec::LocalSelectivityVector remainingRows(context, rows);
    context.applyToSelectedNoThrow(*remainingRows, [&](vector_size_t row) {
      // Throw a user error (so it can be caught be try) if the argument is
      // equal to 31.
      VELOX_USER_CHECK_NE(flatArg->valueAt(row), 31, "Expected");
    });

    context.deselectErrors(*remainingRows);

    // Create the result, importantly the size is rows.end() so may be smaller
    // than the rows being processed after errors are deselected.
    const auto localResult = std::make_shared<FlatVector<int8_t>>(
        context.pool(),
        outputType,
        nullptr,
        rows.end(),
        flatArg->values(),
        std::vector<BufferPtr>{});

    context.moveOrCopyResult(localResult, *remainingRows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("tinyint")
                .argumentType("tinyint")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_testing_shrink_for_errors,
    TestingShrinkForErrorsFunction::signatures(),
    std::make_unique<TestingShrinkForErrorsFunction>());

TEST_P(ParameterizedExprTest, cseUnderTryWithIf) {
  // This tests a particular case where shared subexpression computation can hit
  // errors when combined with TRY. In this case a VectorFunction sizes the
  // result based on rows.end(), can throw user exceptions, and doesn't resize
  // the result Vector to include rows that produced exceptions. If that
  // funciton is evaluated as part of CSE, e.g. on both sides of an if
  // statement, and the last row produces an exception, the result Vector will
  // be smaller than rows.size(). This test validates that in CSE we can
  // tolerate this and does not rely on the fact the result Vector is at least
  // rows.size() large, particularly when we're updating the result of a CSE to
  // add new values, e.g. in the else portion of an if statement.
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_testing_shrink_for_errors, "testing_shrink_for_errors");

  auto input = makeRowVector({
      makeNullableFlatVector<bool>({true, false, true, false, true, false}),
      makeNullableFlatVector<int8_t>({31, 3, 31, 31, 2, 31}),
  });

  VELOX_ASSERT_THROW(
      evaluate(
          "if(c0, testing_shrink_for_errors(c1), testing_shrink_for_errors(c1))",
          input),
      "Expected");

  auto result = evaluate(
      "try(if(c0, testing_shrink_for_errors(c1), testing_shrink_for_errors(c1)))",
      input);

  assertEqualVectors(
      makeNullableFlatVector<int8_t>({
          std::nullopt,
          3,
          std::nullopt,
          std::nullopt,
          2,
          std::nullopt,
      }),
      result);
}

// Test AND/OR expressions with multiple conjucts generating errors in different
// rows. With and without TRY.
TEST_P(ParameterizedExprTest, failingConjuncts) {
  auto input = makeRowVector({
      makeFlatVector<std::string>({"10", "foo", "11"}),
      makeFlatVector<std::string>({"bar", "10", "11"}),
      makeFlatVector<int64_t>({1, 2, 3}),
  });

  // cast(c0 as bigint) throws on 1st row.
  // cast(c1 as bigint) throws on 2nd row.
  // c2 > 0 doesn't throw and returns true for all rows.

  VELOX_ASSERT_THROW(
      evaluate(
          "cast(c0 as bigint) > 0 AND cast(c1 as bigint) > 0 AND c2 > 0",
          input),
      "Cannot cast VARCHAR 'bar' to BIGINT.");

  auto result = evaluate(
      "try(cast(c0 as bigint) > 0 AND cast(c1 as bigint) > 0 AND c2 > 0)",
      input);

  auto expected =
      makeNullableFlatVector<bool>({std::nullopt, std::nullopt, true});
  assertEqualVectors(expected, result);

  // c2 <> 1 returns false for 1st row and masks errors from other conjuncts.
  VELOX_ASSERT_THROW(
      evaluate(
          "cast(c0 as bigint) > 0 AND cast(c1 as bigint) > 0 AND c2 <> 1",
          input),
      "Cannot cast VARCHAR 'foo' to BIGINT.");

  result = evaluate(
      "try(cast(c0 as bigint) > 0 AND cast(c1 as bigint) > 0 AND c2 <> 1)",
      input);
  expected = makeNullableFlatVector<bool>({false, std::nullopt, true});
  assertEqualVectors(expected, result);

  // OR succeeds because c2 > 0 returns 'true' for all rows and masks errors
  // from other conjuncts.

  result = evaluate(
      "cast(c0 as bigint) < 0 OR cast(c1 as bigint) < 0 OR c2 > 0", input);
  expected = makeFlatVector<bool>({true, true, true});
  assertEqualVectors(expected, result);

  result = evaluate(
      "try(cast(c0 as bigint) < 0 OR cast(c1 as bigint) < 0 OR c2 > 0)", input);
  assertEqualVectors(expected, result);

  // c2 <> 1 returns 'false' for 1st row and fails to mask errors from other
  // conjuncts.
  VELOX_ASSERT_THROW(
      evaluate(
          "cast(c0 as bigint) < 0 OR cast(c1 as bigint) < 0 OR c2 <> 1", input),
      "Cannot cast VARCHAR 'bar' to BIGINT.");

  result = evaluate(
      "try(cast(c0 as bigint) < 0 OR cast(c1 as bigint) < 0 OR c2 <> 1)",
      input);
  expected = makeNullableFlatVector<bool>({std::nullopt, true, true});
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, conjunctUnderTry) {
  auto input = makeRowVector({
      makeFlatVector<StringView>({"a"_sv, "b"_sv}),
      makeFlatVector<bool>({true, true}),
      makeFlatVector<bool>({true, true}),
  });

  VELOX_ASSERT_THROW(
      evaluate(
          "array_constructor(like(c0, 'test', 'escape'), c1 OR c2)", input),
      "Escape string must be a single character");

  auto result = evaluate(
      "try(array_constructor(like(c0, 'test', 'escape'), c1 OR c2))", input);
  auto expected =
      BaseVector::createNullConstant(ARRAY(BOOLEAN()), input->size(), pool());
  assertEqualVectors(expected, result);

  // Test conjunct in switch in coalesce where there are errors came from
  // coalesce at rows 50--99 while conjunct is evlauated on rows 31--49.
  // Conjunct should not clear the errors from coalesce.
  input = makeRowVector({
      makeFlatVector<int64_t>(
          100, [](auto row) { return row; }, [](auto row) { return row < 50; }),
      makeFlatVector<bool>(100, [](auto row) { return row > 30; }),
      makeFlatVector<bool>(100, [](auto row) { return row > 20; }),
      makeConstant<int64_t>(0, 100),
  });

  result = evaluate("try(coalesce(c0 / c3 > 0, switch(c1, c1 or c2)))", input);
  expected = makeFlatVector<bool>(
      100,
      [](auto /*row*/) { return true; },
      [](auto row) { return row >= 50 || row <= 30; });
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, flatNoNullsFastPathWithCse) {
  // Test CSE with flat-no-nulls fast path.
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeFlatVector<int64_t>({8, 9, 10, 11, 12}),
  });

  // Make sure CSE "c0 + c1" is evaluated only once for each row.
  auto [result, stats] = evaluateWithStats(
      "if((c0 + c1) > 100::bigint, 100::bigint, c0 + c1)", input);

  auto expected = makeFlatVector<int64_t>({9, 11, 13, 15, 17});
  assertEqualVectors(expected, result);
  EXPECT_EQ(5, stats.at("plus").numProcessedRows);

  std::tie(result, stats) = evaluateWithStats(
      "if((c0 + c1) >= 15::bigint, 100::bigint, c0 + c1)", input);

  expected = makeFlatVector<int64_t>({9, 11, 13, 100, 100});
  assertEqualVectors(expected, result);
  EXPECT_EQ(5, stats.at("plus").numProcessedRows);
}

TEST_P(ParameterizedExprTest, cseOverLazyDictionary) {
  auto input = makeRowVector({
      makeConstant<int64_t>(10, 5),
      std::make_shared<LazyVector>(
          pool(),
          BIGINT(),
          5,
          std::make_unique<SimpleVectorLoader>([=](RowSet /*rows*/) {
            return wrapInDictionary(
                makeIndicesInReverse(5),
                makeFlatVector<int64_t>({8, 9, 10, 11, 12}));
          })),
      makeFlatVector<int64_t>({1, 2, 10, 11, 12}),
  });

  // if (c1 > 10, c0 + c1, c0 - c1) is a null-propagating conditional CSE.
  auto result = evaluate(
      "if (c2 > 10::bigint, "
      "   if (c1 > 10, c0 + c1, c0 - c1) + c2, "
      "   if (c1 > 10, c0 + c1, c0 - c1) - c2)",
      input);

  auto expected = makeFlatVector<int64_t>({21, 19, -10, 12, 14});
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, cseOverConstant) {
  auto input = makeRowVector({
      makeConstant<int64_t>(123, 5),
      makeConstant<int64_t>(-11, 5),
  });

  // Make sure CSE "c0 + c1" is evaluated only once for each row.
  auto [result, stats] =
      evaluateWithStats("if((c0 + c1) < 0::bigint, 0::bigint, c0 + c1)", input);

  auto expected = makeConstant<int64_t>(112, 5);
  assertEqualVectors(expected, result);
  EXPECT_EQ(5, stats.at("plus").numProcessedRows);
}

TEST_P(ParameterizedExprTest, cseOverDictionary) {
  auto indices = makeIndicesInReverse(5);
  auto input = makeRowVector({
      wrapInDictionary(indices, makeFlatVector<int64_t>({1, 2, 3, 4, 5})),
      wrapInDictionary(indices, makeFlatVector<int64_t>({8, 9, 10, 11, 12})),
  });

  // Make sure CSE "c0 + c1" is evaluated only once for each row.
  auto [result, stats] = evaluateWithStats(
      "if((c0 + c1) > 100::bigint, 100::bigint, c0 + c1)", input);

  auto expected = makeFlatVector<int64_t>({17, 15, 13, 11, 9});
  assertEqualVectors(expected, result);
  EXPECT_EQ(5, stats.at("plus").numProcessedRows);

  std::tie(result, stats) = evaluateWithStats(
      "if((c0 + c1) >= 15::bigint, 100::bigint, c0 + c1)", input);

  expected = makeFlatVector<int64_t>({100, 100, 13, 11, 9});
  assertEqualVectors(expected, result);
  EXPECT_EQ(5, stats.at("plus").numProcessedRows);
}

TEST_P(ParameterizedExprTest, cseOverDictionaryOverConstant) {
  auto indices = makeIndicesInReverse(5);
  auto input = makeRowVector({
      wrapInDictionary(indices, makeFlatVector<int64_t>({1, 2, 3, 4, 5})),
      wrapInDictionary(indices, makeConstant<int64_t>(100, 5)),
  });

  // Make sure CSE "c0 + c1" is evaluated only once for each row.
  auto [result, stats] =
      evaluateWithStats("if((c0 + c1) < 0::bigint, 0::bigint, c0 + c1)", input);

  auto expected = makeFlatVector<int64_t>({105, 104, 103, 102, 101});
  assertEqualVectors(expected, result);
  EXPECT_EQ(5, stats.at("plus").numProcessedRows);

  std::tie(result, stats) = evaluateWithStats(
      "if((c0 + c1) < 103::bigint, 0::bigint, c0 + c1)", input);

  expected = makeFlatVector<int64_t>({105, 104, 103, 0, 0});
  assertEqualVectors(expected, result);
  EXPECT_EQ(5, stats.at("plus").numProcessedRows);
}

TEST_P(ParameterizedExprTest, cseOverDictionaryAcrossMultipleExpressions) {
  // This test verifies that CSE across multiple expressions are evaluated
  // correctly, that is, make sure peeling is done before attempting to re-use
  // computed results from CSE.
  auto input = makeRowVector({
      wrapInDictionary(
          makeIndices({1, 3}),
          makeFlatVector<StringView>({"aa1"_sv, "bb2"_sv, "cc3"_sv, "dd4"_sv})),
  });
  // Case 1: Peeled and unpeeled set of rows have overlap. This will ensure the
  // right pre-computed values are used.
  // upper(c0) is the CSE here having c0 as a distinct field. Initially its
  // distinct fields is empty as concat (its parent) will have the same
  // fields. If during compilation distinct field is not set when it is
  // identified as a CSE then it will be empty and peeling
  // will not occur the second time CSE is employed. Here the peeled rows are
  // {0,1,2,3} and unpeeled are {0,1}. If peeling is performed in the first
  // encounter, rows to compute will be {_ , 1, _, 3} and in the second
  // instance if peeling is not performed then rows to computed would be {0,
  // 1} where row 0 will be computed and 1 will be re-used so row 1 would have
  // wrong result.
  {
    // Use an allocated result vector to force copying of values to the result
    // vector. Otherwise, we might end up with a result vector pointing directly
    // to the shared values vector from CSE.
    std::vector<VectorPtr> resultToReuse = {
        makeFlatVector<StringView>({"x"_sv, "y"_sv}),
        makeFlatVector<StringView>({"x"_sv, "y"_sv})};
    auto [result, stats] = evaluateMultipleWithStats(
        {"concat('foo_',upper(c0))", "upper(c0)"}, input, resultToReuse);
    std::vector<VectorPtr> expected = {
        makeFlatVector<StringView>({"foo_BB2"_sv, "foo_DD4"_sv}),
        makeFlatVector<StringView>({"BB2"_sv, "DD4"_sv})};
    assertEqualVectors(expected[0], result[0]);
    assertEqualVectors(expected[1], result[1]);
    EXPECT_EQ(2, stats.at("upper").numProcessedRows);
  }

  // Case 2: Here a CSE_1 "substr(upper(c0),2)" shared twice has a child
  // expression which itself is a CSE_2 "upper(c0)" shared thrice. If expression
  // compilation were not fixed, CSE_1 will have distinct fields set but
  // the CSE_2 has a parent in one of the other expression trees and therefore
  // will have its distinct fields set properly. This would result in CSE_1 not
  // peeling but CSE_2 will. In the first expression tree peeling happens
  // before CSE so both CSE_1 and CSE_2 are tracking peeled rows. In the second
  // expression CSE_2 is used again will peeled rows, however in third
  // expression CSE_1 is not peeled but its child CSE_2 attempts peeling and
  // runs into an error while creating the peel.
  {
    // Use an allocated result vector to force copying of values to the result.
    std::vector<VectorPtr> resultToReuse = {
        makeFlatVector<StringView>({"x"_sv, "y"_sv}),
        makeFlatVector<StringView>({"x"_sv, "y"_sv}),
        makeFlatVector<StringView>({"x"_sv, "y"_sv})};
    auto [result, stats] = evaluateMultipleWithStats(
        {"concat('foo_',substr(upper(c0),2))",
         "substr(upper(c0),3)",
         "substr(upper(c0),2)"},
        input,
        resultToReuse);
    std::vector<VectorPtr> expected = {
        makeFlatVector<StringView>({"foo_B2"_sv, "foo_D4"_sv}),
        makeFlatVector<StringView>({"2"_sv, "4"_sv}),
        makeFlatVector<StringView>({"B2"_sv, "D4"_sv})};
    assertEqualVectors(expected[0], result[0]);
    assertEqualVectors(expected[1], result[1]);
    assertEqualVectors(expected[2], result[2]);
    EXPECT_EQ(2, stats.at("upper").numProcessedRows);
  }
}

TEST_P(ParameterizedExprTest, smallerWrappedBaseVector) {
  // This test verifies that in the case that wrapping the
  // result of a peeledResult (i.e result which is computed after
  // peeling input) results in a smaller result than baseVector,
  // then we don't fault if the rows to be copied are more than the
  // size of the wrapped result.
  // Typically, this happens when the results have a lot of trailing nulls.

  auto baseMap = createMapOfArraysVector<int64_t, int64_t>(
      {{{1, std::nullopt}},
       {{2, {{4, 5, std::nullopt}}}},
       {{2, {{7, 8, 9}}}},
       {{2, std::nullopt}},
       {{2, std::nullopt}},
       {{2, std::nullopt}}});
  auto indices = makeIndices(10, [](auto row) { return row % 6; });
  auto nulls = makeNulls(10, [](auto row) { return row > 5; });
  auto wrappedMap = BaseVector::wrapInDictionary(nulls, indices, 10, baseMap);
  auto input = makeRowVector({wrappedMap});

  auto exprSet = compileMultiple(
      {"element_at(element_at(c0, 2::bigint), 1::bigint)"},
      asRowType(input->type()));

  exec::EvalCtx context(execCtx_.get(), exprSet.get(), input.get());

  // We set finalSelection to false so that
  // we force copy of results into the flatvector.
  *context.mutableIsFinalSelection() = false;
  auto finalRows = SelectivityVector(10);
  *context.mutableFinalSelection() = &finalRows;

  // We need a different SelectivityVector for rows
  // otherwise the copy will not kick in.
  SelectivityVector rows(input->size());
  std::vector<VectorPtr> result(1);
  auto flatResult = makeFlatVector<int64_t>(input->size(), BIGINT());
  result[0] = flatResult;
  exprSet->eval(rows, context, result);

  assertEqualVectors(
      makeNullableFlatVector<int64_t>(
          {std::nullopt,
           4,
           7,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt}),
      result[0]);
}

TEST_P(ParameterizedExprTest, nullPropagation) {
  auto singleString = parseExpression(
      "substr(c0, 1, if (length(c0) > 2, length(c0) - 1, 0))",
      ROW({"c0"}, {VARCHAR()}));
  auto twoStrings = parseExpression(
      "substr(c0, 1, if (length(c1) > 2, length(c0) - 1, 0))",
      ROW({"c0", "c1"}, {VARCHAR(), VARCHAR()}));
  EXPECT_TRUE(propagatesNulls(singleString));
  EXPECT_FALSE(propagatesNulls(twoStrings));

  // Try will call propagateNulls on its child, that should not
  // redo the computation.
  auto switchInTry = parseExpression(
      "try(switch(subscript(c0, array_min(c1)), FALSE, FALSE,"
      "FALSE, is_null('6338838335202944843'::BIGINT)))",
      ROW({"c0", "c1"}, {ARRAY({BOOLEAN()}), ARRAY({BIGINT()})}));
  EXPECT_FALSE(propagatesNulls(switchInTry));
}

TEST_P(ParameterizedExprTest, peelingWithSmallerConstantInput) {
  // This test ensures that when a dictionary-encoded vector is peeled together
  // with a constant vector whose size is smaller than the corresponding
  // selected rows of the dictionary base vector, the subsequent evaluation on
  // the constant vector doesn't access values beyond its size.
  auto data = makeRowVector({makeFlatVector<int64_t>({1, 2})});
  auto c0 = makeRowVector(
      {makeFlatVector<int64_t>({1, 3, 5, 7, 9})}, nullEvery(1, 2));
  auto indices = makeIndices({2, 3, 4});
  auto d0 = wrapInDictionary(indices, c0);
  auto c1 = BaseVector::wrapInConstant(3, 1, data);

  // After evaluating d0, Coalesce copies values from c1 to an existing result
  // vector. c1 should be large enough so that this copy step does not access
  // values out of bound.
  auto result = evaluate("coalesce(c0, c1)", makeRowVector({d0, c1}));
  assertEqualVectors(c1, result);
}

TEST_P(ParameterizedExprTest, ifWithLazyNulls) {
  // Makes a null-propagating switch. Evaluates it so that null propagation
  // masks out errors.
  constexpr int32_t kSize = 100;
  const char* kExpr =
      "CASE WHEN 10 % (c0 - 2) < 0 then c0 + c1 when 10 % (c0 - 4) = 3 then c0 + c1 * 2 else c0 + c1 end";
  auto c0 = makeFlatVector<int64_t>(kSize, [](auto row) { return row % 10; });
  auto c1 = makeFlatVector<int64_t>(
      kSize,
      [](auto row) { return row; },
      [](auto row) { return row % 10 == 2 || row % 10 == 4; });

  auto result = evaluate(kExpr, makeRowVector({c0, c1}));
  auto resultFromLazy =
      evaluate(kExpr, makeRowVector({c0, wrapInLazyDictionary(c1)}));
  assertEqualVectors(result, resultFromLazy);
}

int totalDefaultNullFunc = 0;
template <typename T>
struct DefaultNullFunc {
  void call(int64_t& output, int64_t input1, int64_t input2) {
    output = input1 + input2;
    totalDefaultNullFunc++;
  }
};

int totalNotDefaultNullFunc = 0;
template <typename T>
struct NotDefaultNullFunc {
  bool callNullable(int64_t& output, const int64_t* input) {
    output = totalNotDefaultNullFunc++;
    return input;
  }
};

TEST_F(ExprTest, commonSubExpressionWithPeeling) {
  registerFunction<DefaultNullFunc, int64_t, int64_t, int64_t>(
      {"default_null"});
  registerFunction<NotDefaultNullFunc, int64_t, int64_t>({"not_default_null"});

  // func1(func2(c0), c0) propagates nulls of c0. since func1 is default null.
  std::string expr1 = "default_null(not_default_null(c0), c0)";
  EXPECT_TRUE(propagatesNulls(parseExpression(expr1, ROW({"c0"}, {BIGINT()}))));

  // func2(c0) does not propagate nulls.
  std::string expr2 = "not_default_null(c0)";
  EXPECT_FALSE(
      propagatesNulls(parseExpression(expr2, ROW({"c0"}, {BIGINT()}))));

  auto clearResults = [&]() {
    totalDefaultNullFunc = 0;
    totalNotDefaultNullFunc = 0;
  };

  // When the input does not have additional nulls, peeling happens for both
  // expr1, and expr2 identically, hence each of them will be evaluated only
  // once per peeled row.
  {
    auto data = makeRowVector({wrapInDictionary(
        makeIndices({0, 0, 0, 1}), 4, makeFlatVector<int64_t>({1, 2, 3, 4}))});
    auto check = [&](const std::vector<std::string>& expressions) {
      auto result = makeRowVector(evaluateMultiple(expressions, data));
      ASSERT_EQ(totalDefaultNullFunc, 2);
      ASSERT_EQ(totalNotDefaultNullFunc, 2);
      clearResults();
    };
    check({expr1, expr2});
    check({expr1});
    check({expr1, expr1, expr2});
    check({expr2, expr1, expr2});
  }

  // When the dictionary input have additional nulls, peeling won't happen for
  // expressions that do not propagate nulls. Hence when expr2 is reached it
  // shall be evaluated again for all rows.
  {
    auto data = makeRowVector({BaseVector::wrapInDictionary(
        makeNulls(4, nullEvery(2)),
        makeIndices({0, 0, 0, 1}),
        4,
        makeFlatVector<int64_t>({1, 2, 3, 4}))});
    {
      auto results = makeRowVector(evaluateMultiple({expr1, expr2}, data));

      ASSERT_EQ(totalDefaultNullFunc, 2);
      // It is evaluated twice during expr1 and 4 times during expr2.
      ASSERT_EQ(totalNotDefaultNullFunc, 6);
      clearResults();
    }
    {
      // if expr2 appears again it shall be not be re-evaluated.
      auto results =
          makeRowVector(evaluateMultiple({expr1, expr2, expr1, expr2}, data));
      ASSERT_EQ(totalDefaultNullFunc, 2);
      // It is evaluated twice during expr1 and 4 times during expr2.
      ASSERT_EQ(totalNotDefaultNullFunc, 6);
      clearResults();
    }
    {
      // Set max_shared_subexpr_results_cached low so the
      // second result isn't cached, so expr1 is only evaluated once, but expr2
      // is evaluated twice.
      auto queryCtx = velox::core::QueryCtx::create(
          nullptr,
          core::QueryConfig(std::unordered_map<std::string, std::string>{
              {core::QueryConfig::kMaxSharedSubexprResultsCached, "1"}}));
      core::ExecCtx execCtx(pool_.get(), queryCtx.get());
      auto results = makeRowVector(evaluateMultiple(
          {expr1, expr2, expr1, expr2}, data, std::nullopt, &execCtx));
      ASSERT_EQ(totalDefaultNullFunc, 2);
      // It is evaluated twice during expr1 and 4 times during expr2, expr2 is
      // evaluated twice.
      ASSERT_EQ(totalNotDefaultNullFunc, 10);
    }
  }
}

TEST_P(ParameterizedExprTest, dictionaryOverLoadedLazy) {
  // This test verifies a corner case where peeling does not go past a loaded
  // lazy layer which caused wrong set of inputs being passed to shared
  // sub-expressions evaluation.
  // Inputs are of the form c0: Dict1(Lazy(Dict2(Flat1))) and c1: Dict1(Flat
  constexpr int32_t kSize = 100;

  // Generate inputs of the form c0: Dict1(Lazy(Dict2(Flat1))) and c1:
  // Dict1(Flat2). Note c0 and c1 have the same top encoding layer.

  // Generate indices that randomly point to different rows of the base flat
  // layer. This makes sure that wrong values are copied over if there is a bug
  // in shared sub-expressions evaluation.
  std::vector<int> indicesUnderLazy = {2, 5, 4, 1, 2, 4, 5, 6, 4, 9};
  auto smallFlat =
      makeFlatVector<int64_t>(kSize / 10, [](auto row) { return row * 2; });
  auto indices = makeIndices(kSize, [&indicesUnderLazy](vector_size_t row) {
    return indicesUnderLazy[row % 10];
  });
  auto lazyDict = std::make_shared<LazyVector>(
      execCtx_->pool(),
      smallFlat->type(),
      kSize,
      std::make_unique<SimpleVectorLoader>([=](RowSet /*rows*/) {
        return wrapInDictionary(indices, kSize, smallFlat);
      }));
  // Make sure it is loaded, otherwise during evaluation ensureLoaded() would
  // transform the input vector from Dict1(Lazy(Dict2(Flat1))) to
  // Dict1((Dict2(Flat1))) which recreates the buffers for the top layers and
  // disables any peeling that can happen between c0 and c1.
  lazyDict->loadedVector();

  auto sharedIndices = makeIndices(kSize / 2, [](auto row) { return row * 2; });
  auto c0 = wrapInDictionary(sharedIndices, kSize / 2, lazyDict);
  auto c1 = wrapInDictionary(
      sharedIndices,
      makeFlatVector<int64_t>(kSize, [](auto row) { return row; }));

  // "(c0 < 5 and c1 < 90)" would peel Dict1 layer in the top level conjunct
  // expression then when peeled c0 is passed to the inner "c0 < 5" expression,
  // a call to EvalCtx::getField() removes the lazy layer which ensures the last
  // dictionary layer is peeled. This means that shared sub-expression
  // evaluation is done on the lowest flat layer. In the second expression "c0 <
  // 5" the input is Dict1(Lazy(Dict2(Flat1))) and if peeling only removed till
  // the lazy layer, the shared sub-expression evaluation gets called on
  // Lazy(Dict2(Flat1)) which then results in wrong results.
  auto result = evaluateMultiple(
      {"(c0 < 5 and c1 < 90)", "c0 < 5"}, makeRowVector({c0, c1}));
  auto resultFromLazy = evaluate("c0 < 5", makeRowVector({c0, c1}));
  assertEqualVectors(result[1], resultFromLazy);
}

TEST_P(ParameterizedExprTest, dictionaryResizeWithIndicesReset) {
  // This test verifies a fuzzer failure that was due to resizeDictionary not
  // initializing indices of new rows to 0.
  auto indices = makeIndices({0, 0, 4, 4, 0});
  auto indices2 = makeIndices({0, 1, 3});
  auto nulls = makeNulls({false, false, false, true, false});
  auto c0 = BaseVector::wrapInDictionary(
      nulls, indices, 5, makeNullableFlatVector<int64_t>({1, 1, 1, 2, 2}));
  auto wrappedC0 = BaseVector::wrapInDictionary(nullptr, indices2, 3, c0);

  auto result = evaluate(
      "coalesce(plus(c0, 1::BIGINT), 1::BIGINT)", makeRowVector({wrappedC0}));
  auto expected = makeNullableFlatVector<int64_t>({2, 2, 1});
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, noSelectedRows) {
  VectorPtr result = makeFlatVector<int64_t>({7, 8, 9});
  auto expected = makeFlatVector<int64_t>({7, 8, 9});

  // Test evalFlatNoNulls code path.
  {
    auto input = makeRowVector(
        {makeFlatVector<int64_t>({1, 2, 3}),
         makeFlatVector<int64_t>({4, 5, 6})});
    evalWithEmptyRows("c0 + c1", input, result, expected);
  }

  // Test regular evaluation path.
  {
    auto input = makeRowVector(
        {makeNullableFlatVector<int64_t>({1, std::nullopt, 3}),
         makeNullableFlatVector<int64_t>({std::nullopt, 5, 6})});
    evalWithEmptyRows("c0 + c1", input, result, expected);
  }

  // Test simplified evaluation path.
  {
    auto input = makeRowVector(
        {makeNullableFlatVector<int64_t>({1, std::nullopt, 3}),
         makeNullableFlatVector<int64_t>({std::nullopt, 5, 6})});
    evalWithEmptyRows<exec::ExprSetSimplified>(
        "c0 + c1", input, result, expected);
  }
}

TEST_P(ParameterizedExprTest, multiplyReferencedConstantField) {
  auto data = makeRowVector(
      {makeFlatVector<bool>({true, false, true, false}),
       makeConstantArray<int64_t>(4, {1, 2, 3})});

  auto result = evaluate("if(c0, c1, c1)", data);
  auto expected = makeConstantArray<int64_t>(4, {1, 2, 3});
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, dereference) {
  // Make a vector of Row<Row<int64_t>> where the middle-layer has dictionary
  // over constant encoding. Evaluate nested dereference c0.d0.f0 on it so that
  // the outer dereference expression (i.e., dereference of f0) receive
  // dictionary-encoded input without peeling.
  auto child = makeFlatVector<int64_t>({1, 2});
  auto d0 = makeRowVector({"f0"}, {child});
  auto constantD0 = BaseVector::wrapInConstant(6, 0, d0);
  auto indices = makeIndices({0, 1, 2, 3, 4, 5});
  auto nulls = makeNulls(6, [](auto row) { return row == 5; });
  auto dictionaryD0 =
      BaseVector::wrapInDictionary(nulls, indices, 6, constantD0);
  auto c0 = makeRowVector({"d0"}, {dictionaryD0});
  auto input = makeRowVector({"c0"}, {c0});

  // Skip row 4 during evaluation. FieldReference should not have errors dealing
  // with this situation.
  SelectivityVector rows(input->size(), true);
  rows.setValid(4, false);
  rows.updateBounds();

  auto results = evaluateMultiple({"(c0).d0.f0"}, input, rows);
  BaseVector::flattenVector(results[0]);
  auto flatResult = results[0]->asFlatVector<int64_t>();
  EXPECT_EQ(flatResult->valueAt(0), 1);
  EXPECT_EQ(flatResult->valueAt(1), 1);
  EXPECT_EQ(flatResult->valueAt(2), 1);
  EXPECT_EQ(flatResult->valueAt(3), 1);
  EXPECT_TRUE(flatResult->isNullAt(5));

  // Test dereferencing a field vector that is shorter than the struct. Evaluate
  // nested dereference so that the outer dereference expression receives
  // constant-encoded input.
  auto rowType = ROW({"f0"}, {BIGINT()});
  constantD0 = BaseVector::createNullConstant(rowType, 6, pool());
  c0 = makeRowVector({"d0"}, {constantD0});
  auto result = evaluate("(c0).d0.f0", makeRowVector({c0}));
  auto expected = makeNullConstant(TypeKind::BIGINT, 6);
  assertEqualVectors(expected, result);
}

TEST_P(ParameterizedExprTest, inputFreeFieldReferenceMetaData) {
  auto exprSet = compileExpression("c0", {ROW({"c0"}, {INTEGER()})});
  auto expr = exprSet->expr(0);
  EXPECT_EQ(expr->distinctFields().size(), 1);
  EXPECT_EQ((void*)expr->distinctFields()[0], expr.get());

  EXPECT_TRUE(expr->propagatesNulls());
  EXPECT_TRUE(expr->isDeterministic());
}

TEST_P(ParameterizedExprTest, extractSubfields) {
  auto rowType = ROW({
      {"c0",
       ARRAY(ROW({
           {"c0c0", MAP(BIGINT(), BIGINT())},
           {"c0c1", MAP(VARCHAR(), BIGINT())},
       }))},
      {"c1", ARRAY(BIGINT())},
      {"c2", ARRAY(ARRAY(BIGINT()))},
      {"c3", BIGINT()},
  });
  auto validate = [&](const std::string& expr,
                      const std::vector<std::string>& expected) {
    SCOPED_TRACE(expr);
    auto exprSet = compileExpression(expr, rowType);
    std::vector<std::string> actual;
    for (auto& subfield : exprSet->expr(0)->extractSubfields()) {
      actual.push_back(subfield.toString());
    }
    std::sort(actual.begin(), actual.end());
    auto newEnd = std::unique(actual.begin(), actual.end());
    actual.erase(newEnd, actual.end());
    ASSERT_EQ(actual, expected);
  };
  validate("c0[1].c0c0[0] > c1[1]", {"c0[1].c0c0[0]", "c1[1]"});
  validate("c0[1].c0c1['foo'] > 0", {"c0[1].c0c1[\"foo\"]"});
  validate("c0[1].c0c0[c1[1]] > 0", {"c0[1].c0c0", "c1[1]"});
  validate("element_at(c1, -1)", {"c1"});
  validate("transform(c0, x -> x.c0c0[0] + c1[1])", {"c0", "c1[1]"});
  validate("transform(c0, c1 -> c1.c0c0[0])", {"c0"});
  validate("reduce(c1, 0, (c0, c3) -> c0 + c3, c2 -> c2)", {"c1"});
  validate("reduce(c1, 0, (c0, c3) -> c0 + c3, c2 -> c2) + c3", {"c1", "c3"});
  validate(
      "transform(c2, c0 -> reduce(c0, 0, (c0, c2) -> c0 + c2, c0 -> c0 + c1[1]) + c0[1])",
      {"c1[1]", "c2"});
}
auto makeRow = [](const std::string& fieldName) {
  return fmt::format(
      "cast(row_constructor(1) as struct({} BOOLEAN))", fieldName);
};

TEST_P(ParameterizedExprTest, extractSubfieldsWithDereference) {
  // Tests extracting subfields from expressions using DeferenceTypedExpr to
  // access fields without the field names initialized (all empty string).  In
  // this case, the field's parent should be extracted.
  std::vector<core::TypedExprPtr> expr = {
      std::make_shared<core::DereferenceTypedExpr>(
          REAL(),
          std::make_shared<core::FieldAccessTypedExpr>(
              ROW({{"", DOUBLE()}, {"", REAL()}, {"", BIGINT()}}),
              std::make_shared<core::InputTypedExpr>(ROW(
                  {{"c0",
                    ROW({{"", DOUBLE()}, {"", REAL()}, {"", BIGINT()}})}})),
              "c0"),
          1)};

  auto exprSet =
      std::make_unique<exec::ExprSet>(std::move(expr), execCtx_.get());
  auto subfields = exprSet->expr(0)->extractSubfields();

  ASSERT_EQ(subfields.size(), 1);
  ASSERT_EQ(subfields[0].toString(), "c0");
}

TEST_P(ParameterizedExprTest, lazyHandlingByDereference) {
  // Ensure FieldReference handles an input which has an encoding over a lazy
  // vector. Trying to access the inner flat vector of an input in the form
  // Row(Dict(Lazy(Row(Flat)))) will ensure an intermediate FieldReference
  // expression in the tree receives an input of the form Dict(Lazy(Row(Flat))).
  auto base = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4, 5})});
  VectorPtr col1 = std::make_shared<LazyVector>(
      execCtx_->pool(),
      base->type(),
      5,
      std::make_unique<test::SimpleVectorLoader>(
          [base](auto /*size*/) { return base; }));
  auto indices = makeIndicesInReverse(5);
  col1 = wrapInDictionary(indices, 5, col1);
  col1 = makeRowVector({col1});
  auto result = evaluate("(c0).c0.c0", makeRowVector({col1}));
}

TEST_P(ParameterizedExprTest, switchRowInputTypesAreTheSame) {
  assertErrorSimplified(
      fmt::format("switch(c0, {},  {})", makeRow("f1"), makeRow("f2")),
      makeFlatVector<bool>(1),
      "Else clause of a SWITCH statement must have the same type as 'then' clauses. Expected ROW<f1:BOOLEAN>, but got ROW<f2:BOOLEAN>.");

  assertErrorSimplified(
      fmt::format("if(c0, {},  {})", makeRow("f1"), makeRow("f2")),
      {makeFlatVector<bool>(1)},
      "Else clause of a SWITCH statement must have the same type as 'then' clauses. Expected ROW<f1:BOOLEAN>, but got ROW<f2:BOOLEAN>.");

  {
    auto condition = compileExpression("c0", {ROW({"c0"}, {BOOLEAN()})});
    auto thenBranch = compileExpression(makeRow("f1"), {});
    auto elseBranch = compileExpression(makeRow("f1"), {});
    try {
      exec::SwitchExpr switchExpr(
          ROW({"c0"}, {BOOLEAN()}),
          {condition->expr(0), thenBranch->expr(0), elseBranch->expr(0)},
          false);
      EXPECT_TRUE(false) << "Expected an error";
    } catch (VeloxException& e) {
      EXPECT_EQ(
          "Switch expression type different than then clause. Expected ROW<f1:BOOLEAN> but got Actual ROW<c0:BOOLEAN>.",
          e.message());
    }
  }
}

TEST_P(ParameterizedExprTest, coalesceRowInputTypesAreTheSame) {
  auto makeRow = [](const std::string& fieldName) {
    return fmt::format(
        "cast(row_constructor(1) as struct({} BOOLEAN))", fieldName);
  };

  assertErrorSimplified(
      fmt::format("coalesce({},  {})", makeRow("f1"), makeRow("f2")),
      makeFlatVector<bool>(1),
      "Inputs to coalesce must have the same type. Expected ROW<f1:BOOLEAN>, but got ROW<f2:BOOLEAN>.");

  {
    auto expr1 = compileExpression(makeRow("f1"), {});
    auto expr2 = compileExpression(makeRow("f1"), {});
    try {
      exec::CoalesceExpr coalesceExpr(
          ROW({"c0"}, {BOOLEAN()}), {expr1->expr(0), expr2->expr(0)}, false);
      EXPECT_TRUE(false) << "Expected an error";
    } catch (VeloxException& e) {
      EXPECT_EQ(
          "Coalesce expression type different than its inputs. Expected ROW<f1:BOOLEAN> but got Actual ROW<c0:BOOLEAN>.",
          e.message());
    }
  }

  {
    // Check that operator==() of CallTypedExpr, FieldAccessTypedExpr,
    // ConcatTypedExpr, and LambdaTypedExpr returns false when result types are
    // different.
    auto call1 = std::make_shared<const core::CallTypedExpr>(
        ROW({"row_field0"}, {BIGINT()}),
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::FieldAccessTypedExpr>(BIGINT(), "c0")},
        "foo");
    auto call2 = std::make_shared<const core::CallTypedExpr>(
        ROW({""}, {BIGINT()}),
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::FieldAccessTypedExpr>(BIGINT(), "c0")},
        "foo");
    ASSERT_FALSE(*call1 == *call2);

    auto fieldAccess1 = std::make_shared<const core::FieldAccessTypedExpr>(
        ROW({"row_field0"}, {BIGINT()}), "c0");
    auto fieldAccess2 = std::make_shared<const core::FieldAccessTypedExpr>(
        ROW({""}, {BIGINT()}), "c0");
    ASSERT_FALSE(*fieldAccess1 == *fieldAccess2);

    auto concat1 = std::make_shared<const core::ConcatTypedExpr>(
        std::vector<std::string>{"row_field0"},
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::FieldAccessTypedExpr>(
                BIGINT(), "c0")});
    auto concat2 = std::make_shared<const core::ConcatTypedExpr>(
        std::vector<std::string>{""},
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::FieldAccessTypedExpr>(
                BIGINT(), "c0")});
    ASSERT_FALSE(*concat1 == *concat2);

    auto lambda1 = std::make_shared<const core::LambdaTypedExpr>(
        ROW({"c0"}, {BIGINT()}), call1);
    auto lambda2 = std::make_shared<const core::LambdaTypedExpr>(
        ROW({"c0"}, {BIGINT()}), call2);
    ASSERT_FALSE(*lambda1 == *lambda2);
  }

  {
    // cast(concat(c0) as row(row_field0)).row_field0 + coalesce(c1,
    // concat(c0)).row_field0. The result type of the first concat is
    // Row<"":bigint> while the result type of the second concat is
    // Row<"row_field0":bigint>. This is a valid expression, so the expression
    // compilation should not throw in this situation.
    core::TypedExprPtr concat = std::make_shared<const core::ConcatTypedExpr>(
        std::vector<std::string>{"row_field0"},
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::FieldAccessTypedExpr>(
                BIGINT(), "c0")});
    core::TypedExprPtr coalesce = std::make_shared<const core::CallTypedExpr>(
        ROW({"row_field0"}, {BIGINT()}),
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::FieldAccessTypedExpr>(
                ROW({"row_field0"}, {BIGINT()}), "c1"),
            concat},
        "coalesce");
    core::TypedExprPtr dereference =
        std::make_shared<const core::FieldAccessTypedExpr>(
            BIGINT(), coalesce, "row_field0");
    core::TypedExprPtr concat2 = std::make_shared<const core::ConcatTypedExpr>(
        std::vector<std::string>{""},
        std::vector<core::TypedExprPtr>{
            std::make_shared<const core::FieldAccessTypedExpr>(
                BIGINT(), "c0")});
    ASSERT_FALSE(*concat == *concat2);
    core::TypedExprPtr cast = std::make_shared<const core::CallTypedExpr>(
        ROW({"row_field0"}, {BIGINT()}),
        std::vector<core::TypedExprPtr>{concat2},
        "cast");
    core::TypedExprPtr dereference2 =
        std::make_shared<const core::FieldAccessTypedExpr>(
            BIGINT(), cast, "row_field0");
    core::TypedExprPtr plus = std::make_shared<const core::CallTypedExpr>(
        BIGINT(),
        std::vector<core::TypedExprPtr>{dereference2, dereference},
        "plus");

    ASSERT_NO_THROW(compileExpression(plus));
  }
}

TEST_P(ParameterizedExprTest, evaluatesArgumentsOnNonIncreasingSelection) {
  auto makeLazy = [&](const auto& base, const auto& check) {
    return std::make_shared<LazyVector>(
        execCtx_->pool(),
        base->type(),
        base->size(),
        std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
          check(rows);
          return base;
        }));
  };
  constexpr int kSize = 300;
  auto c0 = makeFlatVector<int64_t>(kSize, folly::identity);

  {
    SCOPED_TRACE("No eager loading for AND clauses");
    auto input = makeRowVector({
        c0,
        makeLazy(
            c0,
            [](auto& rows) {
              // Only the rows passing c0 % 2 == 0 should be loaded.
              VELOX_CHECK_EQ(rows.size(), (kSize + 1) / 2);
              for (auto i : rows) {
                VELOX_CHECK(i % 2 == 0);
              }
            }),
    });
    auto actual =
        evaluate("c0 % 2 == 0 and c1 % 3 == 0 and c1 % 5 == 0", input);
    auto expected =
        makeFlatVector<bool>(kSize, [](auto i) { return i % 30 == 0; });
    assertEqualVectors(expected, actual);
  }

  {
    SCOPED_TRACE("IF inside AND");
    auto input = makeRowVector({
        c0,
        makeLazy(
            c0,
            [](auto& rows) {
              // Only the rows passing c0 % 2 == 0 should be loaded.
              VELOX_CHECK_EQ(rows.size(), (kSize + 1) / 2);
              for (auto i : rows) {
                VELOX_CHECK(i % 2 == 0);
              }
            }),
    });
    auto actual =
        evaluate("c0 % 2 == 0 and if (c0 % 3 == 0, c1, -1 * c1) >= 0", input);
    auto expected =
        makeFlatVector<bool>(kSize, [](auto i) { return i % 6 == 0; });
    assertEqualVectors(expected, actual);
  }

  {
    SCOPED_TRACE("AND inside IF");
    auto input = makeRowVector({
        c0,
        makeLazy(
            c0,
            [&](auto& rows) {
              // All rows should be loaded.
              VELOX_CHECK_EQ(rows.size(), kSize);
            }),
    });
    auto actual = evaluate(
        "if (c0 % 2 == 0, c1 % 3 == 0 and c1 % 5 == 0, c1 % 7 == 0 and c1 % 11 == 0)",
        input);
    auto expected = makeFlatVector<bool>(kSize, [](auto i) {
      return (i % 2 == 0 && i % 15 == 0) || (i % 2 != 0 && i % 77 == 0);
    });
    assertEqualVectors(expected, actual);
  }

  {
    SCOPED_TRACE("Shared subexp");
    auto c1 = makeFlatVector<int64_t>({0, 0});
    auto c2 = makeFlatVector<int64_t>({1, 1});
    auto input = makeRowVector({
        makeNullableFlatVector<int64_t>({0, std::nullopt}),
        makeLazy(
            c1,
            [&](auto& rows) {
              // All rows should be loaded.
              VELOX_CHECK_EQ(rows.size(), 2);
            }),
        c2,
    });
    auto actual =
        evaluate("(c0 <> if (c1 < c2, 1, 0)) is null and c1 < c2", input);
    assertEqualVectors(makeFlatVector<bool>({false, true}), actual);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    ExprTest,
    ParameterizedExprTest,
    testing::ValuesIn({false, true}));

} // namespace
} // namespace facebook::velox::test
