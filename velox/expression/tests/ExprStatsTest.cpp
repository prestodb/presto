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

#include <gmock/gmock.h>
#include "velox/core/Expressions.h"
#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class ExprStatsTest : public testing::Test, public VectorTestBase {
 protected:
  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();

    pool_->setMemoryUsageTracker(memory::MemoryUsageTracker::create());

    // Enable CPU usage tracking.
    queryCtx_->setConfigOverridesUnsafe({
        {core::QueryConfig::kExprTrackCpuUsage, "true"},
    });
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::parseExpr(text);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  std::unique_ptr<exec::ExprSet> compileExpressions(
      const std::vector<std::string>& exprs,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions;
    expressions.reserve(exprs.size());
    for (const auto& expr : exprs) {
      expressions.emplace_back(parseExpression(expr, rowType));
    }
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, &context, &result);
    return result[0];
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

TEST_F(ExprStatsTest, printWithStats) {
  vector_size_t size = 1'024;

  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
  });

  auto rowType = asRowType(data->type());
  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);

    // Check stats before evaluation.
    ASSERT_EQ(
        exec::printExprWithStats(*exprSet),
        "multiply [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#1]\n"
        "   plus [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#2]\n"
        "      cast(c0 as BIGINT) [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#3]\n"
        "         c0 [cpu time: 0ns, rows: 0, batches: 0] -> INTEGER [#4]\n"
        "      3:BIGINT [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#5]\n"
        "   cast(c1 as BIGINT) [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#6]\n"
        "      c1 [cpu time: 0ns, rows: 0, batches: 0] -> INTEGER [#7]\n"
        "\n"
        "eq [cpu time: 0ns, rows: 0, batches: 0] -> BOOLEAN [#8]\n"
        "   mod [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#9]\n"
        "      cast(plus as BIGINT) [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#10]\n"
        "         plus [cpu time: 0ns, rows: 0, batches: 0] -> INTEGER [#11]\n"
        "            c0 -> INTEGER [CSE #4]\n"
        "            c1 -> INTEGER [CSE #7]\n"
        "      2:BIGINT [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#12]\n"
        "   0:BIGINT [cpu time: 0ns, rows: 0, batches: 0] -> BIGINT [#13]\n");

    evaluate(*exprSet, data);

    // Check stats after evaluation.
    ASSERT_THAT(
        exec::printExprWithStats(*exprSet),
        ::testing::MatchesRegex(
            "multiply .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#1.\n"
            "   plus .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#2.\n"
            "      cast.c0 as BIGINT. .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#3.\n"
            "         c0 .cpu time: 0ns, rows: 0, batches: 0. -> INTEGER .#4.\n"
            "      3:BIGINT .cpu time: 0ns, rows: 0, batches: 0. -> BIGINT .#5.\n"
            "   cast.c1 as BIGINT. .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#6.\n"
            "      c1 .cpu time: 0ns, rows: 0, batches: 0. -> INTEGER .#7.\n"
            "\n"
            "eq .cpu time: .+, rows: 1024, batches: 1. -> BOOLEAN .#8.\n"
            "   mod .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#9.\n"
            "      cast.plus as BIGINT. .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#10.\n"
            "         plus .cpu time: .+, rows: 1024, batches: 1. -> INTEGER .#11.\n"
            "            c0 -> INTEGER .CSE #4.\n"
            "            c1 -> INTEGER .CSE #7.\n"
            "      2:BIGINT .cpu time: 0ns, rows: 0, batches: 0. -> BIGINT .#12.\n"
            "   0:BIGINT .cpu time: 0ns, rows: 0, batches: 0. -> BIGINT .#13.\n"));
  }

  // Verify that common sub-expressions are identified properly.
  {
    auto exprSet =
        compileExpressions({"(c0 + c1) % 5", "(c0 + c1) % 3"}, rowType);
    evaluate(*exprSet, data);
    ASSERT_THAT(
        exec::printExprWithStats(*exprSet),
        ::testing::MatchesRegex(
            "mod .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#1.\n"
            "   cast.plus as BIGINT. .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#2.\n"
            "      plus .cpu time: .+, rows: 1024, batches: 1. -> INTEGER .#3.\n"
            "         c0 .cpu time: 0ns, rows: 0, batches: 0. -> INTEGER .#4.\n"
            "         c1 .cpu time: 0ns, rows: 0, batches: 0. -> INTEGER .#5.\n"
            "   5:BIGINT .cpu time: 0ns, rows: 0, batches: 0. -> BIGINT .#6.\n"
            "\n"
            "mod .cpu time: .+, rows: 1024, batches: 1. -> BIGINT .#7.\n"
            "   cast..plus.c0, c1.. as BIGINT. -> BIGINT .CSE #2.\n"
            "   3:BIGINT .cpu time: 0ns, rows: 0, batches: 0. -> BIGINT .#8.\n"));
  }

  // Use dictionary encoding to repeat each row 5 times.
  auto indices = makeIndices(size, [](auto row) { return row / 5; });
  data = makeRowVector({
      wrapInDictionary(indices, size, data->childAt(0)),
      wrapInDictionary(indices, size, data->childAt(1)),
  });

  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);
    evaluate(*exprSet, data);

    ASSERT_THAT(
        exec::printExprWithStats(*exprSet),
        ::testing::MatchesRegex(
            "multiply .cpu time: .+, rows: 205, batches: 1. -> BIGINT .#1.\n"
            "   plus .cpu time: .+, rows: 205, batches: 1. -> BIGINT .#2.\n"
            "      cast.c0 as BIGINT. .cpu time: .+, rows: 205, batches: 1. -> BIGINT .#3.\n"
            "         c0 .cpu time: 0ns, rows: 0, batches: 0. -> INTEGER .#4.\n"
            "      3:BIGINT .cpu time: 0ns, rows: 0, batches: 0. -> BIGINT .#5.\n"
            "   cast.c1 as BIGINT. .cpu time: .+, rows: 205, batches: 1. -> BIGINT .#6.\n"
            "      c1 .cpu time: 0ns, rows: 0, batches: 0. -> INTEGER .#7.\n"
            "\n"
            "eq .cpu time: .+, rows: 205, batches: 1. -> BOOLEAN .#8.\n"
            "   mod .cpu time: .+, rows: 205, batches: 1. -> BIGINT .#9.\n"
            "      cast.plus as BIGINT. .cpu time: .+, rows: 205, batches: 1. -> BIGINT .#10.\n"
            "         plus .cpu time: .+, rows: 205, batches: 1. -> INTEGER .#11.\n"
            "            c0 -> INTEGER .CSE #4.\n"
            "            c1 -> INTEGER .CSE #7.\n"
            "      2:BIGINT .cpu time: 0ns, rows: 0, batches: 0. -> BIGINT .#12.\n"
            "   0:BIGINT .cpu time: 0ns, rows: 0, batches: 0. -> BIGINT .#13.\n"));
  }
}

struct Event {
  std::string uuid;
  std::unordered_map<std::string, exec::ExprStats> stats;
};

class TestListener : public exec::ExprSetListener {
 public:
  explicit TestListener(
      std::vector<Event>& events,
      std::vector<std::string>& exceptions)
      : events_{events}, exceptions_{exceptions}, exceptionCount_{0} {}

  void onCompletion(
      const std::string& uuid,
      const exec::ExprSetCompletionEvent& event) override {
    events_.push_back({uuid, event.stats});
  }

  void onError(
      const SelectivityVector& rows,
      const ::facebook::velox::exec::EvalCtx::ErrorVector& errors) override {
    rows.applyToSelected([&](auto row) {
      exceptionCount_++;

      try {
        auto exception =
            *std::static_pointer_cast<std::exception_ptr>(errors.valueAt(row));
        std::rethrow_exception(exception);
      } catch (const std::exception& e) {
        exceptions_.push_back(e.what());
      }
    });
  }

  int exceptionCount() const {
    return exceptionCount_;
  }

  void reset() {
    exceptionCount_ = 0;
    events_.clear();
    exceptions_.clear();
  }

 private:
  std::vector<Event>& events_;
  std::vector<std::string>& exceptions_;
  int exceptionCount_;
};

TEST_F(ExprStatsTest, listener) {
  vector_size_t size = 1'024;

  // Register a listener to receive stats on ExprSet destruction.
  std::vector<Event> events;
  std::vector<std::string> exceptions;
  auto listener = std::make_shared<TestListener>(events, exceptions);
  ASSERT_TRUE(exec::registerExprSetListener(listener));
  ASSERT_FALSE(exec::registerExprSetListener(listener));

  auto data = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
  });

  // Evaluate a couple of expressions and sanity check the stats received by the
  // listener.
  auto rowType = asRowType(data->type());
  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);
    evaluate(*exprSet, data);
  }
  ASSERT_EQ(1, events.size());
  auto stats = events.back().stats;

  ASSERT_EQ(2, stats.at("plus").numProcessedVectors);
  ASSERT_EQ(1024 * 2, stats.at("plus").numProcessedRows);

  ASSERT_EQ(1, stats.at("multiply").numProcessedVectors);
  ASSERT_EQ(1024, stats.at("multiply").numProcessedRows);

  ASSERT_EQ(1, stats.at("mod").numProcessedVectors);
  ASSERT_EQ(1024, stats.at("mod").numProcessedRows);

  for (const auto& name : {"plus", "multiply", "mod"}) {
    ASSERT_GT(stats.at(name).timing.cpuNanos, 0);
  }

  // Evaluate the same expressions twice and verify that stats received by the
  // listener are "doubled".
  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);
    evaluate(*exprSet, data);
    evaluate(*exprSet, data);
  }
  ASSERT_EQ(2, events.size());
  stats = events.back().stats;

  ASSERT_EQ(4, stats.at("plus").numProcessedVectors);
  ASSERT_EQ(1024 * 2 * 2, stats.at("plus").numProcessedRows);

  ASSERT_EQ(2, stats.at("multiply").numProcessedVectors);
  ASSERT_EQ(1024 * 2, stats.at("multiply").numProcessedRows);

  ASSERT_EQ(2, stats.at("mod").numProcessedVectors);
  ASSERT_EQ(1024 * 2, stats.at("mod").numProcessedRows);
  for (const auto& name : {"plus", "multiply", "mod"}) {
    ASSERT_GT(stats.at(name).timing.cpuNanos, 0);
  }

  ASSERT_NE(events[0].uuid, events[1].uuid);

  // Evaluate an expression with CTE and verify no double accounting.
  {
    auto exprSet =
        compileExpressions({"(c0 + c1) % 5", "pow(c0 + c1, 2)"}, rowType);
    evaluate(*exprSet, data);
  }
  ASSERT_EQ(3, events.size());
  stats = events.back().stats;
  ASSERT_EQ(1024, stats.at("plus").numProcessedRows);
  ASSERT_EQ(1024, stats.at("mod").numProcessedRows);
  ASSERT_EQ(1024, stats.at("pow").numProcessedRows);
  for (const auto& name : {"plus", "mod", "pow"}) {
    ASSERT_EQ(1, stats.at(name).numProcessedVectors);
  }

  // Unregister the listener, evaluate expressions again and verify the listener
  // wasn't invoked.
  ASSERT_TRUE(exec::unregisterExprSetListener(listener));
  ASSERT_FALSE(exec::unregisterExprSetListener(listener));

  {
    auto exprSet =
        compileExpressions({"(c0 + 3) * c1", "(c0 + c1) % 2 = 0"}, rowType);
    evaluate(*exprSet, data);
  }
  ASSERT_EQ(3, events.size());
}

TEST_F(ExprStatsTest, memoryAllocations) {
  std::mt19937 rng;

  vector_size_t size = 256;
  auto data = makeRowVector({
      makeFlatVector<float>(
          size, [&](auto /*row*/) { return folly::Random::randDouble01(rng); }),
  });

  auto rowType = asRowType(data->type());
  auto exprSet =
      compileExpressions({"(c0 - 0.5::REAL) * 2.0::REAL + 0.3::REAL"}, rowType);

  auto prevAllocations = pool_->getMemoryUsageTracker()->getNumAllocs();

  evaluate(*exprSet, data);
  auto currAllocations = pool_->getMemoryUsageTracker()->getNumAllocs();

  // Expect a single allocation for the result. Intermediate results should
  // reuse memory.
  ASSERT_EQ(1, currAllocations - prevAllocations);
}

TEST_F(ExprStatsTest, errorLog) {
  // Register a listener to log exceptions.
  std::vector<Event> events;
  std::vector<std::string> exceptions;
  auto listener = std::make_shared<TestListener>(events, exceptions);
  ASSERT_TRUE(exec::registerExprSetListener(listener));

  auto data = makeRowVector({makeNullableFlatVector<StringView>(
      {"12"_sv, "1a"_sv, "34"_sv, ""_sv, std::nullopt, " 1"_sv})});

  auto rowType = asRowType(data->type());
  auto exprSet = compileExpressions({"try(cast(c0 as integer))"}, rowType);

  evaluate(*exprSet, data);

  ASSERT_EQ(2, listener->exceptionCount());
  ASSERT_EQ(2, exceptions.size());
  for (const auto& exception : exceptions) {
    ASSERT_TRUE(
        exception.find("Context: cast((c0) as INTEGER)") != std::string::npos);
    ASSERT_TRUE(
        exception.find("Error Code: INVALID_ARGUMENT") != std::string::npos);
    ASSERT_TRUE(exception.find("Stack trace:") != std::string::npos);
  }

  // Test with no error.
  listener->reset();

  data = makeRowVector(
      {makeNullableFlatVector<StringView>({"12"_sv, "34"_sv, "56"_sv})});
  evaluate(*exprSet, data);
  ASSERT_EQ(0, listener->exceptionCount());
  ASSERT_EQ(0, exceptions.size());

  ASSERT_TRUE(exec::unregisterExprSetListener(listener));
}
