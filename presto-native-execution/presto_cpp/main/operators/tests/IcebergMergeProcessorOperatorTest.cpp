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
#include "presto_cpp/main/operators/IcebergMergeProcessorOperator.h"

#include <gtest/gtest.h>

#include "velox/connectors/hive/iceberg/IcebergMergeProcessor.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using IMP = facebook::velox::connector::hive::iceberg::IcebergMergeProcessor;

namespace facebook::presto::operators::test {
namespace {

// Channel layout matches the contract documented on
// `IcebergMergeProcessorNode`:
//   0 unique_id        BIGINT
//   1 target_row_id    ROW(file_path VARCHAR, pos BIGINT)
//   2 merge_row        ROW(id BIGINT, name VARCHAR, operation TINYINT,
//                          case_number INTEGER)
//   3 case_number      INTEGER
//   4 is_distinct      BOOLEAN
constexpr column_index_t kTargetRowIdChannel = 1;
constexpr column_index_t kMergeRowChannel = 2;

const RowTypePtr kRowIdType = ROW({"file_path", "pos"}, {VARCHAR(), BIGINT()});
const RowTypePtr kMergeRowType =
    ROW({"id", "name", "operation", "case_number"},
        {BIGINT(), VARCHAR(), TINYINT(), INTEGER()});
const RowTypePtr kInputType = ROW(
    {"unique_id", "target_row_id", "merge_row", "case_number", "is_distinct"},
    {BIGINT(), kRowIdType, kMergeRowType, INTEGER(), BOOLEAN()});

// Wraps the `IcebergMergeProcessorOperator` driven by a `Values` source.
// Covers operator-wrapper behaviors that the standalone
// `IcebergMergeProcessorTest` cannot exercise: the
// `Operator::getOutput() must return nullptr or a non-empty vector`
// contract, plus the `isFinished()` / `noMoreInput()` interaction after an
// empty input page.
class IcebergMergeProcessorOperatorTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    exec::Operator::registerOperator(
        std::make_unique<IcebergMergeProcessorTranslator>());
  }

  // Builds the 5-channel input RowVector per the operator's input contract.
  // Channels 0/3/4 carry valid-but-unused data so an accidental dereference
  // would be obvious in a debugger.
  RowVectorPtr makeInput(
      const std::vector<std::optional<std::string>>& filePaths,
      const std::vector<std::optional<int64_t>>& positions,
      const std::vector<std::optional<int64_t>>& mergeIds,
      const std::vector<std::optional<std::string>>& mergeNames,
      const std::vector<int8_t>& operations,
      const std::vector<int32_t>& caseNumbers) {
    velox::test::VectorMaker vectorMaker{pool_.get()};
    const auto numRows = operations.size();

    auto uniqueId = vectorMaker.flatVector<int64_t>(
        numRows, [](vector_size_t i) { return 1'000 + i; });

    auto filePathField =
        vectorMaker.flatVectorNullable<StringView>(toStringViews(filePaths));
    auto posField = vectorMaker.flatVectorNullable<int64_t>(positions);
    auto rowId = std::make_shared<RowVector>(
        pool_.get(),
        kRowIdType,
        /*nulls=*/nullptr,
        numRows,
        std::vector<VectorPtr>{filePathField, posField});

    auto idField = vectorMaker.flatVectorNullable<int64_t>(mergeIds);
    auto nameField =
        vectorMaker.flatVectorNullable<StringView>(toStringViews(mergeNames));
    auto operationField = vectorMaker.flatVector<int8_t>(operations);
    auto caseNumberField = vectorMaker.flatVector<int32_t>(caseNumbers);
    auto mergeRow = std::make_shared<RowVector>(
        pool_.get(),
        kMergeRowType,
        /*nulls=*/nullptr,
        numRows,
        std::vector<VectorPtr>{
            idField, nameField, operationField, caseNumberField});

    auto caseNumberCh = vectorMaker.flatVector<int32_t>(caseNumbers);
    auto isDistinct = vectorMaker.flatVector<bool>(
        numRows, [](vector_size_t /*i*/) { return true; });

    return std::make_shared<RowVector>(
        pool_.get(),
        kInputType,
        /*nulls=*/nullptr,
        numRows,
        std::vector<VectorPtr>{
            uniqueId, rowId, mergeRow, caseNumberCh, isDistinct});
  }

  // Builds a plan: Values(input) -> IcebergMergeProcessorNode. The processor
  // is configured with two target columns (id BIGINT, name VARCHAR) — the
  // same shape used by `IcebergMergeProcessorTest`.
  core::PlanNodePtr makePlan(const std::vector<RowVectorPtr>& inputs) {
    const std::vector<TypePtr> targetColumnTypes{BIGINT(), VARCHAR()};
    const std::vector<std::string> outputColumnNames{
        "id",
        "name",
        "operation",
        "$target_table_row_id",
        "insert_from_update"};
    return PlanBuilder()
        .values(inputs)
        .addNode(
            [&](core::PlanNodeId nodeId,
                core::PlanNodePtr source) -> core::PlanNodePtr {
              return std::make_shared<IcebergMergeProcessorNode>(
                  nodeId,
                  targetColumnTypes,
                  outputColumnNames,
                  kRowIdType,
                  kTargetRowIdChannel,
                  kMergeRowChannel,
                  std::move(source));
            })
        .planNode();
  }

  static std::vector<std::optional<StringView>> toStringViews(
      const std::vector<std::optional<std::string>>& values) {
    std::vector<std::optional<StringView>> result;
    result.reserve(values.size());
    for (const auto& v : values) {
      if (v.has_value()) {
        result.emplace_back(StringView(*v));
      } else {
        result.emplace_back(std::nullopt);
      }
    }
    return result;
  }
};

// Zero-row input page must not surface as a zero-row output page. Without
// the operator-side guard, `getOutput()` forwards the processor's empty
// RowVector and Velox fails with
// `Operator::getOutput() must return nullptr or a non-empty vector`.
TEST_F(IcebergMergeProcessorOperatorTest, emptyInputPageEmitsNullptr) {
  auto plan = makePlan({makeInput({}, {}, {}, {}, {}, {})});
  auto results = AssertQueryBuilder(plan).copyResults(pool_.get());
  EXPECT_EQ(results->size(), 0);
}

// All-DEFAULT_CASE input (every row has operation == -1) produces no output
// rows but does not crash — same contract as the empty-input case.
TEST_F(IcebergMergeProcessorOperatorTest, allDefaultCaseInputEmitsNullptr) {
  auto plan = makePlan({makeInput(
      /*filePaths=*/{std::nullopt, std::nullopt, std::nullopt},
      /*positions=*/{std::nullopt, std::nullopt, std::nullopt},
      /*mergeIds=*/{1, 2, 3},
      /*mergeNames=*/
      {{std::string("a")}, {std::string("b")}, {std::string("c")}},
      /*operations=*/
      {IMP::kDefaultCaseOperationNumber,
       IMP::kDefaultCaseOperationNumber,
       IMP::kDefaultCaseOperationNumber},
      /*caseNumbers=*/{0, 0, 0})});
  auto results = AssertQueryBuilder(plan).copyResults(pool_.get());
  EXPECT_EQ(results->size(), 0);
}

// Sanity guard that the empty-output filter does not also drop pages with
// real output rows: 1 INSERT + N DEFAULT_CASE rows must produce exactly 1
// output row.
TEST_F(
    IcebergMergeProcessorOperatorTest,
    mixedDefaultCaseAndRealOpsEmitsOnlyRealOps) {
  auto plan = makePlan({makeInput(
      /*filePaths=*/{std::nullopt, std::nullopt, std::nullopt},
      /*positions=*/{std::nullopt, std::nullopt, std::nullopt},
      /*mergeIds=*/{77, 88, 99},
      /*mergeNames=*/
      {{std::string("x")}, {std::string("y")}, {std::string("z")}},
      /*operations=*/
      {IMP::kDefaultCaseOperationNumber,
       IMP::kInsertOperationNumber,
       IMP::kDefaultCaseOperationNumber},
      /*caseNumbers=*/{0, 0, 0})});
  auto results = AssertQueryBuilder(plan).copyResults(pool_.get());
  ASSERT_EQ(results->size(), 1);
  auto* operations = results->childAt(2)->asFlatVector<int8_t>();
  ASSERT_NE(operations, nullptr);
  EXPECT_EQ(operations->valueAt(0), IMP::kInsertOperationNumber);
  auto* ids = results->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(ids, nullptr);
  EXPECT_EQ(ids->valueAt(0), 88);
}

// Drives the operator through an empty input page followed by noMoreInput()
// and verifies the task reaches EOS cleanly (i.e. the cursor returns the
// empty result vector and AssertQueryBuilder does not throw on the
// `isFinished()` path).
TEST_F(IcebergMergeProcessorOperatorTest, finishAfterEmptyInputCompletes) {
  auto plan = makePlan({makeInput({}, {}, {}, {}, {}, {})});
  // AssertQueryBuilder::copyResults drives the cursor to EOS — if
  // `isFinished()` were not satisfied after an empty input the task would
  // hang or throw rather than returning. Reaching this line is the test
  // assertion.
  auto results = AssertQueryBuilder(plan).copyResults(pool_.get());
  EXPECT_EQ(results->size(), 0);
}

} // namespace
} // namespace facebook::presto::operators::test
