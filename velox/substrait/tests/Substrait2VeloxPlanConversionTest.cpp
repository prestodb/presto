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

#include "velox/substrait/tests/JsonToProtoConverter.h"

#include "velox/common/base/tests/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;

class Substrait2VeloxPlanConversionTest
    : public exec::test::HiveConnectorTestBase {
 protected:
  class VeloxConverter {
   public:
    // This class is an iterator for Velox computing.
    class WholeComputeResultIterator {
     public:
      WholeComputeResultIterator(
          const std::shared_ptr<const core::PlanNode>& planNode,
          const std::vector<std::string>& paths,
          const std::vector<u_int64_t>& starts,
          const std::vector<u_int64_t>& lengths)
          : planNode_(planNode),
            paths_(paths),
            starts_(starts),
            lengths_(lengths) {
        // Construct the splits.
        std::vector<std::shared_ptr<facebook::velox::connector::ConnectorSplit>>
            connectorSplits;
        connectorSplits.reserve(paths.size());
        for (int idx = 0; idx < paths.size(); idx++) {
          auto path = paths[idx];
          auto start = starts[idx];
          auto length = lengths[idx];
          connectorSplits.emplace_back(
              makeHiveConnectorSplit(path, start, length));
        }
        splits_.reserve(connectorSplits.size());
        for (const auto& connectorSplit : connectorSplits) {
          splits_.emplace_back(exec::Split(folly::copy(connectorSplit), -1));
        }

        params_.planNode = planNode;
        cursor_ = std::make_unique<exec::test::TaskCursor>(params_);
        addSplits_ = [&](Task* task) {
          if (noMoreSplits_) {
            return;
          }
          for (auto& split : splits_) {
            task->addSplit("0", std::move(split));
          }
          task->noMoreSplits("0");
          noMoreSplits_ = true;
        };
      }

      bool HasNext() {
        if (!mayHasNext_) {
          return false;
        }
        if (numRows_ > 0) {
          return true;
        } else {
          addSplits_(cursor_->task().get());
          if (cursor_->moveNext()) {
            result_ = cursor_->current();
            numRows_ += result_->size();
            return true;
          } else {
            mayHasNext_ = false;
            return false;
          }
        }
      }

      RowVectorPtr Next() {
        numRows_ = 0;
        return result_;
      }

     private:
      const std::shared_ptr<const core::PlanNode> planNode_;
      std::unique_ptr<exec::test::TaskCursor> cursor_;
      exec::test::CursorParameters params_;
      std::vector<exec::Split> splits_;
      bool noMoreSplits_ = false;
      std::function<void(exec::Task*)> addSplits_;
      std::vector<std::string> paths_;
      std::vector<u_int64_t> starts_;
      std::vector<u_int64_t> lengths_;
      uint64_t numRows_ = 0;
      bool mayHasNext_ = true;
      RowVectorPtr result_;
    };

    // This method will resume the Substrait plan from Json file,
    // and convert it into Velox PlanNode. A result iterator for
    // Velox computing will be returned.
    std::shared_ptr<WholeComputeResultIterator> getResIter(
        const std::string& planPath) {
      std::unique_ptr<memory::ScopedMemoryPool> pool_{
          memory::getDefaultScopedMemoryPool()};

      // Read sub.json and resume the Substrait plan.
      ::substrait::Plan substraitPlan;
      JsonToProtoConverter::readFromFile(planPath, substraitPlan);

      auto planConverter = std::make_shared<
          facebook::velox::substrait::SubstraitVeloxPlanConverter>();
      // Convert to Velox PlanNode.
      auto planNode = planConverter->toVeloxPlan(substraitPlan, pool_.get());

      // Get the information for TableScan.
      auto paths = planConverter->getPaths();

      // In test, need to get the absolute path of the generated ORC file.
      auto tempPath = getTmpDirPath();
      std::vector<std::string> absolutePaths;
      absolutePaths.reserve(paths.size());

      for (const auto& path : paths) {
        absolutePaths.emplace_back(fmt::format("{}{}", tempPath, path));
      }

      std::vector<u_int64_t> starts = planConverter->getStarts();
      std::vector<u_int64_t> lengths = planConverter->getLengths();
      // Construct the result iterator.
      return std::make_shared<WholeComputeResultIterator>(
          planNode, absolutePaths, starts, lengths);
    }

    std::string getTmpDirPath() const {
      return tmpDir_->path;
    }

    std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{
        exec::test::TempDirectoryPath::create()};
  };
};

// This test will firstly generate mock TPC-H lineitem ORC file. Then, Velox's
// computing will be tested based on the generated ORC file.
// Input: Json file of the Substrait plan for the below modified TPC-H Q6 query:
//
//  select sum(l_extendedprice*l_discount) as revenue from lineitem where
//  l_shipdate_new >= 8766 and l_shipdate_new < 9131 and l_discount between .06
//  - 0.01 and .06 + 0.01 and l_quantity < 24
//
//  Tested Velox computings include: TableScan (Filter Pushdown) + Project +
//  Aggregate
//  Output: the Velox computed Aggregation result

TEST_F(Substrait2VeloxPlanConversionTest, queryTest) {
  // Generate the used ORC file.
  auto type =
      ROW({"l_orderkey",
           "l_partkey",
           "l_suppkey",
           "l_linenumber",
           "l_quantity",
           "l_extendedprice",
           "l_discount",
           "l_tax",
           "l_returnflag",
           "l_linestatus",
           "l_shipdate_new",
           "l_commitdate_new",
           "l_receiptdate_new",
           "l_shipinstruct",
           "l_shipmode",
           "l_comment"},
          {BIGINT(),
           BIGINT(),
           BIGINT(),
           INTEGER(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           VARCHAR(),
           VARCHAR(),
           DOUBLE(),
           DOUBLE(),
           DOUBLE(),
           VARCHAR(),
           VARCHAR(),
           VARCHAR()});
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  std::vector<VectorPtr> vectors;
  // TPC-H lineitem table has 16 columns.
  int colNum = 16;
  vectors.reserve(colNum);
  std::vector<int64_t> lOrderkeyData = {
      4636438147,
      2012485446,
      1635327427,
      8374290148,
      2972204230,
      8001568994,
      989963396,
      2142695974,
      6354246853,
      4141748419};
  vectors.emplace_back(makeFlatVector<int64_t>(lOrderkeyData));
  std::vector<int64_t> lPartkeyData = {
      263222018,
      255918298,
      143549509,
      96877642,
      201976875,
      196938305,
      100260625,
      273511608,
      112999357,
      299103530};
  vectors.emplace_back(makeFlatVector<int64_t>(lPartkeyData));
  std::vector<int64_t> lSuppkeyData = {
      2102019,
      13998315,
      12989528,
      4717643,
      9976902,
      12618306,
      11940632,
      871626,
      1639379,
      3423588};
  vectors.emplace_back(makeFlatVector<int64_t>(lSuppkeyData));
  std::vector<int32_t> lLinenumberData = {4, 6, 1, 5, 1, 2, 1, 5, 2, 6};
  vectors.emplace_back(makeFlatVector<int32_t>(lLinenumberData));
  std::vector<double> lQuantityData = {
      6.0, 1.0, 19.0, 4.0, 6.0, 12.0, 23.0, 11.0, 16.0, 19.0};
  vectors.emplace_back(makeFlatVector<double>(lQuantityData));
  std::vector<double> lExtendedpriceData = {
      30586.05,
      7821.0,
      1551.33,
      30681.2,
      1941.78,
      66673.0,
      6322.44,
      41754.18,
      8704.26,
      63780.36};
  vectors.emplace_back(makeFlatVector<double>(lExtendedpriceData));
  std::vector<double> lDiscountData = {
      0.05, 0.06, 0.01, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06, 0.07};
  vectors.emplace_back(makeFlatVector<double>(lDiscountData));
  std::vector<double> lTaxData = {
      0.02, 0.03, 0.01, 0.0, 0.01, 0.01, 0.03, 0.07, 0.01, 0.04};
  vectors.emplace_back(makeFlatVector<double>(lTaxData));
  std::vector<std::string> lReturnflagData = {
      "N", "A", "A", "R", "A", "N", "A", "A", "N", "R"};
  vectors.emplace_back(makeFlatVector<std::string>(lReturnflagData));
  std::vector<std::string> lLinestatusData = {
      "O", "F", "F", "F", "F", "O", "F", "F", "O", "F"};
  vectors.emplace_back(makeFlatVector<std::string>(lLinestatusData));
  std::vector<double> lShipdateNewData = {
      8953.666666666666,
      8773.666666666666,
      9034.666666666666,
      8558.666666666666,
      9072.666666666666,
      8864.666666666666,
      9004.666666666666,
      8778.666666666666,
      9013.666666666666,
      8832.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lShipdateNewData));
  std::vector<double> lCommitdateNewData = {
      10447.666666666666,
      8953.666666666666,
      8325.666666666666,
      8527.666666666666,
      8438.666666666666,
      10049.666666666666,
      9036.666666666666,
      8666.666666666666,
      9519.666666666666,
      9138.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lCommitdateNewData));
  std::vector<double> lReceiptdateNewData = {
      10456.666666666666,
      8979.666666666666,
      8299.666666666666,
      8474.666666666666,
      8525.666666666666,
      9996.666666666666,
      9103.666666666666,
      8726.666666666666,
      9593.666666666666,
      9178.666666666666};
  vectors.emplace_back(makeFlatVector<double>(lReceiptdateNewData));
  std::vector<std::string> lShipinstructData = {
      "COLLECT COD",
      "NONE",
      "TAKE BACK RETURN",
      "NONE",
      "TAKE BACK RETURN",
      "NONE",
      "DELIVER IN PERSON",
      "DELIVER IN PERSON",
      "TAKE BACK RETURN",
      "NONE"};
  vectors.emplace_back(makeFlatVector<std::string>(lShipinstructData));
  std::vector<std::string> lShipmodeData = {
      "FOB",
      "REG AIR",
      "MAIL",
      "FOB",
      "RAIL",
      "SHIP",
      "REG AIR",
      "REG AIR",
      "TRUCK",
      "AIR"};
  vectors.emplace_back(makeFlatVector<std::string>(lShipmodeData));
  std::vector<std::string> lCommentData = {
      " the furiously final foxes. quickly final p",
      "thely ironic",
      "ate furiously. even, pending pinto bean",
      "ackages af",
      "odolites. slyl",
      "ng the regular requests sleep above",
      "lets above the slyly ironic theodolites sl",
      "lyly regular excuses affi",
      "lly unusual theodolites grow slyly above",
      " the quickly ironic pains lose car"};
  vectors.emplace_back(makeFlatVector<std::string>(lCommentData));

  // Batches has only one RowVector here.
  auto batches = {makeRowVector(type->names(), vectors)};

  // Find and deserialize Substrait plan json file.
  std::string planPath =
      getDataFilePath("velox/substrait/tests", "data/sub.json");
  auto veloxConverter = std::make_shared<VeloxConverter>();

  // Writes data into an ORC file.
  writeToFile(veloxConverter->getTmpDirPath() + "/mock_lineitem.orc", batches);

  auto resIter = veloxConverter->getResIter(planPath);
  while (resIter->HasNext()) {
    auto rv = resIter->Next();
    auto size = rv->size();
    ASSERT_EQ(size, 1);
    std::string res = rv->toString(0);
    ASSERT_EQ(res, "{13613.1921}");
  }
}
