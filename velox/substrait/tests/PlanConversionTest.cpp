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

#include <google/protobuf/util/json_util.h>
#include <fstream>
#include <sstream>

#include "velox/common/base/tests/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/test/utils/DataFiles.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/type/Type.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec;
using namespace facebook::velox::common::test;
using namespace facebook::velox::exec::test;

class PlanConversionTest : public virtual HiveConnectorTestBase,
                           public testing::WithParamInterface<bool> {
 protected:
  void SetUp() override {
    useAsyncCache_ = GetParam();
    HiveConnectorTestBase::SetUp();
  }

  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
  }

  std::vector<RowVectorPtr> makeVectors(
      int32_t count,
      int32_t rowsPerVector,
      const std::shared_ptr<const RowType>& rowType) {
    return HiveConnectorTestBase::makeVectors(rowType, count, rowsPerVector);
  }

  class VeloxConverter {
   public:
    // This class is an iterator for Velox computing.
    class WholeComputeResultIterator {
     public:
      WholeComputeResultIterator(
          const std::shared_ptr<const core::PlanNode>& planNode,
          u_int32_t index,
          const std::vector<std::string>& paths,
          const std::vector<u_int64_t>& starts,
          const std::vector<u_int64_t>& lengths)
          : planNode_(planNode),
            index_(index),
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
          auto split = std::make_shared<
              facebook::velox::connector::hive::HiveConnectorSplit>(
              facebook::velox::exec::test::kHiveConnectorId,
              path,
              facebook::velox::dwio::common::FileFormat::ORC,
              start,
              length);
          connectorSplits.emplace_back(split);
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
      u_int32_t index_;
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
        const std::string& subPlanPath) {
      // Read sub.json and resume the Substrait plan.
      std::ifstream subJson(subPlanPath);
      std::stringstream buffer;
      buffer << subJson.rdbuf();
      std::string subData = buffer.str();
      ::substrait::Plan subPlan;
      google::protobuf::util::JsonStringToMessage(subData, &subPlan);

      auto planConverter = std::make_shared<
          facebook::velox::substrait::SubstraitVeloxPlanConverter>();
      // Convert to Velox PlanNode.
      auto planNode = planConverter->toVeloxPlan(subPlan);

      // Get the information for TableScan.
      u_int32_t partitionIndex = planConverter->getPartitionIndex();
      std::vector<std::string> paths = planConverter->getPaths();

      // In test, need to get the absolute path of the generated ORC file.
      auto tempPath = getTmpDirPath();
      std::vector<std::string> absolutePaths;
      absolutePaths.reserve(paths.size());

      for (const auto& path : paths) {
        absolutePaths.emplace_back(fmt::format("file://{}{}", tempPath, path));
      }

      std::vector<u_int64_t> starts = planConverter->getStarts();
      std::vector<u_int64_t> lengths = planConverter->getLengths();
      // Construct the result iterator.
      auto resIter = std::make_shared<WholeComputeResultIterator>(
          planNode, partitionIndex, absolutePaths, starts, lengths);
      return resIter;
    }

    std::string getTmpDirPath() const {
      return tmpDir_->path;
    }

    std::shared_ptr<exec::test::TempDirectoryPath> tmpDir_{
        exec::test::TempDirectoryPath::create()};
  };

  // This method can be used to create a Fixed-width type of Vector without Null
  // values.
  template <typename T>
  VectorPtr createSpecificScalar(
      size_t size,
      std::vector<T> vals,
      facebook::velox::memory::MemoryPool& pool) {
    facebook::velox::BufferPtr values = AlignedBuffer::allocate<T>(size, &pool);
    auto valuesPtr = values->asMutableRange<T>();
    facebook::velox::BufferPtr nulls = nullptr;
    for (size_t i = 0; i < size; ++i) {
      valuesPtr[i] = vals[i];
    }
    return std::make_shared<facebook::velox::FlatVector<T>>(
        &pool, nulls, size, values, std::vector<BufferPtr>{});
  }

  // This method can be used to create a String type of Vector without Null
  // values.
  VectorPtr createSpecificStringVector(
      size_t size,
      std::vector<std::string> vals,
      facebook::velox::memory::MemoryPool& pool) {
    auto vector = BaseVector::create(VARCHAR(), size, &pool);
    auto flatVector = vector->asFlatVector<StringView>();

    size_t childSize = 0;
    std::vector<int64_t> lengths(size);
    size_t nullCount = 0;
    for (size_t i = 0; i < size; ++i) {
      auto notNull = true;
      vector->setNull(i, !notNull);
      auto len = vals[i].size();
      lengths[i] = len;
      childSize += len;
    }
    vector->setNullCount(0);

    BufferPtr buf = AlignedBuffer::allocate<char>(childSize, &pool);
    char* bufPtr = buf->asMutable<char>();
    char* dest = bufPtr;
    for (size_t i = 0; i < size; ++i) {
      std::string str = vals[i];
      const char* chr = str.c_str();
      auto length = str.size();
      memcpy(dest, chr, length);
      dest = dest + length;
    }
    size_t offset = 0;
    for (size_t i = 0; i < size; ++i) {
      if (!vector->isNullAt(i)) {
        flatVector->set(
            i, facebook::velox::StringView(bufPtr + offset, lengths[i]));
        offset += lengths[i];
      }
    }
    return vector;
  }
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

TEST_P(PlanConversionTest, queryTest) {
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
  std::unique_ptr<facebook::velox::memory::MemoryPool> pool{
      facebook::velox::memory::getDefaultScopedMemoryPool()};
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
  vectors.emplace_back(createSpecificScalar<int64_t>(10, lOrderkeyData, *pool));
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
  vectors.emplace_back(createSpecificScalar<int64_t>(10, lPartkeyData, *pool));
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
  vectors.emplace_back(createSpecificScalar<int64_t>(10, lSuppkeyData, *pool));
  std::vector<int32_t> lLinenumberData = {4, 6, 1, 5, 1, 2, 1, 5, 2, 6};
  vectors.emplace_back(
      createSpecificScalar<int32_t>(10, lLinenumberData, *pool));
  std::vector<double> lQuantityData = {
      6.0, 1.0, 19.0, 4.0, 6.0, 12.0, 23.0, 11.0, 16.0, 19.0};
  vectors.emplace_back(createSpecificScalar<double>(10, lQuantityData, *pool));
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
  vectors.emplace_back(
      createSpecificScalar<double>(10, lExtendedpriceData, *pool));
  std::vector<double> lDiscountData = {
      0.05, 0.06, 0.01, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06, 0.07};
  vectors.emplace_back(createSpecificScalar<double>(10, lDiscountData, *pool));
  std::vector<double> lTaxData = {
      0.02, 0.03, 0.01, 0.0, 0.01, 0.01, 0.03, 0.07, 0.01, 0.04};
  vectors.emplace_back(createSpecificScalar<double>(10, lTaxData, *pool));
  std::vector<std::string> lReturnflagData = {
      "N", "A", "A", "R", "A", "N", "A", "A", "N", "R"};
  vectors.emplace_back(createSpecificStringVector(10, lReturnflagData, *pool));
  std::vector<std::string> lLinestatusData = {
      "O", "F", "F", "F", "F", "O", "F", "F", "O", "F"};
  vectors.emplace_back(createSpecificStringVector(10, lLinestatusData, *pool));
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
  vectors.emplace_back(
      createSpecificScalar<double>(10, lShipdateNewData, *pool));
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
  vectors.emplace_back(
      createSpecificScalar<double>(10, lCommitdateNewData, *pool));
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
  vectors.emplace_back(
      createSpecificScalar<double>(10, lReceiptdateNewData, *pool));
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
  vectors.emplace_back(
      createSpecificStringVector(10, lShipinstructData, *pool));
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
  vectors.emplace_back(createSpecificStringVector(10, lShipmodeData, *pool));
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
  vectors.emplace_back(createSpecificStringVector(10, lCommentData, *pool));

  // Batches has only one RowVector here.
  uint64_t nullCount = 0;
  std::vector<RowVectorPtr> batches{std::make_shared<RowVector>(
      pool.get(), type, nullptr, 10, vectors, nullCount)};

  // Find the Velox path according current path.
  std::string veloxPath;
  std::string currentPath = fs::current_path().c_str();
  size_t pos = 0;

  if ((pos = currentPath.find("project")) != std::string::npos) {
    // In Github test, the Velox home is /root/project.
    veloxPath = currentPath.substr(0, pos) + "project";
  } else if ((pos = currentPath.find("velox")) != std::string::npos) {
    veloxPath = currentPath.substr(0, pos) + "velox";
  } else if ((pos = currentPath.find("fbcode")) != std::string::npos) {
    veloxPath = currentPath;
  } else {
    throw std::runtime_error("Current path is not a valid Velox path.");
  }

  // Find and deserialize Substrait plan json file.
  std::string subPlanPath = veloxPath + "/velox/substrait/tests/sub.json";
  auto veloxConverter = std::make_shared<VeloxConverter>();

  // Writes data into an ORC file.
  auto sink = std::make_unique<facebook::velox::dwio::common::FileSink>(
      veloxConverter->getTmpDirPath() + "/mock_lineitem.orc");
  auto config = std::make_shared<facebook::velox::dwrf::Config>();
  const int64_t writerMemoryCap = std::numeric_limits<int64_t>::max();
  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = type;
  options.memoryBudget = writerMemoryCap;
  options.flushPolicy = nullptr;
  options.layoutPlannerFactory = nullptr;
  auto writer = std::make_unique<facebook::velox::dwrf::Writer>(
      options,
      std::move(sink),
      facebook::velox::memory::getProcessDefaultMemoryManager().getRoot());
  for (size_t i = 0; i < batches.size(); ++i) {
    writer->write(batches[i]);
  }
  writer->close();

  auto resIter = veloxConverter->getResIter(subPlanPath);
  while (resIter->HasNext()) {
    auto rv = resIter->Next();
    auto size = rv->size();
    ASSERT_EQ(size, 1);
    std::string res = rv->toString(0);
    ASSERT_EQ(res, "{ [child at 0]: 13613.1921}");
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    PlanConversionTests,
    PlanConversionTest,
    testing::Values(true, false));
