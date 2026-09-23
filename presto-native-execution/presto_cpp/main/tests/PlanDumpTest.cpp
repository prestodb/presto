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
#include "presto_cpp/main/PlanDump.h"
#include <gtest/gtest.h>
#include <fstream>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "velox/common/base/Fs.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox;

namespace facebook::presto {
namespace {

class PlanDumpTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    tempDir_ = exec::test::TempDirectoryPath::create();
    // A subdirectory that does not exist yet, to check it gets created.
    dumpDir_ = tempDir_->getPath() + "/dump";
  }

  static protocol::TaskSource makeSource(
      const std::string& planNodeId,
      const std::vector<int64_t>& sequenceIds) {
    protocol::TaskSource source;
    source.planNodeId = planNodeId;
    for (auto sequenceId : sequenceIds) {
      protocol::ScheduledSplit split;
      split.sequenceId = sequenceId;
      split.planNodeId = planNodeId;
      source.splits.push_back(std::move(split));
    }
    return source;
  }

  std::string splitsPath(const std::string& taskId) const {
    return dumpDir_ + "/" + taskId + ".splits.json";
  }

  static nlohmann::json readJson(const std::string& path) {
    std::ifstream inFile(path);
    return nlohmann::json::parse(inFile);
  }

  // Returns the sequenceIds recorded for 'planNodeId' in 'splits', in order.
  static std::vector<int64_t> sequenceIds(
      const nlohmann::json& splits,
      const std::string& planNodeId) {
    std::vector<int64_t> ids;
    for (const auto& split : splits.at(planNodeId)) {
      ids.push_back(split.at("sequenceId").get<int64_t>());
    }
    return ids;
  }

  std::shared_ptr<exec::test::TempDirectoryPath> tempDir_;
  std::string dumpDir_;
};

TEST_F(PlanDumpTest, sanitizeTaskId) {
  EXPECT_EQ(
      sanitizeTaskIdForPlanDumpFile("20260922_000000_00001_abcde.1.0.2.0"),
      "20260922_000000_00001_abcde.1.0.2.0");
  EXPECT_EQ(sanitizeTaskIdForPlanDumpFile("error-task.0"), "error-task.0");
  EXPECT_EQ(sanitizeTaskIdForPlanDumpFile("a/b:c d"), "a_b_c_d");
  // IDs that differ only in '.' vs '_' must not share a dump file.
  EXPECT_NE(
      sanitizeTaskIdForPlanDumpFile("q.1.0"),
      sanitizeTaskIdForPlanDumpFile("q_1_0"));
  EXPECT_EQ(sanitizeTaskIdForPlanDumpFile(""), "task");
  EXPECT_EQ(sanitizeTaskIdForPlanDumpFile(".."), "task");
}

TEST_F(PlanDumpTest, dumpVeloxPlan) {
  auto plan = exec::test::PlanBuilder()
                  .values({makeRowVector({makeFlatVector<int64_t>({1, 2})})})
                  .planNode();
  dumpVeloxPlan(dumpDir_, "q.1.0.0.0", plan);

  auto json = readJson(dumpDir_ + "/q.1.0.0.0.json");
  EXPECT_EQ(json.at("id").get<std::string>(), plan->id());
}

TEST_F(PlanDumpTest, dumpSplitsAccumulatesAndDeduplicates) {
  const std::string taskId{"q.1.0.0.0"};
  dumpSplits(dumpDir_, taskId, {makeSource("0", {1, 2}), makeSource("1", {1})});
  // The coordinator re-sends unacknowledged splits along with new ones.
  dumpSplits(dumpDir_, taskId, {makeSource("0", {2, 3})});

  auto splits = readJson(splitsPath(taskId));
  EXPECT_EQ(splits.size(), 2);
  EXPECT_EQ(sequenceIds(splits, "0"), (std::vector<int64_t>{1, 2, 3}));
  EXPECT_EQ(sequenceIds(splits, "1"), (std::vector<int64_t>{1}));
}

TEST_F(PlanDumpTest, dumpSplitsWithoutSplitsWritesNothing) {
  const std::string taskId{"q.1.0.0.0"};
  dumpSplits(dumpDir_, taskId, {makeSource("0", {})});
  EXPECT_FALSE(fs::exists(splitsPath(taskId)));
}

TEST_F(PlanDumpTest, dumpSplitsReplacesCorruptFile) {
  const std::string taskId{"q.1.0.0.0"};
  fs::create_directories(dumpDir_);
  {
    std::ofstream outFile(splitsPath(taskId));
    outFile << "{\"0\": [";
  }
  dumpSplits(dumpDir_, taskId, {makeSource("0", {5})});

  auto splits = readJson(splitsPath(taskId));
  EXPECT_EQ(sequenceIds(splits, "0"), (std::vector<int64_t>{5}));
}

} // namespace
} // namespace facebook::presto
