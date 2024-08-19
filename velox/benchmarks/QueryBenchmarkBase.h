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

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>

#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

DECLARE_string(test_flags_file);
DECLARE_bool(include_results);
DECLARE_bool(include_custom_stats);
DECLARE_string(data_format);

namespace facebook::velox {

struct RunStats {
  std::map<std::string, std::string> flags;
  int64_t micros{0};
  int64_t rawInputBytes{0};
  int64_t userNanos{0};
  int64_t systemNanos{0};
  std::string output;

  std::string toString(bool detail) {
    std::stringstream out;
    out << succinctNanos(micros * 1000) << " "
        << succinctBytes(rawInputBytes / (micros / 1000000.0)) << "/s raw, "
        << succinctNanos(userNanos) << " user " << succinctNanos(systemNanos)
        << " system (" << (100 * (userNanos + systemNanos) / (micros * 1000))
        << "%), flags: ";
    for (auto& pair : flags) {
      out << pair.first << "=" << pair.second << " ";
    }
    out << std::endl << "======" << std::endl;
    if (detail) {
      out << std::endl << output << std::endl;
    }
    return out.str();
  }
};

struct ParameterDim {
  std::string flag;
  std::vector<std::string> values;
};

class QueryBenchmarkBase {
 public:
  virtual ~QueryBenchmarkBase() = default;
  virtual void initialize();
  void shutdown();
  std::pair<std::unique_ptr<exec::test::TaskCursor>, std::vector<RowVectorPtr>>
  run(const exec::test::TpchPlan& tpchPlan);

  virtual std::vector<std::shared_ptr<connector::ConnectorSplit>> listSplits(
      const std::string& path,
      int32_t numSplitsPerFile,
      const exec::test::TpchPlan& plan);

  static void ensureTaskCompletion(exec::Task* task);

  static bool validateDataFormat(
      const char* flagname,
      const std::string& value);

  static void printResults(
      const std::vector<RowVectorPtr>& results,
      std::ostream& out);

  void readCombinations();

  /// Entry point invoked with different settings to run the benchmark.
  virtual void runMain(std::ostream& out, RunStats& runStats) = 0;

  void runOne(std::ostream& outtt, RunStats& stats);

  void runCombinations(int32_t level);

  void runAllCombinations();

 protected:
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;
  std::shared_ptr<memory::MemoryAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> cache_;
  // Parameter combinations to try. Each element specifies a flag and possible
  // values. All permutations are tried.
  std::vector<ParameterDim> parameters_;

  std::vector<RunStats> runStats_;
};
} // namespace facebook::velox
