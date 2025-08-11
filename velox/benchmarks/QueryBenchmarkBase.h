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
#include <folly/Benchmark.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"

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

  std::string toString(bool detail) const;
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
  std::pair<std::unique_ptr<exec::TaskCursor>, std::vector<RowVectorPtr>> run(
      const exec::test::TpchPlan& tpchPlan);

  virtual std::vector<std::shared_ptr<connector::ConnectorSplit>> listSplits(
      const std::string& path,
      int32_t numSplitsPerFile,
      const exec::test::TpchPlan& plan);

  static void ensureTaskCompletion(exec::Task* task);

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

  // QueryConfig properties. May be part of parameter sweep.
  std::unordered_map<std::string, std::string> config_;

  // Parameter combinations to try. Each element specifies a flag and possible
  // values. All permutations are tried.
  std::vector<ParameterDim> parameters_;

  std::vector<RunStats> runStats_;
};
} // namespace facebook::velox
