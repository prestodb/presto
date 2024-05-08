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

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

namespace {
static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}

static bool validateDataFormat(const char* flagname, const std::string& value) {
  if ((value.compare("parquet") == 0) || (value.compare("dwrf") == 0)) {
    return true;
  }
  std::cout
      << fmt::format(
             "Invalid value for --{}: {}. Allowed values are [\"parquet\", \"dwrf\"]",
             flagname,
             value)
      << std::endl;
  return false;
}

void ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task));
}

void printResults(const std::vector<RowVectorPtr>& results, std::ostream& out) {
  out << "Results:" << std::endl;
  bool printType = true;
  for (const auto& vector : results) {
    // Print RowType only once.
    if (printType) {
      out << vector->type()->asRow().toString() << std::endl;
      printType = false;
    }
    for (vector_size_t i = 0; i < vector->size(); ++i) {
      out << vector->toString(i) << std::endl;
    }
  }
}
} // namespace

DEFINE_string(
    data_path,
    "",
    "Root path of TPC-H data. Data layout must follow Hive-style partitioning. "
    "Example layout for '-data_path=/data/tpch10'\n"
    "       /data/tpch10/customer\n"
    "       /data/tpch10/lineitem\n"
    "       /data/tpch10/nation\n"
    "       /data/tpch10/orders\n"
    "       /data/tpch10/part\n"
    "       /data/tpch10/partsupp\n"
    "       /data/tpch10/region\n"
    "       /data/tpch10/supplier\n"
    "If the above are directories, they contain the data files for "
    "each table. If they are files, they contain a file system path for each "
    "data file, one per line. This allows running against cloud storage or "
    "HDFS");

DEFINE_int32(
    run_query_verbose,
    -1,
    "Run a given query and print execution statistics");
DEFINE_int32(
    io_meter_column_pct,
    0,
    "Percentage of lineitem columns to "
    "include in IO meter query. The columns are sorted by name and the n% first "
    "are scanned");

DEFINE_bool(
    include_custom_stats,
    false,
    "Include custom statistics along with execution statistics");
DEFINE_bool(include_results, false, "Include results in the output");
DEFINE_int32(num_drivers, 4, "Number of drivers");
DEFINE_string(data_format, "parquet", "Data format");
DEFINE_int32(num_splits_per_file, 10, "Number of splits per file");
DEFINE_int32(
    cache_gb,
    0,
    "GB of process memory for cache and query.. if "
    "non-0, uses mmap to allocator and in-process data cache.");
DEFINE_int32(num_repeats, 1, "Number of times to run each query");
DEFINE_int32(num_io_threads, 8, "Threads for speculative IO");
DEFINE_string(
    test_flags_file,
    "",
    "Path to a file containing gflafs and "
    "values to try. Produces results for each flag combination "
    "sorted on performance");
DEFINE_bool(
    full_sorted_stats,
    true,
    "Add full stats to the report on  --test_flags_file");

DEFINE_string(ssd_path, "", "Directory for local SSD cache");
DEFINE_int32(ssd_cache_gb, 0, "Size of local SSD cache in GB");
DEFINE_int32(
    ssd_checkpoint_interval_gb,
    8,
    "Checkpoint every n "
    "GB new data in cache");
DEFINE_bool(
    clear_ram_cache,
    false,
    "Clear RAM cache before each query."
    "Flushes in process and OS file system cache (if root on Linux)");
DEFINE_bool(
    clear_ssd_cache,
    false,
    "Clears SSD cache before "
    "each query");

DEFINE_bool(
    warmup_after_clear,
    false,
    "Runs one warmup of the query before "
    "measured run. Use to run warm after clearing caches.");

DEFINE_validator(data_path, &notEmpty);
DEFINE_validator(data_format, &validateDataFormat);

DEFINE_int64(
    max_coalesced_bytes,
    128 << 20,
    "Maximum size of single coalesced IO");

DEFINE_int32(
    max_coalesced_distance_bytes,
    512 << 10,
    "Maximum distance in bytes in which coalesce will combine requests");

DEFINE_int32(
    parquet_prefetch_rowgroups,
    1,
    "Number of next row groups to "
    "prefetch. 1 means prefetch the next row group before decoding "
    "the current one");

DEFINE_int32(split_preload_per_driver, 2, "Prefetch split metadata");

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

std::shared_ptr<TpchQueryBuilder> queryBuilder;

class TpchBenchmark {
 public:
  void initialize() {
    if (FLAGS_cache_gb) {
      memory::MemoryManagerOptions options;
      int64_t memoryBytes = FLAGS_cache_gb * (1LL << 30);
      options.useMmapAllocator = true;
      options.allocatorCapacity = memoryBytes;
      options.useMmapArena = true;
      options.mmapArenaCapacityRatio = 1;
      memory::MemoryManager::testingSetInstance(options);
      std::unique_ptr<cache::SsdCache> ssdCache;
      if (FLAGS_ssd_cache_gb) {
        constexpr int32_t kNumSsdShards = 16;
        cacheExecutor_ =
            std::make_unique<folly::IOThreadPoolExecutor>(kNumSsdShards);
        ssdCache = std::make_unique<cache::SsdCache>(
            FLAGS_ssd_path,
            static_cast<uint64_t>(FLAGS_ssd_cache_gb) << 30,
            kNumSsdShards,
            cacheExecutor_.get(),
            static_cast<uint64_t>(FLAGS_ssd_checkpoint_interval_gb) << 30);
      }

      cache_ = cache::AsyncDataCache::create(
          memory::memoryManager()->allocator(), std::move(ssdCache));
      cache::AsyncDataCache::setInstance(cache_.get());
    } else {
      memory::MemoryManager::testingSetInstance({});
    }
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    filesystems::registerLocalFileSystem();

    ioExecutor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_num_io_threads);

    // Add new values into the hive configuration...
    auto configurationValues = std::unordered_map<std::string, std::string>();
    configurationValues[connector::hive::HiveConfig::kMaxCoalescedBytes] =
        std::to_string(FLAGS_max_coalesced_bytes);
    configurationValues
        [connector::hive::HiveConfig::kMaxCoalescedDistanceBytes] =
            std::to_string(FLAGS_max_coalesced_distance_bytes);
    auto properties =
        std::make_shared<const core::MemConfig>(configurationValues);

    // Create hive connector with config...
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, properties, ioExecutor_.get());
    connector::registerConnector(hiveConnector);
  }

  void shutdown() {
    cache_->shutdown();
  }

  std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> run(
      const TpchPlan& tpchPlan) {
    int32_t repeat = 0;
    try {
      for (;;) {
        CursorParameters params;
        params.maxDrivers = FLAGS_num_drivers;
        params.planNode = tpchPlan.plan;
        params.queryConfigs[core::QueryConfig::kMaxSplitPreloadPerDriver] =
            std::to_string(FLAGS_split_preload_per_driver);
        const int numSplitsPerFile = FLAGS_num_splits_per_file;

        bool noMoreSplits = false;
        auto addSplits = [&](exec::Task* task) {
          if (!noMoreSplits) {
            for (const auto& entry : tpchPlan.dataFiles) {
              for (const auto& path : entry.second) {
                auto const splits =
                    HiveConnectorTestBase::makeHiveConnectorSplits(
                        path, numSplitsPerFile, tpchPlan.dataFileFormat);
                for (const auto& split : splits) {
                  task->addSplit(entry.first, exec::Split(split));
                }
              }
              task->noMoreSplits(entry.first);
            }
          }
          noMoreSplits = true;
        };
        auto result = readCursor(params, addSplits);
        ensureTaskCompletion(result.first->task().get());
        if (++repeat >= FLAGS_num_repeats) {
          return result;
        }
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "Query terminated with: " << e.what();
      return {nullptr, std::vector<RowVectorPtr>()};
    }
  }

  void runMain(std::ostream& out, RunStats& runStats) {
    if (FLAGS_run_query_verbose == -1 && FLAGS_io_meter_column_pct == 0) {
      folly::runBenchmarks();
    } else {
      const auto queryPlan = FLAGS_io_meter_column_pct > 0
          ? queryBuilder->getIoMeterPlan(FLAGS_io_meter_column_pct)
          : queryBuilder->getQueryPlan(FLAGS_run_query_verbose);
      auto [cursor, actualResults] = run(queryPlan);
      if (!cursor) {
        LOG(ERROR) << "Query terminated with error. Exiting";
        exit(1);
      }
      auto task = cursor->task();
      ensureTaskCompletion(task.get());
      if (FLAGS_include_results) {
        printResults(actualResults, out);
        out << std::endl;
      }
      const auto stats = task->taskStats();
      int64_t rawInputBytes = 0;
      for (auto& pipeline : stats.pipelineStats) {
        auto& first = pipeline.operatorStats[0];
        if (first.operatorType == "TableScan") {
          rawInputBytes += first.rawInputBytes;
        }
      }
      runStats.rawInputBytes = rawInputBytes;
      out << fmt::format(
                 "Execution time: {}",
                 succinctMillis(
                     stats.executionEndTimeMs - stats.executionStartTimeMs))
          << std::endl;
      out << fmt::format(
                 "Splits total: {}, finished: {}",
                 stats.numTotalSplits,
                 stats.numFinishedSplits)
          << std::endl;
      out << printPlanWithStats(
                 *queryPlan.plan, stats, FLAGS_include_custom_stats)
          << std::endl;
    }
  }

  void readCombinations() {
    std::ifstream file(FLAGS_test_flags_file);
    std::string line;
    while (std::getline(file, line)) {
      ParameterDim dim;
      int32_t previous = 0;
      for (auto i = 0; i < line.size(); ++i) {
        if (line[i] == ':') {
          dim.flag = line.substr(0, i);
          previous = i + 1;
        } else if (line[i] == ',') {
          dim.values.push_back(line.substr(previous, i - previous));
          previous = i + 1;
        }
      }
      if (previous < line.size()) {
        dim.values.push_back(line.substr(previous, line.size() - previous));
      }

      parameters_.push_back(dim);
    }
  }

  void runCombinations(int32_t level) {
    if (level == parameters_.size()) {
      if (FLAGS_clear_ram_cache) {
#ifdef linux
        // system("echo 3 >/proc/sys/vm/drop_caches");
        bool success = false;
        auto fd = open("/proc//sys/vm/drop_caches", O_WRONLY);
        if (fd > 0) {
          success = write(fd, "3", 1) == 1;
          close(fd);
        }
        if (!success) {
          LOG(ERROR) << "Failed to clear OS disk cache: errno=" << errno;
        }
#endif

        if (cache_) {
          cache_->testingClear();
        }
      }
      if (FLAGS_clear_ssd_cache) {
        if (cache_) {
          auto ssdCache = cache_->ssdCache();
          if (ssdCache) {
            ssdCache->testingClear();
          }
        }
      }
      if (FLAGS_warmup_after_clear) {
        std::stringstream result;
        RunStats ignore;
        runMain(result, ignore);
      }
      RunStats stats;
      std::stringstream result;
      uint64_t micros = 0;
      {
        struct rusage start;
        getrusage(RUSAGE_SELF, &start);
        MicrosecondTimer timer(&micros);
        runMain(result, stats);
        struct rusage final;
        getrusage(RUSAGE_SELF, &final);
        auto tvNanos = [](struct timeval tv) {
          return tv.tv_sec * 1000000000 + tv.tv_usec * 1000;
        };
        stats.userNanos = tvNanos(final.ru_utime) - tvNanos(start.ru_utime);
        stats.systemNanos = tvNanos(final.ru_stime) - tvNanos(start.ru_stime);
      }
      stats.micros = micros;
      stats.output = result.str();
      for (auto i = 0; i < parameters_.size(); ++i) {
        std::string name;
        gflags::GetCommandLineOption(parameters_[i].flag.c_str(), &name);
        stats.flags[parameters_[i].flag] = name;
      }
      runStats_.push_back(std::move(stats));
    } else {
      auto& flag = parameters_[level].flag;
      for (auto& value : parameters_[level].values) {
        std::string result =
            gflags::SetCommandLineOption(flag.c_str(), value.c_str());
        if (result.empty()) {
          LOG(ERROR) << "Failed to set " << flag << "=" << value;
        }
        std::cout << result << std::endl;
        runCombinations(level + 1);
      }
    }
  }

  void runAllCombinations() {
    readCombinations();
    runCombinations(0);
    std::sort(
        runStats_.begin(),
        runStats_.end(),
        [](const RunStats& left, const RunStats& right) {
          return left.micros < right.micros;
        });
    for (auto& stats : runStats_) {
      std::cout << stats.toString(false);
    }
    if (FLAGS_full_sorted_stats) {
      std::cout << "Detail for stats:" << std::endl;
      for (auto& stats : runStats_) {
        std::cout << stats.toString(true);
      }
    }
  }

  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;
  std::shared_ptr<memory::MemoryAllocator> allocator_;
  std::shared_ptr<cache::AsyncDataCache> cache_;
  // Parameter combinations to try. Each element specifies a flag and possible
  // values. All permutations are tried.
  std::vector<ParameterDim> parameters_;

  std::vector<RunStats> runStats_;
};

TpchBenchmark benchmark;

BENCHMARK(q1) {
  const auto planContext = queryBuilder->getQueryPlan(1);
  benchmark.run(planContext);
}

BENCHMARK(q3) {
  const auto planContext = queryBuilder->getQueryPlan(3);
  benchmark.run(planContext);
}

BENCHMARK(q5) {
  const auto planContext = queryBuilder->getQueryPlan(5);
  benchmark.run(planContext);
}

BENCHMARK(q6) {
  const auto planContext = queryBuilder->getQueryPlan(6);
  benchmark.run(planContext);
}

BENCHMARK(q7) {
  const auto planContext = queryBuilder->getQueryPlan(7);
  benchmark.run(planContext);
}

BENCHMARK(q8) {
  const auto planContext = queryBuilder->getQueryPlan(8);
  benchmark.run(planContext);
}

BENCHMARK(q9) {
  const auto planContext = queryBuilder->getQueryPlan(9);
  benchmark.run(planContext);
}

BENCHMARK(q10) {
  const auto planContext = queryBuilder->getQueryPlan(10);
  benchmark.run(planContext);
}

BENCHMARK(q12) {
  const auto planContext = queryBuilder->getQueryPlan(12);
  benchmark.run(planContext);
}

BENCHMARK(q13) {
  const auto planContext = queryBuilder->getQueryPlan(13);
  benchmark.run(planContext);
}

BENCHMARK(q14) {
  const auto planContext = queryBuilder->getQueryPlan(14);
  benchmark.run(planContext);
}

BENCHMARK(q15) {
  const auto planContext = queryBuilder->getQueryPlan(15);
  benchmark.run(planContext);
}

BENCHMARK(q16) {
  const auto planContext = queryBuilder->getQueryPlan(16);
  benchmark.run(planContext);
}

BENCHMARK(q17) {
  const auto planContext = queryBuilder->getQueryPlan(17);
  benchmark.run(planContext);
}

BENCHMARK(q18) {
  const auto planContext = queryBuilder->getQueryPlan(18);
  benchmark.run(planContext);
}

BENCHMARK(q19) {
  const auto planContext = queryBuilder->getQueryPlan(19);
  benchmark.run(planContext);
}

BENCHMARK(q20) {
  const auto planContext = queryBuilder->getQueryPlan(20);
  benchmark.run(planContext);
}

BENCHMARK(q21) {
  const auto planContext = queryBuilder->getQueryPlan(21);
  benchmark.run(planContext);
}

BENCHMARK(q22) {
  const auto planContext = queryBuilder->getQueryPlan(22);
  benchmark.run(planContext);
}

int tpchBenchmarkMain() {
  benchmark.initialize();
  queryBuilder =
      std::make_shared<TpchQueryBuilder>(toFileFormat(FLAGS_data_format));
  queryBuilder->initialize(FLAGS_data_path);
  if (FLAGS_test_flags_file.empty()) {
    RunStats ignore;
    benchmark.runMain(std::cout, ignore);
  } else {
    benchmark.runAllCombinations();
  }
  benchmark.shutdown();
  queryBuilder.reset();
  return 0;
}
