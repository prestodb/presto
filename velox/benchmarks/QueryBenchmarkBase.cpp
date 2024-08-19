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

#include "velox/benchmarks/QueryBenchmarkBase.h"

DEFINE_string(data_format, "parquet", "Data format");

DEFINE_validator(
    data_format,
    &facebook::velox::QueryBenchmarkBase::validateDataFormat);

DEFINE_bool(
    include_custom_stats,
    false,
    "Include custom statistics along with execution statistics");
DEFINE_bool(include_results, false, "Include results in the output");
DEFINE_int32(num_drivers, 4, "Number of drivers");

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

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

namespace facebook::velox {

//  static
bool QueryBenchmarkBase::validateDataFormat(
    const char* flagname,
    const std::string& value) {
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

// static
void QueryBenchmarkBase::ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(exec::test::waitForTaskCompletion(task));
}

// static
void QueryBenchmarkBase::printResults(
    const std::vector<RowVectorPtr>& results,
    std::ostream& out) {
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

void QueryBenchmarkBase::initialize() {
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
      const cache::SsdCache::Config config(
          FLAGS_ssd_path,
          static_cast<uint64_t>(FLAGS_ssd_cache_gb) << 30,
          kNumSsdShards,
          cacheExecutor_.get(),
          static_cast<uint64_t>(FLAGS_ssd_checkpoint_interval_gb) << 30);
      ssdCache = std::make_unique<cache::SsdCache>(config);
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
  configurationValues[connector::hive::HiveConfig::kMaxCoalescedDistanceBytes] =
      std::to_string(FLAGS_max_coalesced_distance_bytes);
  configurationValues[connector::hive::HiveConfig::kPrefetchRowGroups] =
      std::to_string(FLAGS_parquet_prefetch_rowgroups);
  auto properties = std::make_shared<const config::ConfigBase>(
      std::move(configurationValues));

  // Create hive connector with config...
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, properties, ioExecutor_.get());
  connector::registerConnector(hiveConnector);
}

std::vector<std::shared_ptr<connector::ConnectorSplit>>
QueryBenchmarkBase::listSplits(
    const std::string& path,
    int32_t numSplitsPerFile,
    const exec::test::TpchPlan& plan) {
  std::vector<std::shared_ptr<connector::ConnectorSplit>> result;
  auto temp = HiveConnectorTestBase::makeHiveConnectorSplits(
      path, numSplitsPerFile, plan.dataFileFormat);
  for (auto& i : temp) {
    result.push_back(i);
  }
  return result;
}

void QueryBenchmarkBase::shutdown() {
  if (cache_) {
    cache_->shutdown();
  }
}

std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>>
QueryBenchmarkBase::run(const TpchPlan& tpchPlan) {
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
              auto splits = listSplits(path, numSplitsPerFile, tpchPlan);
              for (auto split : splits) {
                task->addSplit(entry.first, exec::Split(std::move(split)));
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

void QueryBenchmarkBase::readCombinations() {
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
    if (!dim.flag.empty() && !dim.values.empty()) {
      parameters_.push_back(dim);
    }
  }
}

void QueryBenchmarkBase::runCombinations(int32_t level) {
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
        cache_->clear();
      }
    }
    if (FLAGS_clear_ssd_cache) {
      if (cache_) {
        auto ssdCache = cache_->ssdCache();
        if (ssdCache) {
          ssdCache->clear();
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

void QueryBenchmarkBase::runOne(std::ostream& out, RunStats& stats) {
  std::stringstream result;
  uint64_t micros = 0;
  {
    struct rusage start;
    getrusage(RUSAGE_SELF, &start);
    MicrosecondTimer timer(&micros);
    runMain(out, stats);
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
  out << result.str();
}

void QueryBenchmarkBase::runAllCombinations() {
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

} // namespace facebook::velox
