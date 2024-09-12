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
#include "velox/common/process/TraceContext.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/dwrf/writer/WriterContext.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/WaveHiveDataSource.h"
#include "velox/experimental/wave/exec/tests/utils/FileFormat.h"
#include "velox/experimental/wave/exec/tests/utils/WaveTestSplitReader.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

DEFINE_string(
    data_path,
    "",
    "Root path of test data. Data layout must follow Hive-style partitioning. "
    "If the content are directories, they contain the data files for "
    "each table. If they are files, they contain a file system path for each "
    "data file, one per line. This allows running against cloud storage or "
    "HDFS");

DEFINE_bool(
    generate,
    true,
    "Generate input data. If false, data_path must "
    "contain a directory with a subdirectory per table.");

DEFINE_bool(preload, false, "Preload Wave data into RAM before starting query");

DEFINE_bool(wave, true, "Run benchmark with Wave");

DEFINE_int32(num_columns, 10, "Number of columns in test table");

DEFINE_int64(filter_pass_pct, 100, "Passing % for one filter");

DEFINE_int32(null_pct, 0, "Pct of null values in columns");

DEFINE_int32(num_column_filters, 0, "Number of columns wit a filter");

DEFINE_int32(num_expr_filters, 0, "Number of columns  with a filter expr");

DEFINE_int32(
    num_arithmetic,
    0,
    "Number of arithmetic ops per column after filters");

DEFINE_int32(rows_per_stripe, 200000, "Rows in a stripe");

DEFINE_int64(num_rows, 1000000000, "Rows in test table");

DEFINE_int32(
    run_query_verbose,
    -1,
    "Run a given query and print execution statistics");

class WaveBenchmark : public QueryBenchmarkBase {
 public:
  ~WaveBenchmark() {
    wave::test::Table::dropAll();
  }

  void initialize() override {
    QueryBenchmarkBase::initialize();
    if (FLAGS_wave) {
      wave::registerWave();
      wave::WaveHiveDataSource::registerConnector();
      wave::test::WaveTestSplitReader::registerTestSplitReader();
    }
    rootPool_ = memory::memoryManager()->addRootPool("WaveBenchmark");
    leafPool_ = rootPool_->addLeafChild("WaveBenchmark");
  }

  void makeData(
      const RowTypePtr& type,
      int32_t numVectors,
      int32_t vectorSize,
      float nullPct = 0) {
    auto vectors = makeVectors(type, numVectors, vectorSize, nullPct / 100);
    int32_t cnt = 0;
    for (auto& vector : vectors) {
      makeRange(vector, 1000000000, nullPct == 0);
      auto rn = vector->childAt(type->size() - 1)->as<FlatVector<int64_t>>();
      for (auto i = 0; i < rn->size(); ++i) {
        rn->set(i, cnt++);
      }
    }
    if (FLAGS_wave) {
      makeTable(FLAGS_data_path + "/test.wave", vectors);
      if (FLAGS_generate) {
        auto table =
            wave::test::Table::getTable(FLAGS_data_path + "/test.wave");
        table->toFile(FLAGS_data_path + "/test.wave");
      }
    } else {
      std::string temp = FLAGS_data_path + "/data." + FLAGS_data_format;
      auto config = std::make_shared<dwrf::Config>();
      config->set(dwrf::Config::COMPRESSION, common::CompressionKind_NONE);
      config->set(
          dwrf::Config::STRIPE_SIZE,
          static_cast<uint64_t>(FLAGS_rows_per_stripe * FLAGS_num_columns * 4));
      writeToFile(temp, vectors, config, vectors.front()->type());
    }
  }

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector,
      float nullRatio = 0) {
    std::vector<RowVectorPtr> vectors;
    options_.vectorSize = rowsPerVector;
    options_.nullRatio = nullRatio;
    fuzzer_ = std::make_unique<VectorFuzzer>(options_, leafPool_.get());
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = fuzzer_->fuzzInputFlatRow(rowType);
      vectors.push_back(vector);
    }
    return vectors;
  }

  void makeRange(
      RowVectorPtr row,
      int64_t mod = std::numeric_limits<int64_t>::max(),
      bool notNull = true) {
    for (auto i = 0; i < row->type()->size(); ++i) {
      auto child = row->childAt(i);
      if (auto ints = child->as<FlatVector<int64_t>>()) {
        for (auto i = 0; i < child->size(); ++i) {
          if (!notNull && ints->isNullAt(i)) {
            continue;
          }
          ints->set(i, ints->valueAt(i) % mod);
        }
      }
      if (notNull) {
        child->clearNulls(0, row->size());
      }
    }
  }

  wave::test::SplitVector makeTable(
      const std::string& name,
      std::vector<RowVectorPtr>& rows) {
    wave::test::Table::dropTable(name);
    return wave::test::Table::defineTable(name, rows)->splits();
  }

  void writeToFile(
      const std::string& filePath,
      const std::vector<RowVectorPtr>& vectors,
      std::shared_ptr<dwrf::Config> config,
      const TypePtr& schema) {
    dwrf::WriterOptions options;
    options.config = config;
    options.schema = schema;
    auto localWriteFile =
        std::make_unique<LocalWriteFile>(filePath, true, false);
    auto sink = std::make_unique<dwio::common::WriteFileSink>(
        std::move(localWriteFile), filePath);
    auto childPool =
        rootPool_->addAggregateChild("HiveConnectorTestBase.Writer");
    options.memoryPool = childPool.get();
    facebook::velox::dwrf::Writer writer{std::move(sink), options};
    for (size_t i = 0; i < vectors.size(); ++i) {
      writer.write(vectors[i]);
    }
    writer.close();
  }

  exec::test::TpchPlan getQueryPlan(int32_t query) {
    switch (query) {
      case 1: {
        if (!type_) {
          type_ = makeType();
        }

        exec::test::TpchPlan plan;
        if (FLAGS_wave) {
          plan.dataFiles["0"] = {FLAGS_data_path + "/test.wave"};
        } else {
          plan.dataFiles["0"] = {FLAGS_data_path + "/data.dwrf"};
          plan.dataFileFormat = toFileFormat(FLAGS_data_format);
        }
        int64_t bound = (1'000'000'000LL * FLAGS_filter_pass_pct) / 100;
        std::vector<std::string> scanFilters;
        for (auto i = 0; i < FLAGS_num_column_filters; ++i) {
          scanFilters.push_back(fmt::format("c{} < {}", i, bound));
        }
        auto builder =
            PlanBuilder(leafPool_.get()).tableScan(type_, scanFilters);

        for (auto i = 0; i < FLAGS_num_expr_filters; ++i) {
          builder = builder.filter(
              fmt::format("c{} < {}", FLAGS_num_column_filters + i, bound));
        }

        std::vector<std::string> aggInputs;
        if (FLAGS_num_arithmetic > 0) {
          std::vector<std::string> projects;
          for (auto c = 0; c < type_->size(); ++c) {
            std::string expr = fmt::format("c{} ", c);
            for (auto i = 0; i < FLAGS_num_arithmetic; ++i) {
              expr += fmt::format(" + c{}", c);
            }
            expr += fmt::format(" as f{}", c);
            projects.push_back(std::move(expr));
            aggInputs.push_back(fmt::format("f{}", c));
          }
          builder = builder.project(std::move(projects));
        } else {
          for (auto i = 0; i < type_->size(); ++i) {
            aggInputs.push_back(fmt::format("c{}", i));
          }
        }
        std::vector<std::string> aggs;
        for (auto i = 0; i < aggInputs.size(); ++i) {
          aggs.push_back(fmt::format("sum({})", aggInputs[i]));
        }

        plan.plan = builder.singleAggregation({}, aggs).planNode();

        plan.dataFileFormat =
            FLAGS_wave ? FileFormat::UNKNOWN : FileFormat::DWRF;
        return plan;
      }
      default:
        VELOX_FAIL("Bad query number");
    }
  }

  void prepareQuery(int32_t query) {
    switch (query) {
      case 1: {
        type_ = makeType();
        auto numVectors =
            std::max<int64_t>(1, FLAGS_num_rows / FLAGS_rows_per_stripe);
        if (FLAGS_generate) {
          makeData(
              type_, numVectors, FLAGS_num_rows / numVectors, FLAGS_null_pct);
        } else {
          loadData();
        }
        break;
      }
      default:
        VELOX_FAIL("Bad query number");
    }
  }

  void loadData() {
    if (FLAGS_wave) {
      auto table =
          wave::test::Table::getTable(FLAGS_data_path + "/test.wave", true);
      table->fromFile(FLAGS_data_path + "/test.wave");
      if (FLAGS_preload) {
        table->loadData(leafPool_);
      }
    }
  }

  std::vector<std::shared_ptr<connector::ConnectorSplit>> listSplits(
      const std::string& path,
      int32_t numSplitsPerFile,
      const TpchPlan& plan) override {
    if (plan.dataFileFormat == FileFormat::UNKNOWN) {
      auto table = wave::test::Table::getTable(path);
      return table->splits();
    }
    return QueryBenchmarkBase::listSplits(path, numSplitsPerFile, plan);
  }

  void runMain(std::ostream& out, RunStats& runStats) override {
    if (FLAGS_run_query_verbose == -1) {
      folly::runBenchmarks();
    } else {
      const auto queryPlan = getQueryPlan(FLAGS_run_query_verbose);
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
        if (first.operatorType == "TableScan" || first.operatorType == "Wave") {
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

  RowTypePtr makeType() {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (auto i = 0; i < FLAGS_num_columns; ++i) {
      names.push_back(fmt::format("c{}", i));
      types.push_back(BIGINT());
    }
    return ROW(std::move(names), std::move(types));
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  RowTypePtr type_;
  VectorFuzzer::Options options_;
  std::unique_ptr<VectorFuzzer> fuzzer_;
};

void waveBenchmarkMain() {
  auto benchmark = std::make_unique<WaveBenchmark>();
  benchmark->initialize();
  if (FLAGS_run_query_verbose != -1) {
    benchmark->prepareQuery(FLAGS_run_query_verbose);
  }
  if (FLAGS_test_flags_file.empty()) {
    RunStats stats;
    benchmark->runOne(std::cout, stats);
    std::cout << stats.toString(false);
  } else {
    benchmark->runAllCombinations();
  }
  benchmark->shutdown();
}

int main(int argc, char** argv) {
  std::string kUsage(
      "This program benchmarks Wave. Run 'velox_wave_benchmark -helpon=WaveBenchmark' for available options.\n");
  gflags::SetUsageMessage(kUsage);
  folly::Init init{&argc, &argv, false};
  facebook::velox::wave::printKernels();
  waveBenchmarkMain();
  return 0;
}
