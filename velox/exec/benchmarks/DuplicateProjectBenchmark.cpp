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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

namespace {

static constexpr int32_t kNumVectors = 50;
static constexpr int32_t kRowsPerVector = 10'000;

class DuplicateProjectBenchmark : public HiveConnectorTestBase {
 public:
  explicit DuplicateProjectBenchmark() {
    HiveConnectorTestBase::SetUp();

    inputType_ = ROW({
        {"a", INTEGER()},
        {"b", INTEGER()},
        {"c", VARCHAR()},
        {"d", VARCHAR()},
    });

    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0.2;
    VectorFuzzer fuzzer(opts, pool_.get(), FLAGS_fuzzer_seed);
    std::vector<RowVectorPtr> inputVectors;
    for (auto i = 0; i < kNumVectors; ++i) {
      std::vector<VectorPtr> children;
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
      children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
      children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));

      inputVectors.emplace_back(makeRowVector(inputType_->names(), children));
    }

    sourceFilePath_ = TempFilePath::create();
    writeToFile(sourceFilePath_->getPath(), inputVectors);
  }

  ~DuplicateProjectBenchmark() override {
    HiveConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  void run(const std::string& filter, const std::vector<std::string>& project) {
    auto plan = PlanBuilder()
                    .tableScan(inputType_)
                    .filter(filter)
                    .project(project)
                    .planNode();
    auto result = exec::test::AssertQueryBuilder(plan)
                      .split(makeHiveConnectorSplit(sourceFilePath_->getPath()))
                      .copyResults(pool_.get());
    auto numResultRows = result->size();
    folly::doNotOptimizeAway(numResultRows);
  }

  void addBenchmarks() {
    folly::addBenchmark(
        __FILE__, "filter_integer_duplicate_project_integer", [this]() {
          run("a > 99999", {"b", "b"});
          return 1;
        });
    folly::addBenchmark(
        __FILE__, "filter_string_duplicate_project_string", [this]() {
          run("length(c) > 10", {"d", "d"});
          return 1;
        });
    folly::addBenchmark(__FILE__, "duplicate_project_integer", [this]() {
      run("true", {"b", "b"});
      return 1;
    });
    folly::addBenchmark(__FILE__, "duplicate_project_string", [this]() {
      run("true", {"d", "d"});
      return 1;
    });
    folly::addBenchmark(
        __FILE__, "filter_integers_duplicate_project_integers", [this]() {
          run("a > 9999 and b < 10000", {"a", "a", "b", "b"});
          return 1;
        });
    folly::addBenchmark(
        __FILE__, "filter_strings_duplicate_project_strings", [this]() {
          run("length(c) > 10 and length(d) < 20", {"c", "c", "d", "d"});
          return 1;
        });
  }

 private:
  RowTypePtr inputType_;
  std::shared_ptr<TempFilePath> sourceFilePath_;
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});
  memory::SharedArbitrator::registerFactory();
  functions::prestosql::registerAllScalarFunctions();

  DuplicateProjectBenchmark bm;
  bm.addBenchmarks();
  folly::runBenchmarks();
  return 0;
}
