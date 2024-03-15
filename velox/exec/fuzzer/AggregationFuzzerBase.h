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

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Split.h"
#include "velox/exec/fuzzer/InputGenerator.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/fuzzer/ResultVerifier.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/expression/tests/utils/FuzzerToolkit.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DECLARE_int32(steps);

DECLARE_int32(duration_sec);

DECLARE_int32(batch_size);

DECLARE_int32(num_batches);

DECLARE_int32(max_num_varargs);

DECLARE_double(null_ratio);

DECLARE_string(repro_persist_path);

DECLARE_bool(persist_and_run_once);

DECLARE_bool(log_signature_stats);

namespace facebook::velox::exec::test {

using facebook::velox::test::CallableSignature;
using facebook::velox::test::SignatureTemplate;

constexpr const std::string_view kPlanNodeFileName = "plan_nodes";

class AggregationFuzzerBase {
 public:
  AggregationFuzzerBase(
      size_t initialSeed,
      const std::unordered_map<std::string, std::shared_ptr<ResultVerifier>>&
          customVerificationFunctions,
      const std::unordered_map<std::string, std::shared_ptr<InputGenerator>>&
          customInputGenerators,
      VectorFuzzer::Options::TimestampPrecision timestampPrecision,
      const std::unordered_map<std::string, std::string>& queryConfigs,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
      : customVerificationFunctions_{customVerificationFunctions},
        customInputGenerators_{customInputGenerators},
        queryConfigs_{queryConfigs},
        persistAndRunOnce_{FLAGS_persist_and_run_once},
        reproPersistPath_{FLAGS_repro_persist_path},
        referenceQueryRunner_{std::move(referenceQueryRunner)},
        vectorFuzzer_{getFuzzerOptions(timestampPrecision), pool_.get()} {
    filesystems::registerLocalFileSystem();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId, std::make_shared<core::MemConfig>());
    connector::registerConnector(hiveConnector);

    seed(initialSeed);
  }

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    std::vector<exec::Split> splits;
  };

  struct FunctionsStats {
    size_t numFunctions = 0;
    size_t numSignatures = 0;
    size_t numSupportedFunctions = 0;
    size_t numSupportedSignatures = 0;
  };

  struct SignatureStats {
    /// Number of times a signature was chosen.
    size_t numRuns{0};

    /// Number of times generated query plan failed.
    size_t numFailed{0};
  };

  enum ReferenceQueryErrorCode {
    kSuccess,
    kReferenceQueryFail,
    kReferenceQueryUnsupported
  };

 protected:
  static inline const std::string kHiveConnectorId = "test-hive";

  struct Stats {
    // Names of functions that were tested.
    std::unordered_set<std::string> functionNames;

    // Number of iterations using aggregations over sorted inputs.
    size_t numSortedInputs{0};

    // Number of iterations where results were verified against reference DB,
    size_t numVerified{0};

    // Number of iterations where results verification was skipped because
    // function results are non-determinisic.
    size_t numVerificationSkipped{0};

    // Number of iterations where results verification was skipped because
    // reference DB doesn't support the query.
    size_t numReferenceQueryNotSupported{0};

    // Number of iterations where results verification was skipped because
    // reference DB failed to execute the query.
    size_t numReferenceQueryFailed{0};

    // Number of iterations where aggregation failed.
    size_t numFailed{0};

    void print(size_t numIterations) const;

    void updateReferenceQueryStats(
        AggregationFuzzerBase::ReferenceQueryErrorCode errorCode);
  };

  bool addSignature(
      const std::string& name,
      const FunctionSignaturePtr& signature);

  void addAggregationSignatures(
      const AggregateFunctionSignatureMap& signatureMap);

  std::shared_ptr<InputGenerator> findInputGenerator(
      const CallableSignature& signature);

  static exec::Split makeSplit(const std::string& filePath);

  std::vector<exec::Split> makeSplits(
      const std::vector<RowVectorPtr>& inputs,
      const std::string& path);

  PlanWithSplits deserialize(const folly::dynamic& obj);

  static VectorFuzzer::Options getFuzzerOptions(
      VectorFuzzer::Options::TimestampPrecision timestampPrecision) {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringVariableLength = true;
    opts.stringLength = 4'000;
    opts.nullRatio = FLAGS_null_ratio;
    opts.timestampPrecision = timestampPrecision;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  // Generates at least one and up to 5 scalar columns to be used as grouping,
  // partition or sorting keys.
  // Column names are generated using template '<prefix>N', where N is
  // zero-based ordinal number of the column.
  std::vector<std::string> generateKeys(
      const std::string& prefix,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  // Similar to generateKeys, but restricts types to orderable types (i.e. no
  // maps).
  std::vector<std::string> generateSortingKeys(
      const std::string& prefix,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  std::pair<CallableSignature, SignatureStats&> pickSignature();

  std::vector<RowVectorPtr> generateInputData(
      std::vector<std::string> names,
      std::vector<TypePtr> types,
      const std::optional<CallableSignature>& signature);

  // Generate a RowVector of the given types of children with an additional
  // child named "row_number" of BIGINT row numbers that differentiates every
  // row. Row numbers start from 0. This additional input vector is needed for
  // result verification of window aggregations.
  std::vector<RowVectorPtr> generateInputDataWithRowNumber(
      std::vector<std::string> names,
      std::vector<TypePtr> types,
      const CallableSignature& signature);

  std::pair<std::optional<MaterializedRowMultiset>, ReferenceQueryErrorCode>
  computeReferenceResults(
      const core::PlanNodePtr& plan,
      const std::vector<RowVectorPtr>& input);

  velox::test::ResultOrError execute(
      const core::PlanNodePtr& plan,
      const std::vector<exec::Split>& splits = {},
      bool injectSpill = false,
      bool abandonPartial = false,
      int32_t maxDrivers = 2);

  // Will throw if referenceQueryRunner doesn't support
  // returning results as a vector.
  std::pair<
      std::optional<std::vector<RowVectorPtr>>,
      AggregationFuzzerBase::ReferenceQueryErrorCode>
  computeReferenceResultsAsVector(
      const core::PlanNodePtr& plan,
      const std::vector<RowVectorPtr>& input);

  void compare(
      const velox::test::ResultOrError& actual,
      bool customVerification,
      const std::vector<std::shared_ptr<ResultVerifier>>& customVerifiers,
      const velox::test::ResultOrError& expected);

  /// Returns false if the type or its children are unsupported.
  /// Currently returns false if type is Date,IntervalDayTime or Unknown.
  /// @param type
  /// @return bool
  bool isSupportedType(const TypePtr& type) const;

  // @param customVerification If false, results are compared as is. Otherwise,
  // only row counts are compared.
  // @param customVerifiers Custom verifier for each aggregate function. These
  // can be null. If not null and customVerification is true, custom verifier is
  // used to further verify the results.
  void testPlan(
      const PlanWithSplits& planWithSplits,
      bool injectSpill,
      bool abandonPartial,
      bool customVerification,
      const std::vector<std::shared_ptr<ResultVerifier>>& customVerifiers,
      const velox::test::ResultOrError& expected,
      int32_t maxDrivers = 2);

  void printSignatureStats();

  const std::unordered_map<std::string, std::shared_ptr<ResultVerifier>>
      customVerificationFunctions_;
  const std::unordered_map<std::string, std::shared_ptr<InputGenerator>>
      customInputGenerators_;
  const std::unordered_map<std::string, std::string> queryConfigs_;
  const bool persistAndRunOnce_;
  const std::string reproPersistPath_;

  std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner_;

  std::vector<CallableSignature> signatures_;
  std::vector<SignatureTemplate> signatureTemplates_;

  FunctionsStats functionsStats;

  // Stats for 'signatures_' and 'signatureTemplates_'. Stats for 'signatures_'
  // come before stats for 'signatureTemplates_'.
  std::vector<SignatureStats> signatureStats_;

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("leaf")};
  VectorFuzzer vectorFuzzer_;
};

// Returns true if the elapsed time is greater than or equal to
// FLAGS_duration_sec. If FLAGS_duration_sec is 0, returns true if the
// iterations is greater than or equal to FLAGS_steps.
template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

// Returns whether type is supported in TableScan. Empty Row type and Unknown
// type are not supported.
bool isTableScanSupported(const TypePtr& type);

// Prints statistics about supported and unsupported function signatures.
void printStats(const AggregationFuzzerBase::FunctionsStats& stats);

// Prints (n / total) in percentage format.
std::string printPercentageStat(size_t n, size_t total);

// Makes an aggregation call string for the given function name and arguments.
std::string makeFunctionCall(
    const std::string& name,
    const std::vector<std::string>& argNames,
    bool sortedInputs = false,
    bool distinctInputs = false,
    bool ignoreNulls = false);

// Returns a list of column names from c0 to cn.
std::vector<std::string> makeNames(size_t n);

// Persists plans to files under basePath.
void persistReproInfo(
    const std::vector<AggregationFuzzerBase::PlanWithSplits>& plans,
    const std::string& basePath);

// Returns a PrestoQueryRunner instance if prestoUrl is non-empty. Otherwise,
// returns a DuckQueryRunner instance and set disabled aggregation functions
// properly.
std::unique_ptr<ReferenceQueryRunner> setupReferenceQueryRunner(
    const std::string& prestoUrl,
    const std::string& runnerName);

// Returns the function name used in a WindowNode. The input `node` should be a
// pointer to a WindowNode.
std::vector<std::string> retrieveWindowFunctionName(
    const core::PlanNodePtr& node);

} // namespace facebook::velox::exec::test
