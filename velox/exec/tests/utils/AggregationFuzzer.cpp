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
#include "velox/exec/tests/utils/AggregationFuzzer.h"
#include <boost/random/uniform_int_distribution.hpp>
#include "velox/common/base/Fs.h"

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/tests/ArgumentTypeFuzzer.h"

#include "velox/exec/PartitionFunction.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/expression/tests/FuzzerToolkit.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DEFINE_int32(steps, 10, "Number of plans to generate and execute.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 10, "The number of generated vectors.");

DEFINE_int32(
    max_num_varargs,
    5,
    "The maximum number of variadic arguments fuzzer will generate for "
    "functions that accept variadic arguments. Fuzzer will generate up to "
    "max_num_varargs arguments for the variadic list in addition to the "
    "required arguments by the function.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null constant to the plan, or null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_string(
    repro_persist_path,
    "",
    "Directory path for persistence of data and SQL when fuzzer fails for "
    "future reproduction. Empty string disables this feature.");

DEFINE_bool(
    enable_window_reference_verification,
    false,
    "When true, the results of the window aggregation will be compared to reference DB results");

DEFINE_bool(
    persist_and_run_once,
    false,
    "Persist repro info before evaluation and only run one iteration. "
    "This is to rerun with the seed number and persist repro info upon a "
    "crash failure. Only effective if repro_persist_path is set.");

using facebook::velox::test::CallableSignature;
using facebook::velox::test::SignatureTemplate;

namespace facebook::velox::exec::test {
namespace {

class AggregationFuzzer {
 public:
  AggregationFuzzer(
      AggregateFunctionSignatureMap signatureMap,
      size_t seed,
      const std::unordered_map<std::string, std::string>&
          customVerificationFunctions,
      VectorFuzzer::Options::TimestampPrecision timestampPrecision,
      const std::unordered_map<std::string, std::string>& queryConfigs,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    std::vector<exec::Split> splits;
  };

  void go();
  void go(const std::string& planPath);

 private:
  static inline const std::string kHiveConnectorId = "test-hive";

  static exec::Split makeSplit(const std::string& filePath);

  PlanWithSplits deserialize(const folly::dynamic& obj);

  struct Stats {
    // Names of functions that were tested.
    std::unordered_set<std::string> functionNames;

    // Number of iterations using masked aggregation.
    size_t numMask{0};

    // Number of iterations using group-by aggregation.
    size_t numGroupBy{0};

    // Number of iterations using global aggregation.
    size_t numGlobal{0};

    // Number of iterations using distinct aggregation.
    size_t numDistinct{0};

    // Number of iterations using window expressions.
    size_t numWindow{0};

    // Number of iterations where results were verified against reference DB,
    size_t numVerified{0};

    // Number of iterations where results verification was skipped because
    // function results are non-determinisic.
    size_t numVerificationSkipped{0};

    // Number of iterations where results verification was skipped because
    // reference DB doesn't support the function.
    size_t numVerificationNotSupported{0};

    // Number of iterations where results verification was skipped because
    // reference DB failed to execute the query.
    size_t numReferenceQueryFailed{0};

    // Number of iterations where aggregation failed.
    size_t numFailed{0};

    void print(size_t numIterations) const;
  };

  static VectorFuzzer::Options getFuzzerOptions(
      VectorFuzzer::Options::TimestampPrecision timestampPrecision) {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
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

  // Generate at least one and up to 5 scalar columns to be used as grouping,
  // partition or sorting keys.
  // Column names are generated using template '<prefix>N', where N is
  // zero-based ordinal number of the column.
  std::vector<std::string> generateKeys(
      const std::string& prefix,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  CallableSignature pickSignature();

  std::vector<RowVectorPtr> generateInputData(
      std::vector<std::string> names,
      std::vector<TypePtr> types);

  // Generate a RowVector of the given types of children with an additional
  // child named "row_number" of BIGINT row numbers that differentiates every
  // row. Row numbers start from 0. This additional input vector is needed for
  // result verification of window aggregations.
  std::vector<RowVectorPtr> generateInputDataWithRowNumber(
      std::vector<std::string> names,
      std::vector<TypePtr> types);

  void verifyWindow(
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<RowVectorPtr>& input,
      bool customVerification,
      bool enableWindowVerification);

  void verifyAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      const std::vector<RowVectorPtr>& input,
      bool customVerification,
      const std::vector<std::string>& projections);

  void verifyAggregation(const std::vector<PlanWithSplits>& plans);

  std::optional<MaterializedRowMultiset> computeReferenceResults(
      const core::PlanNodePtr& plan,
      const std::vector<RowVectorPtr>& input);

  velox::test::ResultOrError execute(
      const core::PlanNodePtr& plan,
      const std::vector<exec::Split>& splits = {},
      bool injectSpill = false,
      bool abandonPartial = false);

  static bool hasPartialGroupBy(const core::PlanNodePtr& plan) {
    auto partialAgg = core::PlanNode::findFirstNode(
        plan.get(), [](const core::PlanNode* node) {
          if (auto aggregation =
                  dynamic_cast<const core::AggregationNode*>(node)) {
            return aggregation->step() ==
                core::AggregationNode::Step::kPartial &&
                !aggregation->groupingKeys().empty();
          }

          return false;
        });
    return partialAgg != nullptr;
  }

  void testPlans(
      const std::vector<PlanWithSplits>& plans,
      bool verifyResults,
      const velox::test::ResultOrError& expected) {
    for (auto i = 0; i < plans.size(); ++i) {
      const auto& planWithSplits = plans[i];

      LOG(INFO) << "Testing plan #" << i;
      testPlan(
          planWithSplits,
          false /*injectSpill*/,
          false /*abandonPartial*/,
          verifyResults,
          expected);

      LOG(INFO) << "Testing plan #" << i << " with spilling";
      testPlan(
          planWithSplits,
          true /*injectSpill*/,
          false /*abandonPartial*/,
          verifyResults,
          expected);

      if (hasPartialGroupBy(planWithSplits.plan)) {
        LOG(INFO) << "Testing plan #" << i
                  << " with forced abandon-partial-aggregation";
        testPlan(
            planWithSplits,
            false /*injectSpill*/,
            true /*abandonPartial*/,
            verifyResults,
            expected);
      }
    }
  }

  void testPlan(
      const PlanWithSplits& planWithSplits,
      bool injectSpill,
      bool abandonPartial,
      bool verifyResults,
      const velox::test::ResultOrError& expected);

  const std::unordered_map<std::string, std::string>
      customVerificationFunctions_;
  const std::unordered_map<std::string, std::string> queryConfigs_;
  const bool persistAndRunOnce_;
  const std::string reproPersistPath_;

  std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner_;

  std::vector<CallableSignature> signatures_;
  std::vector<SignatureTemplate> signatureTemplates_;

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::defaultMemoryManager().addRootPool()};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild("leaf")};
  VectorFuzzer vectorFuzzer_;

  Stats stats_;
};
} // namespace

void aggregateFuzzer(
    AggregateFunctionSignatureMap signatureMap,
    size_t seed,
    const std::unordered_map<std::string, std::string>&
        customVerificationFunctions,
    VectorFuzzer::Options::TimestampPrecision timestampPrecision,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    const std::optional<std::string>& planPath,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
  auto aggregationFuzzer = AggregationFuzzer(
      std::move(signatureMap),
      seed,
      customVerificationFunctions,
      timestampPrecision,
      queryConfigs,
      std::move(referenceQueryRunner));
  planPath.has_value() ? aggregationFuzzer.go(planPath.value())
                       : aggregationFuzzer.go();
}

namespace {

std::string printStat(size_t n, size_t total) {
  return fmt::format("{} ({:.2f}%)", n, (double)n / total * 100);
}

void printStats(
    size_t numFunctions,
    size_t numSignatures,
    size_t numSupportedFunctions,
    size_t numSupportedSignatures) {
  LOG(INFO) << fmt::format(
      "Total functions: {} ({} signatures)", numFunctions, numSignatures);
  LOG(INFO) << "Functions with at least one supported signature: "
            << printStat(numSupportedFunctions, numFunctions);

  size_t numNotSupportedFunctions = numFunctions - numSupportedFunctions;
  LOG(INFO) << "Functions with no supported signature: "
            << printStat(numNotSupportedFunctions, numFunctions);
  LOG(INFO) << "Supported function signatures: "
            << printStat(numSupportedSignatures, numSignatures);

  size_t numNotSupportedSignatures = numSignatures - numSupportedSignatures;
  LOG(INFO) << "Unsupported function signatures: "
            << printStat(numNotSupportedSignatures, numSignatures);
}

AggregationFuzzer::AggregationFuzzer(
    AggregateFunctionSignatureMap signatureMap,
    size_t initialSeed,
    const std::unordered_map<std::string, std::string>&
        customVerificationFunctions,
    VectorFuzzer::Options::TimestampPrecision timestampPrecision,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
    : customVerificationFunctions_{customVerificationFunctions},
      queryConfigs_{queryConfigs},
      persistAndRunOnce_{FLAGS_persist_and_run_once},
      reproPersistPath_{FLAGS_repro_persist_path},
      referenceQueryRunner_{std::move(referenceQueryRunner)},
      vectorFuzzer_{getFuzzerOptions(timestampPrecision), pool_.get()} {
  filesystems::registerLocalFileSystem();
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, nullptr);
  connector::registerConnector(hiveConnector);

  seed(initialSeed);
  VELOX_CHECK(!signatureMap.empty(), "No function signatures available.");

  if (persistAndRunOnce_ && reproPersistPath_.empty()) {
    std::cout
        << "--repro_persist_path must be specified if --persist_and_run_once is specified"
        << std::endl;
    exit(1);
  }

  size_t numFunctions = 0;
  size_t numSignatures = 0;
  size_t numSupportedFunctions = 0;
  size_t numSupportedSignatures = 0;

  for (auto& [name, signatures] : signatureMap) {
    ++numFunctions;
    bool hasSupportedSignature = false;
    for (auto& signature : signatures) {
      ++numSignatures;

      if (signature->variableArity()) {
        LOG(WARNING) << "Skipping variadic function signature: " << name
                     << signature->toString();
        continue;
      }

      if (!signature->variables().empty()) {
        bool skip = false;
        std::unordered_set<std::string> typeVariables;
        for (auto& [name, variable] : signature->variables()) {
          if (variable.isIntegerParameter()) {
            LOG(WARNING) << "Skipping generic function signature: " << name
                         << signature->toString();
            skip = true;
            break;
          }

          typeVariables.insert(name);
        }
        if (skip) {
          continue;
        }

        signatureTemplates_.push_back(
            {name, signature.get(), std::move(typeVariables)});
      } else {
        CallableSignature callable{
            .name = name,
            .args = {},
            .returnType = SignatureBinder::tryResolveType(
                signature->returnType(), {}, {}),
            .constantArgs = {}};
        VELOX_CHECK_NOT_NULL(callable.returnType);

        // Process each argument and figure out its type.
        for (const auto& arg : signature->argumentTypes()) {
          auto resolvedType = SignatureBinder::tryResolveType(arg, {}, {});
          VELOX_CHECK_NOT_NULL(resolvedType);

          // SignatureBinder::tryResolveType produces ROW types with empty field
          // names. These won't work with TableScan.
          if (resolvedType->isRow()) {
            std::vector<std::string> names;
            for (auto i = 0; i < resolvedType->size(); ++i) {
              names.push_back(fmt::format("field{}", i));
            }

            std::vector<TypePtr> types = resolvedType->asRow().children();

            resolvedType = ROW(std::move(names), std::move(types));
          }

          callable.args.emplace_back(resolvedType);
        }

        signatures_.emplace_back(callable);
      }

      ++numSupportedSignatures;
      hasSupportedSignature = true;
    }
    if (hasSupportedSignature) {
      ++numSupportedFunctions;
    }
  }

  printStats(
      numFunctions,
      numSignatures,
      numSupportedFunctions,
      numSupportedSignatures);

  sortCallableSignatures(signatures_);
  sortSignatureTemplates(signatureTemplates_);
}

template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

std::string makeFunctionCall(
    const std::string& name,
    const std::vector<std::string>& argNames) {
  std::ostringstream call;
  call << name << "(";
  for (auto i = 0; i < argNames.size(); ++i) {
    if (i > 0) {
      call << ", ";
    }
    call << argNames[i];
  }
  call << ")";

  return call.str();
}

std::vector<std::string> makeNames(size_t n) {
  std::vector<std::string> names;
  for (auto i = 0; i < n; ++i) {
    names.push_back(fmt::format("c{}", i));
  }
  return names;
}

folly::dynamic serialize(
    const AggregationFuzzer::PlanWithSplits& planWithSplits,
    const std::string& dirPath,
    std::unordered_map<std::string, std::string>& filePaths) {
  folly::dynamic obj = folly::dynamic::object();
  obj["plan"] = planWithSplits.plan->serialize();
  if (planWithSplits.splits.empty()) {
    return obj;
  }

  folly::dynamic jsonSplits = folly::dynamic::array();
  jsonSplits.reserve(planWithSplits.splits.size());
  for (const auto& split : planWithSplits.splits) {
    const auto filePath =
        std::dynamic_pointer_cast<connector::hive::HiveConnectorSplit>(
            split.connectorSplit)
            ->filePath;
    if (filePaths.count(filePath) == 0) {
      const auto newFilePath = fmt::format("{}/{}", dirPath, filePaths.size());
      fs::copy(filePath, newFilePath);
      filePaths.insert({filePath, newFilePath});
    }
    jsonSplits.push_back(filePaths.at(filePath));
  }
  obj["splits"] = jsonSplits;
  return obj;
}

void persistReproInfo(
    const std::vector<AggregationFuzzer::PlanWithSplits>& plans,
    const std::string& basePath) {
  if (!common::generateFileDirectory(basePath.c_str())) {
    return;
  }

  // Create a new directory
  const auto dirPathOptional =
      common::generateTempFolderPath(basePath.c_str(), "aggregationVerifier");
  if (!dirPathOptional.has_value()) {
    LOG(ERROR)
        << "Failed to create directory for persisting plans using base path: "
        << basePath;
    return;
  }

  const auto dirPath = dirPathOptional.value();

  // Save plans and splits.
  const std::string planPath = fmt::format("{}/{}", dirPath, kPlanNodeFileName);
  std::unordered_map<std::string, std::string> filePaths;
  try {
    folly::dynamic array = folly::dynamic::array();
    array.reserve(plans.size());
    for (auto planWithSplits : plans) {
      array.push_back(serialize(planWithSplits, dirPath, filePaths));
    }
    auto planJson = folly::toJson(array);
    saveStringToFile(planJson, planPath.c_str());
    LOG(INFO) << "Persisted aggregation plans to " << planPath;
  } catch (std::exception& e) {
    LOG(ERROR) << "Failed to store aggregation plans to " << planPath << ": "
               << e.what();
  }
}

CallableSignature AggregationFuzzer::pickSignature() {
  size_t idx = boost::random::uniform_int_distribution<uint32_t>(
      0, signatures_.size() + signatureTemplates_.size() - 1)(rng_);
  CallableSignature signature;
  if (idx < signatures_.size()) {
    signature = signatures_[idx];
  } else {
    const auto& signatureTemplate =
        signatureTemplates_[idx - signatures_.size()];
    signature.name = signatureTemplate.name;
    velox::test::ArgumentTypeFuzzer typeFuzzer(
        *signatureTemplate.signature, rng_);
    VELOX_CHECK(typeFuzzer.fuzzArgumentTypes(FLAGS_max_num_varargs));
    signature.args = typeFuzzer.argumentTypes();
  }

  return signature;
}

std::vector<std::string> AggregationFuzzer::generateKeys(
    const std::string& prefix,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types) {
  static const std::vector<TypePtr> kNonFloatingPointTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
  };

  auto numKeys = boost::random::uniform_int_distribution<uint32_t>(1, 5)(rng_);
  std::vector<std::string> keys;
  for (auto i = 0; i < numKeys; ++i) {
    keys.push_back(fmt::format("{}{}", prefix, i));

    // Pick random, possibly complex, type.
    types.push_back(vectorFuzzer_.randType(kNonFloatingPointTypes, 2));
    names.push_back(keys.back());
  }
  return keys;
}

std::vector<RowVectorPtr> AggregationFuzzer::generateInputData(
    std::vector<std::string> names,
    std::vector<TypePtr> types) {
  auto inputType = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> input;
  for (auto i = 0; i < FLAGS_num_batches; ++i) {
    input.push_back(vectorFuzzer_.fuzzInputRow(inputType));
  }
  return input;
}

std::vector<RowVectorPtr> AggregationFuzzer::generateInputDataWithRowNumber(
    std::vector<std::string> names,
    std::vector<TypePtr> types) {
  names.push_back("row_number");
  types.push_back(BIGINT());

  std::vector<RowVectorPtr> input;
  auto size = vectorFuzzer_.getOptions().vectorSize;
  velox::test::VectorMaker vectorMaker{pool_.get()};
  int64_t rowNumber = 0;
  for (auto j = 0; j < FLAGS_num_batches; ++j) {
    std::vector<VectorPtr> children;
    for (auto i = 0; i < types.size() - 1; ++i) {
      children.push_back(vectorFuzzer_.fuzz(types[i], size));
    }
    children.push_back(vectorMaker.flatVector<int64_t>(
        size, [&](auto /*row*/) { return rowNumber++; }));
    input.push_back(vectorMaker.rowVector(names, children));
  }
  return input;
}

// static
exec::Split AggregationFuzzer::makeSplit(const std::string& filePath) {
  return exec::Split{std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, filePath, dwio::common::FileFormat::DWRF)};
}

AggregationFuzzer::PlanWithSplits AggregationFuzzer::deserialize(
    const folly::dynamic& obj) {
  auto plan = velox::ISerializable::deserialize<core::PlanNode>(
      obj["plan"], pool_.get());

  std::vector<exec::Split> splits;
  if (obj.count("splits") > 0) {
    auto paths =
        ISerializable::deserialize<std::vector<std::string>>(obj["splits"]);
    for (const auto& path : paths) {
      splits.push_back(makeSplit(path));
    }
  }

  return PlanWithSplits{plan, splits};
}

void AggregationFuzzer::go(const std::string& planPath) {
  Type::registerSerDe();
  connector::hive::HiveTableHandle::registerSerDe();
  connector::hive::LocationHandle::registerSerDe();
  connector::hive::HiveColumnHandle::registerSerDe();
  connector::hive::HiveInsertTableHandle::registerSerDe();
  core::ITypedExpr::registerSerDe();
  core::PlanNode::registerSerDe();
  registerPartitionFunctionSerDe();

  LOG(INFO) << "Attempting to use serialized plan at: " << planPath;
  auto planString = restoreStringFromFile(planPath.c_str());
  auto parsedPlans = folly::parseJson(planString);
  std::vector<PlanWithSplits> plans(parsedPlans.size());
  std::transform(
      parsedPlans.begin(),
      parsedPlans.end(),
      plans.begin(),
      [&](const folly::dynamic& obj) { return deserialize(obj); });

  verifyAggregation(plans);
}

void AggregationFuzzer::go() {
  VELOX_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.")

  auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";

    // 10% of times test distinct aggregation.
    if (vectorFuzzer_.coinToss(0.1)) {
      ++stats_.numDistinct;

      std::vector<TypePtr> types;
      std::vector<std::string> names;

      auto groupingKeys = generateKeys("g", names, types);
      auto input = generateInputData(names, types);

      verifyAggregation(groupingKeys, {}, {}, input, false, {});
    } else {
      // Pick a random signature.
      CallableSignature signature = pickSignature();
      stats_.functionNames.insert(signature.name);

      const bool customVerification =
          customVerificationFunctions_.count(signature.name) != 0;

      std::vector<TypePtr> argTypes = signature.args;
      std::vector<std::string> argNames = makeNames(argTypes.size());
      auto call = makeFunctionCall(signature.name, argNames);

      // 10% of times test window operator.
      if (vectorFuzzer_.coinToss(0.1)) {
        ++stats_.numWindow;

        auto partitionKeys = generateKeys("p", argNames, argTypes);
        auto sortingKeys = generateKeys("s", argNames, argTypes);
        auto input = generateInputDataWithRowNumber(argNames, argTypes);

        verifyWindow(
            partitionKeys,
            sortingKeys,
            {call},
            input,
            customVerification,
            FLAGS_enable_window_reference_verification);
      } else {
        // 20% of times use mask.
        std::vector<std::string> masks;
        if (vectorFuzzer_.coinToss(0.2)) {
          ++stats_.numMask;

          masks.push_back("m0");
          argTypes.push_back(BOOLEAN());
          argNames.push_back(masks.back());
        }

        // 10% of times use global aggregation (no grouping keys).
        std::vector<std::string> groupingKeys;
        if (vectorFuzzer_.coinToss(0.1)) {
          ++stats_.numGlobal;
        } else {
          ++stats_.numGroupBy;
          groupingKeys = generateKeys("g", argNames, argTypes);
        }

        std::vector<std::string> projections;
        if (customVerification) {
          // Add optional projection on the original result to make it order
          // independent for comparison.
          auto mitigation = customVerificationFunctions_.at(signature.name);
          if (!mitigation.empty()) {
            projections = groupingKeys;
            projections.push_back(fmt::format(fmt::runtime(mitigation), "a0"));
          }
        }

        auto input = generateInputData(argNames, argTypes);

        verifyAggregation(
            groupingKeys,
            {call},
            masks,
            input,
            customVerification,
            projections);
      }
    }
    LOG(INFO) << "==============================> Done with iteration "
              << iteration;

    if (persistAndRunOnce_) {
      LOG(WARNING)
          << "Iteration succeeded with --persist_and_run_once flag enabled "
             "(expecting crash failure)";
      exit(0);
    }

    reSeed();
    ++iteration;
  }

  stats_.print(iteration);
}

velox::test::ResultOrError AggregationFuzzer::execute(
    const core::PlanNodePtr& plan,
    const std::vector<exec::Split>& splits,
    bool injectSpill,
    bool abandonPartial) {
  LOG(INFO) << "Executing query plan: " << std::endl
            << plan->toString(true, true);

  velox::test::ResultOrError resultOrError;
  try {
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    AssertQueryBuilder builder(plan);

    builder.configs(queryConfigs_);

    if (injectSpill) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      builder.spillDirectory(spillDirectory->path)
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kAggregationSpillEnabled, "true")
          .config(core::QueryConfig::kTestingSpillPct, "100");
    }

    if (abandonPartial) {
      builder.config(core::QueryConfig::kAbandonPartialAggregationMinRows, "1")
          .config(core::QueryConfig::kAbandonPartialAggregationMinPct, "0")
          .config(core::QueryConfig::kMaxPartialAggregationMemory, "0")
          .config(core::QueryConfig::kMaxExtendedPartialAggregationMemory, "0");
    }

    if (!splits.empty()) {
      builder.splits(splits);
    }

    resultOrError.result = builder.maxDrivers(2).copyResults(pool_.get());
    LOG(INFO) << resultOrError.result->toString();
  } catch (VeloxUserError& e) {
    // NOTE: velox user exception is accepted as it is caused by the invalid
    // fuzzer test inputs.
    resultOrError.exceptionPtr = std::current_exception();
  }

  return resultOrError;
}

std::optional<MaterializedRowMultiset>
AggregationFuzzer::computeReferenceResults(
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& input) {
  if (auto sql = referenceQueryRunner_->toSql(plan)) {
    try {
      return referenceQueryRunner_->execute(
          sql.value(), input, plan->outputType());
    } catch (std::exception& e) {
      ++stats_.numReferenceQueryFailed;
      LOG(WARNING) << "Couldn't get results from reference DB";
      return std::nullopt;
    }
  } else {
    ++stats_.numVerificationNotSupported;
  }

  return std::nullopt;
}

void makeAlternativePlansWithValues(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<std::string>& projections,
    const std::vector<RowVectorPtr>& inputVectors,
    std::vector<core::PlanNodePtr>& plans) {
  // Partial -> final aggregation plan.
  plans.push_back(PlanBuilder()
                      .values(inputVectors)
                      .partialAggregation(groupingKeys, aggregates, masks)
                      .finalAggregation()
                      .optionalProject(projections)
                      .planNode());

  // Partial -> intermediate -> final aggregation plan.
  plans.push_back(PlanBuilder()
                      .values(inputVectors)
                      .partialAggregation(groupingKeys, aggregates, masks)
                      .intermediateAggregation()
                      .finalAggregation()
                      .optionalProject(projections)
                      .planNode());

  // Partial -> local exchange -> final aggregation plan.
  auto numSources = std::min<size_t>(4, inputVectors.size());
  std::vector<std::vector<RowVectorPtr>> sourceInputs(numSources);
  for (auto i = 0; i < inputVectors.size(); ++i) {
    sourceInputs[i % numSources].push_back(inputVectors[i]);
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<core::PlanNodePtr> sources;
  for (const auto& sourceInput : sourceInputs) {
    sources.push_back(PlanBuilder(planNodeIdGenerator)
                          .values({sourceInput})
                          .partialAggregation(groupingKeys, aggregates, masks)
                          .planNode());
  }
  plans.push_back(PlanBuilder(planNodeIdGenerator)
                      .localPartition(groupingKeys, sources)
                      .finalAggregation()
                      .optionalProject(projections)
                      .planNode());
}

void makeAlternativePlansWithTableScan(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<std::string>& projections,
    const RowTypePtr& inputRowType,
    std::vector<core::PlanNodePtr>& plans) {
  // Partial -> final aggregation plan.
  plans.push_back(PlanBuilder()
                      .tableScan(inputRowType)
                      .partialAggregation(groupingKeys, aggregates, masks)
                      .localPartition(groupingKeys)
                      .finalAggregation()
                      .optionalProject(projections)
                      .planNode());

  // Partial -> intermediate -> final aggregation plan.
  plans.push_back(PlanBuilder()
                      .tableScan(inputRowType)
                      .partialAggregation(groupingKeys, aggregates, masks)
                      .localPartition(groupingKeys)
                      .intermediateAggregation()
                      .finalAggregation()
                      .optionalProject(projections)
                      .planNode());
}

void makeStreamingPlansWithValues(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<std::string>& projections,
    const std::vector<RowVectorPtr>& inputVectors,
    std::vector<core::PlanNodePtr>& plans) {
  // Single aggregation.
  plans.push_back(PlanBuilder()
                      .values(inputVectors)
                      .orderBy(groupingKeys, false)
                      .streamingAggregation(
                          groupingKeys,
                          aggregates,
                          masks,
                          core::AggregationNode::Step::kSingle,
                          false)
                      .optionalProject(projections)
                      .planNode());

  // Partial -> final aggregation plan.
  plans.push_back(
      PlanBuilder()
          .values(inputVectors)
          .orderBy(groupingKeys, false)
          .partialStreamingAggregation(groupingKeys, aggregates, masks)
          .finalAggregation()
          .optionalProject(projections)
          .planNode());

  // Partial -> intermediate -> final aggregation plan.
  plans.push_back(
      PlanBuilder()
          .values(inputVectors)
          .orderBy(groupingKeys, false)
          .partialStreamingAggregation(groupingKeys, aggregates, masks)
          .intermediateAggregation()
          .finalAggregation()
          .optionalProject(projections)
          .planNode());

  // Partial -> local merge -> final aggregation plan.
  auto numSources = std::min<size_t>(4, inputVectors.size());
  std::vector<std::vector<RowVectorPtr>> sourceInputs(numSources);
  for (auto i = 0; i < inputVectors.size(); ++i) {
    sourceInputs[i % numSources].push_back(inputVectors[i]);
  }

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<core::PlanNodePtr> sources;
  for (const auto& sourceInput : sourceInputs) {
    sources.push_back(
        PlanBuilder(planNodeIdGenerator)
            .values({sourceInput})
            .orderBy(groupingKeys, false)
            .partialStreamingAggregation(groupingKeys, aggregates, masks)
            .planNode());
  }
  plans.push_back(PlanBuilder(planNodeIdGenerator)
                      .localMerge(groupingKeys, sources)
                      .finalAggregation()
                      .optionalProject(projections)
                      .planNode());
}

void makeStreamingPlansWithTableScan(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<std::string>& projections,
    const RowTypePtr& inputRowType,
    std::vector<core::PlanNodePtr>& plans) {
  // Single aggregation.
  plans.push_back(PlanBuilder()
                      .tableScan(inputRowType)
                      .orderBy(groupingKeys, false)
                      .streamingAggregation(
                          groupingKeys,
                          aggregates,
                          masks,
                          core::AggregationNode::Step::kSingle,
                          false)
                      .optionalProject(projections)
                      .planNode());

  // Partial -> final aggregation plan.
  plans.push_back(
      PlanBuilder()
          .tableScan(inputRowType)
          .orderBy(groupingKeys, false)
          .partialStreamingAggregation(groupingKeys, aggregates, masks)
          .finalAggregation()
          .optionalProject(projections)
          .planNode());

  // Partial -> intermediate -> final aggregation plan.
  plans.push_back(
      PlanBuilder()
          .tableScan(inputRowType)
          .orderBy(groupingKeys, false)
          .partialStreamingAggregation(groupingKeys, aggregates, masks)
          .intermediateAggregation()
          .finalAggregation()
          .optionalProject(projections)
          .planNode());

  // Partial -> local merge -> final aggregation plan.
  plans.push_back(
      PlanBuilder()
          .tableScan(inputRowType)
          .orderBy(groupingKeys, true)
          .partialStreamingAggregation(groupingKeys, aggregates, masks)
          .localMerge(groupingKeys)
          .finalAggregation()
          .optionalProject(projections)
          .planNode());
}

void AggregationFuzzer::testPlan(
    const PlanWithSplits& planWithSplits,
    bool injectSpill,
    bool abandonPartial,
    bool verifyResults,
    const velox::test::ResultOrError& expected) {
  auto actual = execute(
      planWithSplits.plan, planWithSplits.splits, injectSpill, abandonPartial);

  // Compare results or exceptions (if any). Fail is anything is different.
  if (expected.exceptionPtr || actual.exceptionPtr) {
    // Throws in case exceptions are not compatible.
    velox::test::compareExceptions(expected.exceptionPtr, actual.exceptionPtr);
  } else if (verifyResults) {
    VELOX_CHECK(
        assertEqualResults({expected.result}, {actual.result}),
        "Logically equivalent plans produced different results");
  } else {
    VELOX_CHECK_EQ(
        expected.result->size(),
        actual.result->size(),
        "Logically equivalent plans produced different number of rows");
  }
}

void AggregationFuzzer::verifyWindow(
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<RowVectorPtr>& input,
    bool customVerification,
    bool enableWindowVerification) {
  std::stringstream frame;
  if (!partitionKeys.empty()) {
    frame << "partition by " << folly::join(", ", partitionKeys);
  }
  if (!sortingKeys.empty()) {
    frame << " order by " << folly::join(", ", sortingKeys);
  }

  auto plan =
      PlanBuilder()
          .values(input)
          .window({fmt::format("{} over ({})", aggregates[0], frame.str())})
          .planNode();
  if (persistAndRunOnce_) {
    persistReproInfo({{plan, {}}}, reproPersistPath_);
  }
  try {
    auto resultOrError = execute(plan);
    if (resultOrError.exceptionPtr) {
      ++stats_.numFailed;
    }

    if (!customVerification && enableWindowVerification) {
      if (resultOrError.result) {
        if (auto expectedResult = computeReferenceResults(plan, input)) {
          ++stats_.numVerified;
          VELOX_CHECK(
              assertEqualResults(
                  expectedResult.value(),
                  plan->outputType(),
                  {resultOrError.result}),
              "Velox and reference DB results don't match");
        }
      }
    } else {
      ++stats_.numVerificationSkipped;
    }
  } catch (...) {
    if (!reproPersistPath_.empty()) {
      persistReproInfo({{plan, {}}}, reproPersistPath_);
    }
    throw;
  }
}

namespace {
void writeToFile(
    const std::string& path,
    const VectorPtr& vector,
    memory::MemoryPool* pool) {
  dwrf::WriterOptions options;
  options.schema = vector->type();
  options.memoryPool = pool;
  auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink =
      std::make_unique<dwio::common::WriteFileSink>(std::move(writeFile), path);
  dwrf::Writer writer(std::move(sink), options);
  writer.write(vector);
  writer.close();
}

bool isTableScanSupported(const TypePtr& type) {
  if (type->kind() == TypeKind::ROW && type->size() == 0) {
    return false;
  }
  if (type->kind() == TypeKind::UNKNOWN) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isTableScanSupported(type->childAt(i))) {
      return false;
    }
  }

  return true;
}
} // namespace

void AggregationFuzzer::verifyAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<RowVectorPtr>& input,
    bool customVerification,
    const std::vector<std::string>& projections) {
  auto firstPlan = PlanBuilder()
                       .values(input)
                       .singleAggregation(groupingKeys, aggregates, masks)
                       .optionalProject(projections)
                       .planNode();

  // Create all the plans upfront.
  std::vector<PlanWithSplits> plans;
  plans.push_back({firstPlan, {}});

  auto directory = exec::test::TempDirectoryPath::create();

  // Alternate between using Values and TableScan node.

  // Sometimes we generate zero-column input of type ROW({}) or a column of type
  // UNKNOWN(). Such data cannot be written to a file and therefore cannot
  // be tested with TableScan.
  if (isTableScanSupported(input[0]->type()) && vectorFuzzer_.coinToss(0.5)) {
    std::vector<exec::Split> splits;
    auto writerPool = rootPool_->addAggregateChild("writer");
    for (auto i = 0; i < input.size(); ++i) {
      const std::string filePath = fmt::format("{}/{}", directory->path, i);
      writeToFile(filePath, input[i], writerPool.get());
      splits.push_back(makeSplit(filePath));
    }

    const auto inputRowType = asRowType(input[0]->type());

    std::vector<core::PlanNodePtr> tableScanPlans;
    makeAlternativePlansWithTableScan(
        groupingKeys,
        aggregates,
        masks,
        projections,
        inputRowType,
        tableScanPlans);

    if (!groupingKeys.empty()) {
      // Use OrderBy + StreamingAggregation on original input.
      makeStreamingPlansWithTableScan(
          groupingKeys,
          aggregates,
          masks,
          projections,
          inputRowType,
          tableScanPlans);
    }

    for (const auto& plan : tableScanPlans) {
      plans.push_back({plan, splits});
    }
  } else {
    std::vector<core::PlanNodePtr> valuesPlans;
    makeAlternativePlansWithValues(
        groupingKeys, aggregates, masks, projections, input, valuesPlans);

    // Evaluate same plans on flat inputs.
    std::vector<RowVectorPtr> flatInput;
    for (const auto& vector : input) {
      auto flat = BaseVector::create<RowVector>(
          vector->type(), vector->size(), vector->pool());
      flat->copy(vector.get(), 0, 0, vector->size());
      flatInput.push_back(flat);
    }

    makeAlternativePlansWithValues(
        groupingKeys, aggregates, masks, projections, flatInput, valuesPlans);

    if (!groupingKeys.empty()) {
      // Use OrderBy + StreamingAggregation on original input.
      makeStreamingPlansWithValues(
          groupingKeys, aggregates, masks, projections, input, valuesPlans);

      // Use OrderBy + StreamingAggregation on flattened input.
      makeStreamingPlansWithValues(
          groupingKeys, aggregates, masks, projections, flatInput, valuesPlans);
    }

    for (const auto& plan : valuesPlans) {
      plans.push_back({plan, {}});
    }
  }

  if (persistAndRunOnce_) {
    persistReproInfo(plans, reproPersistPath_);
  }

  try {
    auto resultOrError = execute(firstPlan);
    if (resultOrError.exceptionPtr) {
      ++stats_.numFailed;
    }

    const bool verifyResults = !customVerification || !projections.empty();

    std::optional<MaterializedRowMultiset> expectedResult;
    if (verifyResults) {
      expectedResult = computeReferenceResults(firstPlan, input);
    } else {
      ++stats_.numVerificationSkipped;
    }

    if (expectedResult && resultOrError.result) {
      ++stats_.numVerified;
      VELOX_CHECK(
          assertEqualResults(
              expectedResult.value(),
              firstPlan->outputType(),
              {resultOrError.result}),
          "Velox and DuckDB results don't match");
    }

    testPlans(plans, verifyResults, resultOrError);

  } catch (...) {
    if (!reproPersistPath_.empty()) {
      persistReproInfo(plans, reproPersistPath_);
    }
    throw;
  }
}

// verifyAggregation(std::vector<core::PlanNodePtr> plans) is tied to plan
// created by previous verifyAggregation function. Changes in nodes there will
// require corresponding changes here.
void AggregationFuzzer::verifyAggregation(
    const std::vector<PlanWithSplits>& plans) {
  VELOX_CHECK_GT(plans.size(), 0);
  const auto& plan = plans.front().plan;
  auto node = dynamic_cast<const core::AggregationNode*>(plan.get());
  auto projectionNode = dynamic_cast<const core::ProjectNode*>(plan.get());
  VELOX_CHECK(node || projectionNode);

  if (!node) {
    VELOX_CHECK_GT(projectionNode->sources().size(), 0);
    node = dynamic_cast<const core::AggregationNode*>(
        projectionNode->sources()[0].get());
    VELOX_CHECK_NOT_NULL(node, "Unable to create aggregation node!");
  }

  // Get groupingKeys.
  auto groupingKeys = node->groupingKeys();
  std::vector<std::string> groupingKeyNames;
  groupingKeyNames.reserve(groupingKeys.size());

  for (const auto& key : groupingKeys) {
    groupingKeyNames.push_back(key->name());
  }

  // Get masks.
  std::vector<std::string> maskNames;
  maskNames.reserve(node->aggregates().size());

  for (const auto& aggregate : node->aggregates()) {
    if (aggregate.mask) {
      maskNames.push_back(aggregate.mask->name());
    }
  }

  // Get projections.
  auto projections =
      projectionNode ? projectionNode->names() : std::vector<std::string>{};

  // Get inputs.
  std::vector<RowVectorPtr> input;
  input.reserve(node->sources().size());

  for (auto source : node->sources()) {
    auto valueNode = dynamic_cast<const core::ValuesNode*>(source.get());
    VELOX_CHECK_NOT_NULL(valueNode);
    auto values = valueNode->values();
    input.insert(input.end(), values.begin(), values.end());
  }

  auto resultOrError = execute(plan);
  if (resultOrError.exceptionPtr) {
    ++stats_.numFailed;
  }

  // Get aggregations and determine if order dependent.
  std::vector<std::string> aggregateStrings;
  aggregateStrings.reserve(node->aggregates().size());

  bool customVerification = false;
  for (auto aggregate : node->aggregates()) {
    aggregateStrings.push_back(aggregate.call->toString());
    customVerification |=
        customVerificationFunctions_.count(aggregate.call->name()) != 0;
  }

  const bool verifyResults = !customVerification || !projections.empty();

  std::optional<MaterializedRowMultiset> expectedResult;
  if (verifyResults) {
    expectedResult = computeReferenceResults(plan, input);
  }

  if (expectedResult && resultOrError.result) {
    ++stats_.numVerified;
    VELOX_CHECK(
        assertEqualResults(
            expectedResult.value(), plan->outputType(), {resultOrError.result}),
        "Velox and DuckDB results don't match");
  }

  // Test all plans.
  testPlans(plans, verifyResults, resultOrError);
}

void AggregationFuzzer::Stats::print(size_t numIterations) const {
  LOG(INFO) << "Total functions tested: " << functionNames.size();
  LOG(INFO) << "Total masked aggregations: "
            << printStat(numMask, numIterations);
  LOG(INFO) << "Total global aggregations: "
            << printStat(numGlobal, numIterations);
  LOG(INFO) << "Total group-by aggregations: "
            << printStat(numGroupBy, numIterations);
  LOG(INFO) << "Total distinct aggregations: "
            << printStat(numDistinct, numIterations);
  LOG(INFO) << "Total window expressions: "
            << printStat(numWindow, numIterations);
  LOG(INFO) << "Total aggregations verified against reference DB: "
            << printStat(numVerified, numIterations);
  LOG(INFO)
      << "Total aggregations not verified (non-deterministic function / not supported by reference DB / reference DB failed): "
      << printStat(numVerificationSkipped, numIterations) << " / "
      << printStat(numVerificationNotSupported, numIterations) << " / "
      << printStat(numReferenceQueryFailed, numIterations);
  LOG(INFO) << "Total failed aggregations: "
            << printStat(numFailed, numIterations);
}

} // namespace
} // namespace facebook::velox::exec::test
