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
#include "velox/exec/tests/AggregationFuzzer.h"
#include <boost/random/uniform_int_distribution.hpp>
#include "velox/common/base/Fs.h"

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
          customVerificationFunctions);

  void go();
  void go(const std::string& planPath);

 private:
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

    // Number of interations using window expressions.
    size_t numWindow{0};

    // Number of iterations where results were verified against DuckDB,
    size_t numDuckVerified{0};

    // Number of iterations where aggregation failed.
    size_t numFailed{0};

    void print(size_t numIterations) const;
  };

  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
    opts.nullRatio = FLAGS_null_ratio;
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
      bool customVerification);

  std::optional<MaterializedRowMultiset> computeDuckWindow(
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<RowVectorPtr>& input,
      const core::PlanNodePtr& plan);

  void verifyAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      const std::vector<RowVectorPtr>& input,
      bool customVerification,
      const std::vector<std::string>& projections);

  void verifyAggregation(std::vector<core::PlanNodePtr> plans);

  std::optional<MaterializedRowMultiset> computeDuckAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      const std::vector<std::string>& projections,
      const std::vector<RowVectorPtr>& input,
      const core::PlanNodePtr& plan);

  velox::test::ResultOrError execute(
      const core::PlanNodePtr& plan,
      bool injectSpill);

  void testPlans(
      const std::vector<core::PlanNodePtr>& plans,
      bool verifyResults,
      const velox::test::ResultOrError& expected) {
    for (auto i = 0; i < plans.size(); ++i) {
      LOG(INFO) << "Testing plan #" << i;
      testPlan(plans[i], false /*injectSpill*/, verifyResults, expected);

      LOG(INFO) << "Testing plan #" << i << " with spilling";
      testPlan(plans[i], true /*injectSpill*/, verifyResults, expected);
    }
  }

  void testPlan(
      const core::PlanNodePtr& plan,
      bool injectSpill,
      bool verifyResults,
      const velox::test::ResultOrError& expected);

  const std::unordered_map<std::string, std::string>
      customVerificationFunctions_;
  const bool persistAndRunOnce_;
  const std::string reproPersistPath_;

  std::unordered_set<std::string> duckFunctionNames_;

  std::vector<CallableSignature> signatures_;
  std::vector<SignatureTemplate> signatureTemplates_;

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> pool_{memory::addDefaultLeafMemoryPool()};
  VectorFuzzer vectorFuzzer_;

  Stats stats_;
};
} // namespace

void aggregateFuzzer(
    AggregateFunctionSignatureMap signatureMap,
    size_t seed,
    const std::unordered_map<std::string, std::string>&
        customVerificationFunctions,
    const std::optional<std::string>& planPath) {
  auto aggregationFuzzer = AggregationFuzzer(
      std::move(signatureMap), seed, customVerificationFunctions);
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

std::unordered_set<std::string> getDuckFunctions() {
  std::string sql =
      "SELECT distinct on(function_name) function_name "
      "FROM duckdb_functions() "
      "WHERE function_type = 'aggregate'";

  DuckDbQueryRunner queryRunner;
  auto result = queryRunner.executeOrdered(sql, ROW({VARCHAR()}));

  std::unordered_set<std::string> names;
  for (const auto& row : result) {
    names.insert(row[0].value<std::string>());
  }

  return names;
}

AggregationFuzzer::AggregationFuzzer(
    AggregateFunctionSignatureMap signatureMap,
    size_t initialSeed,
    const std::unordered_map<std::string, std::string>&
        customVerificationFunctions)
    : customVerificationFunctions_{customVerificationFunctions},
      persistAndRunOnce_{FLAGS_persist_and_run_once},
      reproPersistPath_{FLAGS_repro_persist_path},
      vectorFuzzer_{getFuzzerOptions(), pool_.get()} {
  seed(initialSeed);
  VELOX_CHECK(!signatureMap.empty(), "No function signatures available.");

  if (persistAndRunOnce_ && reproPersistPath_.empty()) {
    std::cout
        << "--repro_persist_path must be specified if --persist_and_run_once is specified"
        << std::endl;
    exit(1);
  }

  duckFunctionNames_ = getDuckFunctions();

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

void persistReproInfo(
    const std::vector<core::PlanNodePtr>& plans,
    const std::string& basePath) {
  if (!common::generateFileDirectory(basePath.c_str())) {
    return;
  }

  // Create a new directory
  auto dirPath =
      common::generateTempFolderPath(basePath.c_str(), "aggregationVerifier");
  if (!dirPath.has_value()) {
    LOG(INFO) << "Failed to create directory for persisting plans.";
    return;
  }

  // Save plans.
  std::string planPath;
  planPath = fmt::format("{}/{}", dirPath->c_str(), kPlanNodeFileName);
  try {
    folly::dynamic array = folly::dynamic::array();
    array.reserve(plans.size());
    for (auto plan : plans) {
      array.push_back(plan->serialize());
    }
    auto planJson = folly::toJson(array);
    saveStringToFile(planJson, planPath.c_str());
  } catch (std::exception& e) {
    planPath = e.what();
  }

  LOG(INFO) << "Persisted aggregation plans @ : " << planPath;
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
  auto numKeys = boost::random::uniform_int_distribution<uint32_t>(1, 5)(rng_);
  std::vector<std::string> keys;
  for (auto i = 0; i < numKeys; ++i) {
    keys.push_back(fmt::format("{}{}", prefix, i));

    // Pick random scalar type.
    types.push_back(vectorFuzzer_.randScalarNonFloatingPointType());
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

void AggregationFuzzer::go(const std::string& planPath) {
  Type::registerSerDe();
  core::ITypedExpr::registerSerDe();
  core::PlanNode::registerSerDe();
  registerPartitionFunctionSerDe();

  LOG(INFO) << "Attempting to use serialized plan at: " << planPath;
  auto planString = restoreStringFromFile(planPath.c_str());
  auto parsedPlans = folly::parseJson(planString);
  std::vector<core::PlanNodePtr> plans(parsedPlans.size());
  std::transform(
      parsedPlans.begin(),
      parsedPlans.end(),
      plans.begin(),
      [&](const folly::dynamic& plan) {
        return velox::ISerializable::deserialize<core::PlanNode>(
            plan, pool_.get());
      });

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
    if (vectorFuzzer_.coinToss(0.0)) {
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
            partitionKeys, sortingKeys, {call}, input, customVerification);
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
    bool injectSpill) {
  LOG(INFO) << "Executing query plan: " << std::endl
            << plan->toString(true, true);

  velox::test::ResultOrError resultOrError;
  try {
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    AssertQueryBuilder builder(plan);
    if (injectSpill) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      builder.spillDirectory(spillDirectory->path)
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kAggregationSpillEnabled, "true")
          .config(core::QueryConfig::kTestingSpillPct, "100");
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

// Generate SELECT <keys>, <aggregates> FROM tmp GROUP BY <keys>.
std::string makeDuckAggregationSql(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<std::string>& projections) {
  std::stringstream sql;
  sql << "SELECT " << folly::join(", ", groupingKeys);

  if (!groupingKeys.empty()) {
    sql << ", ";
  }

  for (auto i = 0; i < aggregates.size(); ++i) {
    if (i > 0) {
      sql << ", ";
    }
    sql << aggregates[i];
    if (masks.size() > i && !masks[i].empty()) {
      sql << " filter (where " << masks[i] << ")";
    }
    sql << " as a" << i;
  }

  sql << " FROM tmp";

  if (!groupingKeys.empty()) {
    sql << " GROUP BY " << folly::join(", ", groupingKeys);
  }

  if (!projections.empty()) {
    return fmt::format(
        "SELECT {} FROM ({})", folly::join(", ", projections), sql.str());
  }

  return sql.str();
}

bool isDuckSupported(const TypePtr& type) {
  for (auto i = 0; i < type->size(); ++i) {
    // DuckDB doesn't support nanosecond precision for timestamps.
    if (type->childAt(i)->isTimestamp()) {
      return false;
    }
  }

  return true;
}

std::optional<MaterializedRowMultiset> computeDuckResults(
    const std ::string& sql,
    const std::vector<RowVectorPtr>& input,
    const RowTypePtr& resultType) {
  try {
    DuckDbQueryRunner queryRunner;
    queryRunner.createTable("tmp", input);
    return queryRunner.execute(sql, resultType);
  } catch (std::exception& e) {
    LOG(WARNING) << "Couldn't get results from DuckDB";
    return std::nullopt;
  }
}

std::optional<MaterializedRowMultiset>
AggregationFuzzer::computeDuckAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<std::string>& projections,
    const std::vector<RowVectorPtr>& input,
    const core::PlanNodePtr& plan) {
  // Check if DuckDB supports specified aggregate functions.
  auto aggregationNode = dynamic_cast<const core::AggregationNode*>(
      projections.empty() ? plan.get() : plan->sources()[0].get());
  VELOX_CHECK_NOT_NULL(aggregationNode);
  for (const auto& agg : aggregationNode->aggregates()) {
    if (duckFunctionNames_.count(agg->name()) == 0) {
      return std::nullopt;
    }
  }

  const auto& outputType = plan->outputType();

  if (!isDuckSupported(input[0]->type()) || !isDuckSupported(outputType)) {
    return std::nullopt;
  }

  return computeDuckResults(
      makeDuckAggregationSql(groupingKeys, aggregates, masks, projections),
      input,
      outputType);
}

void makeAlternativePlans(
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

void makeStreamingPlans(
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

void AggregationFuzzer::testPlan(
    const core::PlanNodePtr& plan,
    bool injectSpill,
    bool verifyResults,
    const velox::test::ResultOrError& expected) {
  auto actual = execute(plan, injectSpill);

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

std::string makeDuckWindowSql(
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& inputs) {
  std::stringstream sql;
  sql << "SELECT " << folly::join(", ", inputs) << ", "
      << folly::join(", ", aggregates) << " OVER (";

  if (!partitionKeys.empty()) {
    sql << "partition by " << folly::join(", ", partitionKeys);
  }
  if (!sortingKeys.empty()) {
    sql << " order by " << folly::join(", ", sortingKeys);
  }

  sql << ") FROM tmp";

  return sql.str();
}

std::optional<MaterializedRowMultiset> AggregationFuzzer::computeDuckWindow(
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<RowVectorPtr>& input,
    const core::PlanNodePtr& plan) {
  // Check if DuckDB supports specified aggregate functions.
  auto windowNode = dynamic_cast<const core::WindowNode*>(plan.get());
  VELOX_CHECK_NOT_NULL(windowNode);
  for (const auto& window : windowNode->windowFunctions()) {
    if (duckFunctionNames_.count(window.functionCall->name()) == 0) {
      return std::nullopt;
    }
  }

  const auto& outputType = plan->outputType();

  if (!isDuckSupported(input[0]->type()) || !isDuckSupported(outputType)) {
    return std::nullopt;
  }

  return computeDuckResults(
      makeDuckWindowSql(
          partitionKeys,
          sortingKeys,
          aggregates,
          asRowType(input[0]->type())->names()),
      input,
      outputType);
}

void AggregationFuzzer::verifyWindow(
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<RowVectorPtr>& input,
    bool customVerification) {
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
    persistReproInfo({plan}, reproPersistPath_);
  }
  try {
    auto resultOrError = execute(plan, false /*injectSpill*/);
    if (resultOrError.exceptionPtr) {
      ++stats_.numFailed;
    }

    if (!customVerification && resultOrError.result) {
      if (auto expectedResult = computeDuckWindow(
              partitionKeys, sortingKeys, aggregates, input, plan)) {
        ++stats_.numDuckVerified;
        VELOX_CHECK(
            assertEqualResults(expectedResult.value(), {resultOrError.result}),
            "Velox and DuckDB results don't match");
      }
    }
  } catch (...) {
    if (!reproPersistPath_.empty()) {
      persistReproInfo({plan}, reproPersistPath_);
    }
    throw;
  }
}

void AggregationFuzzer::verifyAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<RowVectorPtr>& input,
    bool customVerification,
    const std::vector<std::string>& projections) {
  auto plan = PlanBuilder()
                  .values(input)
                  .singleAggregation(groupingKeys, aggregates, masks)
                  .optionalProject(projections)
                  .planNode();

  std::vector<core::PlanNodePtr> plans;
  plans.push_back(plan);
  // Create all the plans upfront.
  makeAlternativePlans(
      groupingKeys, aggregates, masks, projections, input, plans);

  // Evaluate same plans on flat inputs.
  std::vector<RowVectorPtr> flatInput;
  for (const auto& vector : input) {
    auto flat = BaseVector::create<RowVector>(
        vector->type(), vector->size(), vector->pool());
    flat->copy(vector.get(), 0, 0, vector->size());
    flatInput.push_back(flat);
  }

  makeAlternativePlans(
      groupingKeys, aggregates, masks, projections, flatInput, plans);

  if (!groupingKeys.empty()) {
    // Use OrderBy + StreamingAggregation on original input.
    makeStreamingPlans(
        groupingKeys, aggregates, masks, projections, input, plans);

    // Use OrderBy + StreamingAggregation on flattened input.
    makeStreamingPlans(
        groupingKeys, aggregates, masks, projections, flatInput, plans);
  }

  if (persistAndRunOnce_) {
    persistReproInfo(plans, reproPersistPath_);
  }

  try {
    auto resultOrError = execute(plan, false /*injectSpill*/);
    if (resultOrError.exceptionPtr) {
      ++stats_.numFailed;
    }

    const bool verifyResults = !customVerification || !projections.empty();

    std::optional<MaterializedRowMultiset> expectedResult;
    if (verifyResults) {
      expectedResult = computeDuckAggregation(
          groupingKeys, aggregates, masks, projections, input, plan);
    }

    if (expectedResult && resultOrError.result) {
      ++stats_.numDuckVerified;
      VELOX_CHECK(
          assertEqualResults(expectedResult.value(), {resultOrError.result}),
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
    std::vector<core::PlanNodePtr> plans) {
  VELOX_CHECK_GT(plans.size(), 0);
  auto plan = plans.front();
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

  for (auto gkey : groupingKeys) {
    groupingKeyNames.push_back(gkey->name());
  }

  // Get masks.
  auto masks = node->aggregateMasks();
  std::vector<std::string> maskNames;
  maskNames.reserve(masks.size());

  for (auto maskName : masks) {
    if (maskName) {
      maskNames.push_back(maskName->name());
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

  auto resultOrError = execute(plan, false /*injectSpill*/);
  if (resultOrError.exceptionPtr) {
    ++stats_.numFailed;
  }

  // Get aggregations and determine if order dependent.
  std::vector<std::string> aggregateStrings;
  aggregateStrings.reserve(node->aggregates().size());

  bool customVerification = false;
  for (auto aggregate : node->aggregates()) {
    aggregateStrings.push_back(aggregate->toString());
    customVerification |=
        customVerificationFunctions_.count(aggregate->name()) != 0;
  }

  const bool verifyResults = !customVerification || !projections.empty();

  std::optional<MaterializedRowMultiset> expectedResult;
  if (verifyResults) {
    expectedResult = computeDuckAggregation(
        groupingKeyNames,
        aggregateStrings,
        maskNames,
        projections,
        input,
        plan);
  }

  if (expectedResult && resultOrError.result) {
    ++stats_.numDuckVerified;
    VELOX_CHECK(
        assertEqualResults(expectedResult.value(), {resultOrError.result}),
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
  LOG(INFO) << "Total aggregations verified against DuckDB: "
            << printStat(numDuckVerified, numIterations);
  LOG(INFO) << "Total failed aggregations: "
            << printStat(numFailed, numIterations);
}

} // namespace
} // namespace facebook::velox::exec::test
