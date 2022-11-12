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
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/tests/ArgumentTypeFuzzer.h"
#include "velox/expression/tests/FuzzerToolkit.h"
#include "velox/vector/VectorSaver.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

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

struct ResultOrError {
  RowVectorPtr result;
  std::exception_ptr exceptionPtr;
};

class AggregationFuzzer {
 public:
  AggregationFuzzer(
      AggregateFunctionSignatureMap signatureMap,
      size_t seed,
      const std::unordered_map<std::string, std::string>&
          orderDependentFunctions);

  void go();

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

    // Number of iterations where results were verified against DuckDB,
    size_t numDuckDbVerified{0};

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

  std::vector<std::string> generateGroupingKeys(
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  CallableSignature pickSignature();

  std::vector<RowVectorPtr> generateInputData(
      std::vector<std::string> names,
      std::vector<TypePtr> types);

  void verify(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      const std::vector<RowVectorPtr>& input,
      bool orderDependent,
      const std::vector<std::string>& projections);

  std::optional<MaterializedRowMultiset> computeDuckDbResult(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      const std::vector<std::string>& projections,
      const std::vector<RowVectorPtr>& input,
      const core::PlanNodePtr& plan);

  ResultOrError execute(const core::PlanNodePtr& plan);

  void testPlans(
      const std::vector<core::PlanNodePtr>& plans,
      bool verifyResults,
      const ResultOrError& expected);

  const std::unordered_map<std::string, std::string> orderDependentFunctions_;
  const bool persistAndRunOnce_;
  const std::string reproPersistPath_;

  std::unordered_set<std::string> duckDbFunctionNames_;

  std::vector<CallableSignature> signatures_;
  std::vector<SignatureTemplate> signatureTemplates_;

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  VectorFuzzer vectorFuzzer_;

  Stats stats_;
};
} // namespace

void aggregateFuzzer(
    AggregateFunctionSignatureMap signatureMap,
    size_t seed,
    const std::unordered_map<std::string, std::string>&
        orderDependentFunctions) {
  AggregationFuzzer(std::move(signatureMap), seed, orderDependentFunctions)
      .go();
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
  LOG(INFO) << "Supported function signatures: {} "
            << printStat(numSupportedSignatures, numSignatures);

  size_t numNotSupportedSignatures = numSignatures - numSupportedSignatures;
  LOG(INFO) << "Unsupported function signatures: {} "
            << printStat(numNotSupportedSignatures, numSignatures);
}

std::unordered_set<std::string> getDuckDbFunctions() {
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
    const std::unordered_map<std::string, std::string>& orderDependentFunctions)
    : orderDependentFunctions_{orderDependentFunctions},
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

  duckDbFunctionNames_ = getDuckDbFunctions();

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

      if (!signature->typeVariableConstraints().empty()) {
        bool skip = false;
        std::unordered_set<std::string> typeVariables;
        for (auto& constraint : signature->typeVariableConstraints()) {
          if (constraint.isIntegerParameter()) {
            LOG(WARNING) << "Skipping generic function signature: " << name
                         << signature->toString();
            skip = true;
            break;
          }

          typeVariables.insert(constraint.name());
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
            .returnType =
                SignatureBinder::tryResolveType(signature->returnType(), {})};
        VELOX_CHECK_NOT_NULL(callable.returnType);

        // Process each argument and figure out its type.
        for (const auto& arg : signature->argumentTypes()) {
          auto resolvedType = SignatureBinder::tryResolveType(arg, {});
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
    const std::vector<RowVectorPtr>& input,
    const core::PlanNodePtr& plan,
    const std::string& basePath) {
  std::string inputPath;

  if (!common::generateFileDirectory(basePath.c_str())) {
    return;
  }

  // Save input vector.
  auto inputPathOpt = generateFilePath(basePath.c_str(), "vector");
  if (!inputPathOpt.has_value()) {
    inputPath = "Failed to create file for saving input vector.";
  } else {
    inputPath = inputPathOpt.value();
    try {
      // TODO Save all input vectors.
      saveVectorToFile(input.front().get(), inputPath.c_str());
    } catch (std::exception& e) {
      inputPath = e.what();
    }
  }

  // Save plan.
  std::string planPath;
  auto planPathOpt = generateFilePath(basePath.c_str(), "plan");
  if (!planPathOpt.has_value()) {
    planPath = "Failed to create file for saving SQL.";
  } else {
    planPath = planPathOpt.value();
    try {
      saveStringToFile(
          plan->toString(true /*detailed*/, true /*recursive*/),
          planPath.c_str());
    } catch (std::exception& e) {
      planPath = e.what();
    }
  }

  LOG(INFO) << "Persisted input: " << inputPath << " and plan: " << planPath;
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

std::vector<std::string> AggregationFuzzer::generateGroupingKeys(
    std::vector<std::string>& names,
    std::vector<TypePtr>& types) {
  auto numGroupingKeys =
      boost::random::uniform_int_distribution<uint32_t>(1, 5)(rng_);
  std::vector<std::string> groupingKeys;
  for (auto i = 0; i < numGroupingKeys; ++i) {
    groupingKeys.push_back(fmt::format("g{}", i));

    // Pick random scalar type.
    types.push_back(vectorFuzzer_.randType(0 /*maxDepth*/));
    names.push_back(groupingKeys.back());
  }
  return groupingKeys;
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

      auto groupingKeys = generateGroupingKeys(names, types);
      auto input = generateInputData(names, types);

      verify(groupingKeys, {}, {}, input, false, {});
    } else {
      // Pick a random signature.
      CallableSignature signature = pickSignature();
      stats_.functionNames.insert(signature.name);

      const bool orderDependent =
          orderDependentFunctions_.count(signature.name) != 0;

      std::vector<TypePtr> argTypes = signature.args;
      std::vector<std::string> argNames = makeNames(argTypes.size());
      auto call = makeFunctionCall(signature.name, argNames);

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
        groupingKeys = generateGroupingKeys(argNames, argTypes);
      }

      std::vector<std::string> projections;
      if (orderDependent) {
        // Add optional projection on the original result to make it order
        // independent for comparison.
        auto mitigation = orderDependentFunctions_.at(signature.name);
        if (!mitigation.empty()) {
          projections = groupingKeys;
          projections.push_back(fmt::format(fmt::runtime(mitigation), "a0"));
        }
      }

      auto input = generateInputData(argNames, argTypes);

      verify(groupingKeys, {call}, masks, input, orderDependent, projections);
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

ResultOrError AggregationFuzzer::execute(const core::PlanNodePtr& plan) {
  LOG(INFO) << "Executing query plan: " << std::endl
            << plan->toString(true, true);

  ResultOrError resultOrError;
  try {
    resultOrError.result =
        AssertQueryBuilder(plan).maxDrivers(2).copyResults(pool_.get());
    LOG(INFO) << resultOrError.result->toString();
  } catch (VeloxUserError& e) {
    // NOTE: velox user exception is accepted as it is caused by the invalid
    // fuzzer test inputs.
    resultOrError.exceptionPtr = std::current_exception();
  }

  return resultOrError;
}

// Generate SELECT <keys>, <aggregates> FROM tmp GROUP BY <keys>.
std::string makeDuckDbSql(
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

std::optional<MaterializedRowMultiset> AggregationFuzzer::computeDuckDbResult(
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
    if (duckDbFunctionNames_.count(agg->name()) == 0) {
      return std::nullopt;
    }
  }

  const auto& outputType = plan->outputType();

  // Skip queries that use Timestamp type.
  // DuckDB doesn't support nanosecond precision for timestamps.
  for (auto i = 0; i < input[0]->type()->size(); ++i) {
    if (input[0]->type()->childAt(i)->isTimestamp()) {
      return std::nullopt;
    }
  }

  for (auto i = 0; i < outputType->size(); ++i) {
    if (outputType->childAt(i)->isTimestamp()) {
      return std::nullopt;
    }
  }

  DuckDbQueryRunner queryRunner;
  queryRunner.createTable("tmp", {input});
  return queryRunner.execute(
      makeDuckDbSql(groupingKeys, aggregates, masks, projections), outputType);
}

std::vector<core::PlanNodePtr> makeAlternativePlans(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<std::string>& projections,
    const std::vector<RowVectorPtr>& inputVectors) {
  std::vector<core::PlanNodePtr> plans;

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

  return plans;
}

void AggregationFuzzer::testPlans(
    const std::vector<core::PlanNodePtr>& plans,
    bool verifyResults,
    const ResultOrError& expected) {
  for (const auto& plan : plans) {
    auto actual = execute(plan);

    // Compare results or exceptions (if any). Fail is anything is different.
    if (expected.exceptionPtr || actual.exceptionPtr) {
      // Throws in case exceptions are not compatible.
      velox::test::compareExceptions(
          expected.exceptionPtr, actual.exceptionPtr);
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
}

void AggregationFuzzer::verify(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    const std::vector<RowVectorPtr>& input,
    bool orderDependent,
    const std::vector<std::string>& projections) {
  auto plan = PlanBuilder()
                  .values(input)
                  .singleAggregation(groupingKeys, aggregates, masks)
                  .optionalProject(projections)
                  .planNode();

  if (persistAndRunOnce_) {
    persistReproInfo(input, plan, reproPersistPath_);
  }

  try {
    auto resultOrError = execute(plan);
    if (resultOrError.exceptionPtr) {
      ++stats_.numFailed;
    }

    const bool verifyResults = !orderDependent || !projections.empty();

    std::optional<MaterializedRowMultiset> expectedResult;
    try {
      if (verifyResults) {
        expectedResult = computeDuckDbResult(
            groupingKeys, aggregates, masks, projections, input, plan);
        ++stats_.numDuckDbVerified;
      }
    } catch (std::exception& e) {
      LOG(WARNING) << "Couldn't get results from DuckDB";
    }

    if (expectedResult && resultOrError.result) {
      VELOX_CHECK(
          assertEqualResults(expectedResult.value(), {resultOrError.result}),
          "Velox and DuckDB results don't match");
    }

    auto altPlans = makeAlternativePlans(
        groupingKeys, aggregates, masks, projections, input);
    testPlans(altPlans, verifyResults, resultOrError);

    // Evaluate same plans on flat inputs.
    std::vector<RowVectorPtr> flatInput;
    for (const auto& vector : input) {
      auto flat = BaseVector::create<RowVector>(
          vector->type(), vector->size(), vector->pool());
      flat->copy(vector.get(), 0, 0, vector->size());
      flatInput.push_back(flat);
    }

    altPlans = makeAlternativePlans(
        groupingKeys, aggregates, masks, projections, flatInput);
    testPlans(altPlans, verifyResults, resultOrError);

  } catch (...) {
    if (!reproPersistPath_.empty()) {
      persistReproInfo(input, plan, reproPersistPath_);
    }
    throw;
  }
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
  LOG(INFO) << "Total aggregations verified against DuckDB: "
            << printStat(numDuckDbVerified, numIterations);
  LOG(INFO) << "Total failed aggregations: "
            << printStat(numFailed, numIterations);
}

} // namespace
} // namespace facebook::velox::exec::test
