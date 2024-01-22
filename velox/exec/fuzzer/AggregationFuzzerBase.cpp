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
#include "velox/exec/fuzzer/AggregationFuzzerBase.h"

#include <boost/random/uniform_int_distribution.hpp>
#include "velox/common/base/Fs.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/expression/tests/utils/ArgumentTypeFuzzer.h"
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

DEFINE_bool(
    log_signature_stats,
    false,
    "Log statistics about function signatures");

namespace facebook::velox::exec::test {

bool AggregationFuzzerBase::addSignature(
    const std::string& name,
    const FunctionSignaturePtr& signature) {
  ++functionsStats.numSignatures;

  if (signature->variableArity()) {
    LOG(WARNING) << "Skipping variadic function signature: " << name
                 << signature->toString();
    return false;
  }

  if (!signature->variables().empty()) {
    bool skip = false;
    std::unordered_set<std::string> typeVariables;
    for (auto& [variableName, variable] : signature->variables()) {
      if (variable.isIntegerParameter()) {
        LOG(WARNING) << "Skipping generic function signature: " << name
                     << signature->toString();
        skip = true;
        break;
      }

      typeVariables.insert(variableName);
    }
    if (skip) {
      return false;
    }

    signatureTemplates_.push_back(
        {name, signature.get(), std::move(typeVariables)});
  } else {
    CallableSignature callable{
        .name = name,
        .args = {},
        .returnType =
            SignatureBinder::tryResolveType(signature->returnType(), {}, {}),
        .constantArgs = {}};
    VELOX_CHECK_NOT_NULL(callable.returnType);

    // Process each argument and figure out its type.
    for (const auto& arg : signature->argumentTypes()) {
      auto resolvedType = SignatureBinder::tryResolveType(arg, {}, {});
      VELOX_CHECK_NOT_NULL(resolvedType);

      // SignatureBinder::tryResolveType produces ROW types with empty
      // field names. These won't work with TableScan.
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

  ++functionsStats.numSupportedSignatures;
  return true;
}

void AggregationFuzzerBase::addAggregationSignatures(
    const AggregateFunctionSignatureMap& signatureMap) {
  for (auto& [name, signatures] : signatureMap) {
    ++functionsStats.numFunctions;
    bool hasSupportedSignature = false;
    for (auto& signature : signatures) {
      hasSupportedSignature |= addSignature(name, signature);
    }
    if (hasSupportedSignature) {
      ++functionsStats.numSupportedFunctions;
    }
  }
}

std::pair<CallableSignature, AggregationFuzzerBase::SignatureStats&>
AggregationFuzzerBase::pickSignature() {
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

  return {signature, signatureStats_[idx]};
}

std::vector<std::string> AggregationFuzzerBase::generateKeys(
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

std::vector<std::string> AggregationFuzzerBase::generateSortingKeys(
    const std::string& prefix,
    std::vector<std::string>& names,
    std::vector<TypePtr>& types) {
  std::vector<std::string> keys;
  auto numKeys = boost::random::uniform_int_distribution<uint32_t>(1, 5)(rng_);
  for (auto i = 0; i < numKeys; ++i) {
    keys.push_back(fmt::format("{}{}", prefix, i));

    // Pick random, possibly complex, type.
    types.push_back(vectorFuzzer_.randOrderableType(2));
    names.push_back(keys.back());
  }

  return keys;
}

std::shared_ptr<InputGenerator> AggregationFuzzerBase::findInputGenerator(
    const CallableSignature& signature) {
  auto generatorIt = customInputGenerators_.find(signature.name);
  if (generatorIt != customInputGenerators_.end()) {
    return generatorIt->second;
  }

  return nullptr;
}

std::vector<RowVectorPtr> AggregationFuzzerBase::generateInputData(
    std::vector<std::string> names,
    std::vector<TypePtr> types,
    const std::optional<CallableSignature>& signature) {
  std::shared_ptr<InputGenerator> generator;
  if (signature.has_value()) {
    generator = findInputGenerator(signature.value());
  }

  const auto size = vectorFuzzer_.getOptions().vectorSize;

  auto inputType = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> input;
  for (auto i = 0; i < FLAGS_num_batches; ++i) {
    std::vector<VectorPtr> children;

    if (generator != nullptr) {
      children = generator->generate(
          signature->args, vectorFuzzer_, rng_, pool_.get());
    }

    for (auto j = children.size(); j < inputType->size(); ++j) {
      children.push_back(vectorFuzzer_.fuzz(inputType->childAt(j), size));
    }

    input.push_back(std::make_shared<RowVector>(
        pool_.get(), inputType, nullptr, size, std::move(children)));
  }

  if (generator != nullptr) {
    generator->reset();
  }

  return input;
}

std::vector<RowVectorPtr> AggregationFuzzerBase::generateInputDataWithRowNumber(
    std::vector<std::string> names,
    std::vector<TypePtr> types,
    const CallableSignature& signature) {
  names.push_back("row_number");
  types.push_back(BIGINT());

  auto generator = findInputGenerator(signature);

  std::vector<RowVectorPtr> input;
  auto size = vectorFuzzer_.getOptions().vectorSize;
  velox::test::VectorMaker vectorMaker{pool_.get()};
  int64_t rowNumber = 0;
  for (auto j = 0; j < FLAGS_num_batches; ++j) {
    std::vector<VectorPtr> children;

    if (generator != nullptr) {
      children =
          generator->generate(signature.args, vectorFuzzer_, rng_, pool_.get());
    }

    for (auto i = children.size(); i < types.size() - 1; ++i) {
      children.push_back(vectorFuzzer_.fuzz(types[i], size));
    }
    children.push_back(vectorMaker.flatVector<int64_t>(
        size, [&](auto /*row*/) { return rowNumber++; }));
    input.push_back(vectorMaker.rowVector(names, children));
  }

  if (generator != nullptr) {
    generator->reset();
  }

  return input;
}

// static
exec::Split AggregationFuzzerBase::makeSplit(const std::string& filePath) {
  return exec::Split{std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId, filePath, dwio::common::FileFormat::DWRF)};
}

AggregationFuzzerBase::PlanWithSplits AggregationFuzzerBase::deserialize(
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

void AggregationFuzzerBase::printSignatureStats() {
  if (!FLAGS_log_signature_stats) {
    return;
  }

  for (auto i = 0; i < signatureStats_.size(); ++i) {
    const auto& stats = signatureStats_[i];
    if (stats.numRuns == 0) {
      continue;
    }

    if (stats.numFailed * 1.0 / stats.numRuns < 0.5) {
      continue;
    }

    if (i < signatures_.size()) {
      LOG(INFO) << "Signature #" << i << " failed " << stats.numFailed
                << " out of " << stats.numRuns
                << " times: " << signatures_[i].toString();
    } else {
      const auto& signatureTemplate =
          signatureTemplates_[i - signatures_.size()];
      LOG(INFO) << "Signature template #" << i << " failed " << stats.numFailed
                << " out of " << stats.numRuns
                << " times: " << signatureTemplate.name << "("
                << signatureTemplate.signature->toString() << ")";
    }
  }
}

velox::test::ResultOrError AggregationFuzzerBase::execute(
    const core::PlanNodePtr& plan,
    const std::vector<exec::Split>& splits,
    bool injectSpill,
    bool abandonPartial,
    int32_t maxDrivers) {
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

    resultOrError.result =
        builder.maxDrivers(maxDrivers).copyResults(pool_.get());
  } catch (VeloxUserError& e) {
    // NOTE: velox user exception is accepted as it is caused by the invalid
    // fuzzer test inputs.
    resultOrError.exceptionPtr = std::current_exception();
  }

  return resultOrError;
}

std::pair<
    std::optional<MaterializedRowMultiset>,
    AggregationFuzzerBase::ReferenceQueryErrorCode>
AggregationFuzzerBase::computeReferenceResults(
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& input) {
  if (auto sql = referenceQueryRunner_->toSql(plan)) {
    try {
      return std::make_pair(
          referenceQueryRunner_->execute(
              sql.value(), input, plan->outputType()),
          ReferenceQueryErrorCode::kSuccess);
    } catch (std::exception& e) {
      // ++stats_.numReferenceQueryFailed;
      LOG(WARNING) << "Query failed in the reference DB";
      return std::make_pair(
          std::nullopt, ReferenceQueryErrorCode::kReferenceQueryFail);
    }
  } else {
    LOG(INFO) << "Query not supported by the reference DB";
    // ++stats_.numVerificationNotSupported;
  }

  return std::make_pair(
      std::nullopt, ReferenceQueryErrorCode::kReferenceQueryUnsupported);
}

void AggregationFuzzerBase::testPlan(
    const PlanWithSplits& planWithSplits,
    bool injectSpill,
    bool abandonPartial,
    bool customVerification,
    const std::vector<std::shared_ptr<ResultVerifier>>& customVerifiers,
    const velox::test::ResultOrError& expected,
    int32_t maxDrivers) {
  auto actual = execute(
      planWithSplits.plan,
      planWithSplits.splits,
      injectSpill,
      abandonPartial,
      maxDrivers);

  // Compare results or exceptions (if any). Fail is anything is different.
  if (expected.exceptionPtr || actual.exceptionPtr) {
    // Throws in case exceptions are not compatible.
    velox::test::compareExceptions(expected.exceptionPtr, actual.exceptionPtr);
    return;
  }

  if (!customVerification) {
    VELOX_CHECK(
        assertEqualResults({expected.result}, {actual.result}),
        "Logically equivalent plans produced different results");
    return;
  }

  VELOX_CHECK_EQ(
      expected.result->size(),
      actual.result->size(),
      "Logically equivalent plans produced different number of rows");

  for (auto& verifier : customVerifiers) {
    if (verifier == nullptr) {
      continue;
    }

    if (verifier->supportsCompare()) {
      VELOX_CHECK(
          verifier->compare(expected.result, actual.result),
          "Logically equivalent plans produced different results");
    } else if (verifier->supportsVerify()) {
      VELOX_CHECK(
          verifier->verify(actual.result),
          "Result of a logically equivalent plan failed custom verification");
    } else {
      VELOX_UNREACHABLE(
          "Custom verifier must support either 'compare' or 'verify' API.");
    }
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
} // namespace

// Sometimes we generate zero-column input of type ROW({}) or a column of type
// UNKNOWN(). Such data cannot be written to a file and therefore cannot
// be tested with TableScan.
bool isTableScanSupported(const TypePtr& type) {
  if (type->kind() == TypeKind::ROW && type->size() == 0) {
    return false;
  }
  if (type->kind() == TypeKind::UNKNOWN) {
    return false;
  }
  if (type->kind() == TypeKind::HUGEINT) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isTableScanSupported(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

std::vector<exec::Split> AggregationFuzzerBase::makeSplits(
    const std::vector<RowVectorPtr>& inputs,
    const std::string& path) {
  std::vector<exec::Split> splits;
  auto writerPool = rootPool_->addAggregateChild("writer");
  for (auto i = 0; i < inputs.size(); ++i) {
    const std::string filePath = fmt::format("{}/{}", path, i);
    writeToFile(filePath, inputs[i], writerPool.get());
    splits.push_back(makeSplit(filePath));
  }

  return splits;
}

std::string printPercentageStat(size_t n, size_t total) {
  return fmt::format("{} ({:.2f}%)", n, (double)n / total * 100);
}

void printStats(const AggregationFuzzerBase::FunctionsStats& stats) {
  LOG(INFO) << fmt::format(
      "Total functions: {} ({} signatures)",
      stats.numFunctions,
      stats.numSignatures);
  LOG(INFO) << "Functions with at least one supported signature: "
            << printPercentageStat(
                   stats.numSupportedFunctions, stats.numFunctions);

  size_t numNotSupportedFunctions =
      stats.numFunctions - stats.numSupportedFunctions;
  LOG(INFO) << "Functions with no supported signature: "
            << printPercentageStat(
                   numNotSupportedFunctions, stats.numFunctions);
  LOG(INFO) << "Supported function signatures: "
            << printPercentageStat(
                   stats.numSupportedSignatures, stats.numSignatures);

  size_t numNotSupportedSignatures =
      stats.numSignatures - stats.numSupportedSignatures;
  LOG(INFO) << "Unsupported function signatures: "
            << printPercentageStat(
                   numNotSupportedSignatures, stats.numSignatures);
}

std::string makeFunctionCall(
    const std::string& name,
    const std::vector<std::string>& argNames,
    bool sortedInputs,
    bool distinctInputs) {
  std::ostringstream call;
  call << name << "(";

  const auto args = folly::join(", ", argNames);
  if (sortedInputs) {
    call << args << " ORDER BY " << args;
  } else if (distinctInputs) {
    call << "distinct " << args;
  } else {
    call << args;
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
    const AggregationFuzzerBase::PlanWithSplits& planWithSplits,
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
    const std::vector<AggregationFuzzerBase::PlanWithSplits>& plans,
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

} // namespace facebook::velox::exec::test
