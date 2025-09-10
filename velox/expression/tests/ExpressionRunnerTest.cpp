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

#include "velox/expression/tests/ExpressionRunner.h"
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfWriter.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/vector/VectorSaver.h"

using namespace facebook::velox;
using facebook::velox::exec::test::PrestoQueryRunner;
using facebook::velox::test::ReferenceQueryRunner;

DEFINE_string(
    input_paths,
    "",
    "Comma separated list of paths for vectors to be restored from disk. This "
    "will enable single run of the fuzzer with the on-disk persisted repro "
    "information. This has to be set with sql_path and optionally "
    "result_path.");

DEFINE_string(
    input_selectivity_vector_paths,
    "",
    "Comma separated list of paths for selectivity vectors to be restored "
    "from disk. The list needs to match 1-to-1 with the files specified in "
    "input_paths. If not specified, all rows will be selected.");

DEFINE_string(
    sql_path,
    "",
    "Path for expression SQL to be restored from disk. This will enable "
    "single run of the fuzzer with the on-disk persisted repro information. "
    "This has to be set with input_path and optionally result_path.");

DEFINE_string(
    complex_constant_path,
    "",
    "Path for complex constants that aren't expressible in SQL.");

DEFINE_string(
    sql,
    "",
    "Comma separated SQL expressions to evaluate. This flag and --sql_path "
    "flag are mutually exclusive. If both are specified, --sql is used and "
    "--sql_path is ignored.");

DEFINE_string(
    registry,
    "presto",
    "Funciton registry to use for expression evaluation. Currently supported values are "
    "presto and spark. Default is presto.");

DEFINE_string(
    result_path,
    "",
    "Path for result vector to restore from disk. This is optional for "
    "on-disk reproduction. Don't set if the initial repro result vector is "
    "nullptr");

DEFINE_string(
    mode,
    "common",
    "Mode for expression runner: \n"
    "verify: evaluate the expression and compare results between common and "
    "simplified paths.\n"
    "common: evaluate the expression using common path and print out results.\n"
    "simplified: evaluate the expression using simplified path and print out "
    "results.\n"
    "query: evaluate SQL query specified in --sql or --sql_path and print out "
    "results. If --input_paths is specified, the query may reference it as "
    "table 't'. Note: --input_selectivity_vector_paths is ignored in this mode");

DEFINE_string(
    input_row_metadata_path,
    "",
    "Path for the file stored on-disk which contains a struct containing "
    "input row metadata. This includes columns in the input row vector to "
    "be wrapped in a lazy vector and/or dictionary encoded. It may also "
    "contain a dictionary peel for columns requiring dictionary encoding.");

DEFINE_bool(
    use_seperate_memory_pool_for_input_vector,
    true,
    "If true, expression evaluator and input vectors use different memory pools."
    " This helps trigger code-paths that can depend on vectors having different"
    " pools. For eg, when copying a flat string vector copies of the strings"
    " stored in the string buffers need to be created. If however, the pools"
    " were the same between the vectors then the buffers can simply be shared"
    " between them instead.");

DEFINE_string(
    reference_db_url,
    "",
    "ReferenceDB URI along with port. If set, we use the reference DB as the "
    "source of truth. Otherwise, use Velox simplified eval path. Example: "
    "--reference_db_url=http://127.0.0.1:8080");

DEFINE_uint32(
    req_timeout_ms,
    10000,
    "Timeout in milliseconds for HTTP requests made to reference DB, "
    "such as Presto. Example: --req_timeout_ms=2000");

static bool validateMode(const char* flagName, const std::string& value) {
  static const std::unordered_set<std::string> kModes = {
      "common", "simplified", "verify", "query"};
  if (kModes.count(value) != 1) {
    std::cerr << "Invalid value for --" << flagName << ": " << value << ". ";
    std::cerr << "Valid values are: " << folly::join(", ", kModes) << "."
              << std::endl;
    return false;
  }

  return true;
}

static bool validateRegistry(const char* flagName, const std::string& value) {
  static const std::unordered_set<std::string> kRegistries = {
      "presto", "spark"};
  if (kRegistries.count(value) != 1) {
    std::cerr << "Invalid value for --" << flagName << ": " << value << ". ";
    std::cerr << "Valid values are: " << folly::join(", ", kRegistries) << "."
              << std::endl;
    return false;
  }
  if (value == "spark") {
    functions::sparksql::registerFunctions("");
  } else if (value == "presto") {
    functions::prestosql::registerAllScalarFunctions();
  }

  return true;
}

DEFINE_validator(mode, &validateMode);
DEFINE_validator(registry, &validateRegistry);

DEFINE_int32(
    num_rows,
    10,
    "Maximum number of rows to process. Zero means 'all rows'. Applies to "
    "'common' and 'simplified' modes only. Ignored for 'verify' mode.");

DEFINE_string(
    store_result_path,
    "",
    "Directory path for storing the results of evaluating SQL expression or "
    "query in common, simplified or query modes.");

DEFINE_string(
    fuzzer_repro_path,
    "",
    "Directory path where all input files generated by ExpressionVerifier are "
    "expected to reside. For more details on which files and their names are "
    "expected, please checkout the ExpressionVerifier class. Any file paths "
    "already specified via a startup flag will take precedence.");

DEFINE_bool(
    find_minimal_subexpression,
    false,
    "Automatically seeks minimum failed subexpression on result mismatch");

static std::string checkAndReturnFilePath(
    const std::string_view& fileName,
    const std::string& flagName) {
  auto path = fmt::format("{}/{}", FLAGS_fuzzer_repro_path, fileName);
  if (fs::exists(path)) {
    LOG(INFO) << "Using " << flagName << " = " << path;
    return path;
  } else {
    LOG(INFO) << "File for " << flagName << " not found.";
  }
  return "";
}

static std::string getFilesWithPrefix(
    const char* dirPath,
    const std::string_view& prefix,
    const std::string& flagName) {
  std::vector<std::string> filesPaths;
  std::stringstream ss;
  int numFilesFound = 0;
  if (!std::filesystem::exists(dirPath)) {
    LOG(ERROR) << "Directory does not exist: " << dirPath << std::endl;
    return "";
  }
  for (const auto& entry : std::filesystem::directory_iterator(dirPath)) {
    if (entry.is_regular_file()) {
      std::string filename = entry.path().filename();
      if (filename.find(prefix) == 0) {
        if (++numFilesFound > 1) {
          ss << ",";
        }
        ss << entry.path().string();
      }
    }
  }
  LOG(INFO) << "Using " << flagName << " = " << ss.str();
  return ss.str();
}

static void checkDirForExpectedFiles() {
  LOG(INFO) << "Searching input directory for expected files at "
            << FLAGS_fuzzer_repro_path;

  FLAGS_input_paths = FLAGS_input_paths.empty()
      ? getFilesWithPrefix(
            FLAGS_fuzzer_repro_path.c_str(),
            test::ExpressionVerifier::kInputVectorFileNamePrefix,
            "input_paths")
      : FLAGS_input_paths;
  FLAGS_input_selectivity_vector_paths =
      FLAGS_input_selectivity_vector_paths.empty()
      ? getFilesWithPrefix(
            FLAGS_fuzzer_repro_path.c_str(),
            test::ExpressionVerifier::kInputSelectivityVectorFileNamePrefix,
            "input_selectivity_vector_paths")
      : FLAGS_input_selectivity_vector_paths;
  FLAGS_result_path = FLAGS_result_path.empty()
      ? checkAndReturnFilePath(
            test::ExpressionVerifier::kResultVectorFileName, "result_path")
      : FLAGS_result_path;
  FLAGS_sql_path = FLAGS_sql_path.empty()
      ? checkAndReturnFilePath(
            test::ExpressionVerifier::kExpressionSqlFileName, "sql_path")
      : FLAGS_sql_path;
  FLAGS_input_row_metadata_path = FLAGS_input_row_metadata_path.empty()
      ? checkAndReturnFilePath(
            test::ExpressionVerifier::kInputRowMetadataFileName,
            "input_row_metadata_path")
      : FLAGS_input_row_metadata_path;
  FLAGS_complex_constant_path = FLAGS_complex_constant_path.empty()
      ? checkAndReturnFilePath(
            test::ExpressionVerifier::kComplexConstantsFileName,
            "complex_constant_path")
      : FLAGS_complex_constant_path;
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  if (!FLAGS_fuzzer_repro_path.empty()) {
    checkDirForExpectedFiles();
  }

  if (FLAGS_sql.empty() && FLAGS_sql_path.empty()) {
    std::cerr << "One of --sql or --sql_path flags must be set." << std::endl;
    exit(1);
  }

  auto sql = FLAGS_sql;
  if (sql.empty()) {
    sql = restoreStringFromFile(FLAGS_sql_path.c_str());
    VELOX_CHECK(!sql.empty());
  }

  memory::initializeMemoryManager(memory::MemoryManager::Options{});

  filesystems::registerLocalFileSystem();
  exec::test::registerHiveConnector({});
  dwrf::registerDwrfWriterFactory();

  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner{nullptr};
  if (FLAGS_registry == "presto" && !FLAGS_reference_db_url.empty()) {
    referenceQueryRunner = std::make_shared<PrestoQueryRunner>(
        rootPool.get(),
        FLAGS_reference_db_url,
        "expression_runner_test",
        static_cast<std::chrono::milliseconds>(FLAGS_req_timeout_ms));
    LOG(INFO) << "Using Presto as the reference DB.";
  }

  test::ExpressionRunner::run(
      FLAGS_input_paths,
      FLAGS_input_selectivity_vector_paths,
      sql,
      FLAGS_complex_constant_path,
      FLAGS_result_path,
      FLAGS_mode,
      FLAGS_num_rows,
      FLAGS_store_result_path,
      FLAGS_input_row_metadata_path,
      referenceQueryRunner,
      FLAGS_find_minimal_subexpression,
      FLAGS_use_seperate_memory_pool_for_input_vector);
}
