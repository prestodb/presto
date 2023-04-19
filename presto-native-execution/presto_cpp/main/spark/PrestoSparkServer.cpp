/*
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
#include "presto_cpp/main/spark/PrestoSparkServer.h"
#include "presto_cpp/main/common/Configs.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::presto {
using namespace facebook::velox;

PrestoSparkServer::PrestoSparkServer(const std::string& configDirectoryPath)
    : PrestoServerBase(configDirectoryPath) {}

void PrestoSparkServer::registerFunctions() {
  PrestoServerBase::registerFunctions();
  // Register Test functions (if test mode is enabled)
  registerTestFunctions();
}

void PrestoSparkServer::registerTestFunctions() {
  if (SystemConfig::instance()->registerTestFunctions()) {
    velox::functions::prestosql::registerComparisonFunctions(
        "json.test_schema.");
    velox::aggregate::prestosql::registerAllAggregateFunctions(
        "json.test_schema.");
  }
}
} // namespace facebook::presto