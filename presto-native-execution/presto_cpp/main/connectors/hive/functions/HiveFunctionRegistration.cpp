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

#include "presto_cpp/main/connectors/hive/functions/HiveFunctionRegistration.h"

#include "presto_cpp/main/connectors/hive/functions/InitcapFunction.h"
#include "presto_cpp/main/functions/dynamic_registry/DynamicFunctionRegistrar.h"

using namespace facebook::velox;
namespace facebook::presto::hive::functions {

namespace {
void registerHiveFunctions() {
  // Register functions under the 'hive.default' namespace.
  facebook::presto::registerPrestoFunction<InitCapFunction, Varchar, Varchar>(
      "initcap", "hive.default");
}
} // namespace

void registerHiveNativeFunctions() {
  static std::once_flag once;
  std::call_once(once, []() { registerHiveFunctions(); });
}

} // namespace facebook::presto::hive::functions
