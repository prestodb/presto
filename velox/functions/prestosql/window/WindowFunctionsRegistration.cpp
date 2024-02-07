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
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/functions/lib/window/RegistrationFunctions.h"

namespace facebook::velox::window {

namespace prestosql {

extern void registerCumeDist(const std::string& name);
extern void registerNtileBigint(const std::string& name);
extern void registerFirstValue(const std::string& name);
extern void registerLastValue(const std::string& name);
extern void registerLag(const std::string& name);
extern void registerLead(const std::string& name);

void registerAllWindowFunctions(const std::string& prefix) {
  functions::window::registerRowNumberBigint(prefix + "row_number");
  functions::window::registerRankBigint(prefix + "rank");
  functions::window::registerDenseRankBigint(prefix + "dense_rank");
  functions::window::registerPercentRank(prefix + "percent_rank");
  registerCumeDist(prefix + "cume_dist");
  functions::window::registerNtileBigint(prefix + "ntile");
  functions::window::registerNthValueBigint(prefix + "nth_value");
  registerFirstValue(prefix + "first_value");
  registerLastValue(prefix + "last_value");
  registerLag(prefix + "lag");
  registerLead(prefix + "lead");
}

} // namespace prestosql

} // namespace facebook::velox::window
