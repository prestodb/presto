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
#include "velox/functions/sparksql/window/WindowFunctionsRegistration.h"
#include "velox/functions/lib/window/RegistrationFunctions.h"

namespace facebook::velox::functions::window::sparksql {

void registerWindowFunctions(const std::string& prefix) {
  functions::window::registerNthValueInteger(prefix + "nth_value");
  functions::window::registerRowNumberInteger(prefix + "row_number");
  functions::window::registerRankInteger(prefix + "rank");
  functions::window::registerDenseRankInteger(prefix + "dense_rank");
  functions::window::registerNtileInteger(prefix + "ntile");
}

} // namespace facebook::velox::functions::window::sparksql
