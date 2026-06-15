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

#pragma once

#include "velox/exec/Aggregate.h"

namespace facebook::presto::functions::aggregate {

velox::exec::AggregateRegistrationResult registerKllSketchAggregate(
    const std::string& prefix,
    bool withCompanionFunctions = true,
    bool overwrite = false);

velox::exec::AggregateRegistrationResult registerKllSketchWithKAggregate(
    const std::string& prefix,
    bool withCompanionFunctions = true,
    bool overwrite = false);

} // namespace facebook::presto::functions::aggregate

namespace facebook::presto::functions {

void registerKllSketchType();

void registerKllSketchFunctions(const std::string& prefix = "");

inline void registerAllKllSketchFunctions(const std::string& prefix = "") {
  registerKllSketchType();

  aggregate::registerKllSketchAggregate(prefix);
  aggregate::registerKllSketchWithKAggregate(prefix);
  registerKllSketchFunctions(prefix);
}

} // namespace facebook::presto::functions
