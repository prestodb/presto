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

#include "presto_cpp/main/types/VeloxPlanValidator.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/memory/MemoryPool.h"

namespace facebook::presto {

/// Convert a Presto plan fragment to a Velox Plan. If the conversion fails,
/// the failure info will be included in the response.
/// @param planFragmentJson string that will parse as protocol::PlanFragment.
/// @param pool MemoryPool used during conversion.
/// @param planValidator VeloxPlanValidator to validate the converted plan.
protocol::PlanConversionResponse prestoToVeloxPlanConversion(
    const std::string& planFragmentJson,
    velox::memory::MemoryPool* pool,
    const VeloxPlanValidator* planValidator);

} // namespace facebook::presto
