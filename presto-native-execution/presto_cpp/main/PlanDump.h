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

#include <string>
#include <string_view>
#include <vector>
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/PlanNode.h"

namespace facebook::presto {

/// Helpers backing the 'plan-dump-dir' system config. They write each task's
/// Velox plan and the splits it receives to JSON files under a directory so
/// that queries run through Presto can be replayed in pure Velox.

/// Returns a filename stem for 'taskId'. Characters other than letters, digits,
/// '_', '-' and '.' are replaced with '_'. Presto task IDs only contain those
/// characters, so distinct task IDs map to distinct files.
std::string sanitizeTaskIdForPlanDumpFile(std::string_view taskId);

/// Writes 'planNode' as pretty-printed JSON to '<dir>/<taskId>.json', creating
/// 'dir' if needed. Failures are logged and never thrown.
void dumpVeloxPlan(
    const std::string& dir,
    const protocol::TaskId& taskId,
    const velox::core::PlanNodePtr& planNode);

/// Merges the splits in 'sources' into '<dir>/<taskId>.splits.json', a JSON
/// object mapping each planNodeId to an array of ScheduledSplits. Splits whose
/// sequenceId is already recorded for that planNodeId are skipped, because the
/// coordinator re-sends splits until the worker acknowledges them. Failures are
/// logged and never thrown.
void dumpSplits(
    const std::string& dir,
    const protocol::TaskId& taskId,
    const std::vector<protocol::TaskSource>& sources);

} // namespace facebook::presto
