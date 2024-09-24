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

#include <folly/json.h>

#include "velox/exec/trace/QueryTraceTraits.h"
#include "velox/exec/trace/QueryTraceUtil.h"
#include "velox/tool/trace/QueryTraceReplayer.h"

#include "velox/common/serialization/Serializable.h"
#include "velox/core/PlanNode.h"

DEFINE_bool(usage, false, "Show the usage");
DEFINE_string(root, "", "Root dir of the query tracing");
DEFINE_bool(summary, false, "Show the summary of the tracing");
DEFINE_bool(short_summary, false, "Only show number of tasks and task ids");
DEFINE_string(
    task_id,
    "",
    "Specify the target task id, if empty, show the summary of all the traced query task.");

using namespace facebook::velox;

namespace facebook::velox::tool::trace {

QueryTraceReplayer::QueryTraceReplayer()
    : rootDir_(FLAGS_root), taskId_(FLAGS_task_id) {}

void QueryTraceReplayer::printSummary() const {
  const auto fs = filesystems::getFileSystem(rootDir_, nullptr);
  const auto taskIds = exec::trace::getTaskIds(rootDir_, fs);
  if (taskIds.empty()) {
    LOG(ERROR) << "No traced query task under " << rootDir_;
    return;
  }

  std::ostringstream summary;
  summary << "\n++++++Query trace summary++++++\n";
  summary << "Number of tasks: " << taskIds.size() << "\n";
  summary << "Task ids: " << folly::join(",", taskIds);

  if (FLAGS_short_summary) {
    LOG(INFO) << summary.str();
    return;
  }

  const auto summaryTaskIds =
      taskId_.empty() ? taskIds : std::vector<std::string>{taskId_};
  for (const auto& taskId : summaryTaskIds) {
    summary << "\n++++++Query configs and plan of task " << taskId
            << ":++++++\n";
    const auto traceTaskDir = fmt::format("{}/{}", rootDir_, taskId);
    const auto queryMetaFile = fmt::format(
        "{}/{}",
        traceTaskDir,
        exec::trace::QueryTraceTraits::kQueryMetaFileName);
    const auto metaObj = exec::trace::getMetadata(queryMetaFile, fs);
    const auto& configObj =
        metaObj[exec::trace::QueryTraceTraits::kQueryConfigKey];
    summary << "++++++Query configs++++++\n";
    summary << folly::toJson(configObj) << "\n";
    summary << "++++++Query plan++++++\n";
    const auto queryPlan = ISerializable::deserialize<core::PlanNode>(
        metaObj[exec::trace::QueryTraceTraits::kPlanNodeKey],
        memory::MemoryManager::getInstance()->tracePool());
    summary << queryPlan->toString(true, true);
  }
  LOG(INFO) << summary.str();
}

std::string QueryTraceReplayer::usage() {
  std::ostringstream usage;
  usage
      << "++++++Query Trace Tool Usage++++++\n"
      << "The following options are available:\n"
      << "--usage: Show the usage\n"
      << "--root: Root dir of the query tracing, it must be set\n"
      << "--summary: Show the summary of the tracing including number of tasks"
      << "and task ids. It also print the query metadata including"
      << "query configs, connectors properties, and query plan in JSON format.\n"
      << "--short_summary: Only show number of tasks and task ids.\n"
      << "--pretty: Show the summary of the tracing in pretty JSON.\n"
      << "--task_id: Specify the target task id, if empty, show the summary of "
      << "all the traced query task.\n";
  return usage.str();
}

} // namespace facebook::velox::tool::trace
