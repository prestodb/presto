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
#include "presto_cpp/main/PlanDump.h"
#include <fmt/format.h>
#include <folly/json.h>
#include <glog/logging.h>
#include <array>
#include <cctype>
#include <fstream>
#include <mutex>
#include <unordered_set>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "velox/common/base/Fs.h"

namespace facebook::presto {

namespace {

// Serializes writes to the same dump file across concurrent task updates. A
// fixed set of mutexes keeps memory bounded however many tasks are dumped.
std::mutex& dumpFileMutex(const std::string& path) {
  static constexpr size_t kNumMutexes = 64;
  static std::array<std::mutex, kNumMutexes> mutexes;
  return mutexes[std::hash<std::string>{}(path) % kNumMutexes];
}

std::string dumpFilePath(
    const std::string& dir,
    const protocol::TaskId& taskId,
    std::string_view suffix) {
  return fmt::format(
      "{}/{}{}", dir, sanitizeTaskIdForPlanDumpFile(taskId), suffix);
}

void writeFile(const std::string& path, const std::string& content) {
  std::ofstream outFile;
  outFile.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  outFile.open(path);
  outFile << content;
  outFile.close();
}

// Reads previously dumped splits from 'path'. Returns an empty object if the
// file does not exist or cannot be parsed.
nlohmann::json readSplitsFile(const std::string& path) {
  std::ifstream inFile(path);
  if (!inFile.good()) {
    return nlohmann::json::object();
  }
  try {
    nlohmann::json existing;
    inFile >> existing;
    if (existing.is_object()) {
      return existing;
    }
    LOG(WARNING) << "Discarding splits dump " << path
                 << ": expected a JSON object";
  } catch (const std::exception& e) {
    LOG(WARNING) << "Discarding unreadable splits dump " << path << ": "
                 << e.what();
  }
  return nlohmann::json::object();
}

} // namespace

std::string sanitizeTaskIdForPlanDumpFile(std::string_view taskId) {
  std::string safeId;
  safeId.reserve(taskId.size());
  for (char c : taskId) {
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' ||
        c == '.') {
      safeId.push_back(c);
    } else {
      safeId.push_back('_');
    }
  }
  if (safeId.empty() || safeId == "." || safeId == "..") {
    return "task";
  }
  return safeId;
}

void dumpVeloxPlan(
    const std::string& dir,
    const protocol::TaskId& taskId,
    const velox::core::PlanNodePtr& planNode) {
  const auto path = dumpFilePath(dir, taskId, ".json");
  try {
    const auto content = folly::toPrettyJson(planNode->serialize());
    std::lock_guard<std::mutex> lock(dumpFileMutex(path));
    fs::create_directories(dir);
    writeFile(path, content);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to dump plan for task " << taskId << " to " << path
                 << ": " << e.what();
  }
}

void dumpSplits(
    const std::string& dir,
    const protocol::TaskId& taskId,
    const std::vector<protocol::TaskSource>& sources) {
  bool hasSplits = false;
  for (const auto& source : sources) {
    if (!source.splits.empty()) {
      hasSplits = true;
      break;
    }
  }
  if (!hasSplits) {
    return;
  }

  const auto path = dumpFilePath(dir, taskId, ".splits.json");
  try {
    std::lock_guard<std::mutex> lock(dumpFileMutex(path));
    fs::create_directories(dir);
    auto existing = readSplitsFile(path);
    for (const auto& source : sources) {
      if (source.splits.empty()) {
        continue;
      }
      auto& nodeSplits = existing[source.planNodeId];
      if (!nodeSplits.is_array()) {
        nodeSplits = nlohmann::json::array();
      }
      std::unordered_set<int64_t> seenSequenceIds;
      for (const auto& split : nodeSplits) {
        if (split.contains("sequenceId")) {
          seenSequenceIds.insert(split["sequenceId"].get<int64_t>());
        }
      }
      for (const auto& split : source.splits) {
        if (!seenSequenceIds.insert(split.sequenceId).second) {
          continue;
        }
        nlohmann::json splitJson;
        protocol::to_json(splitJson, split);
        nodeSplits.push_back(std::move(splitJson));
      }
    }
    writeFile(path, existing.dump(2));
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to dump splits for task " << taskId << " to "
                 << path << ": " << e.what();
  }
}

} // namespace facebook::presto
