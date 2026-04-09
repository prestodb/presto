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

#include <folly/Synchronized.h>
#include <functional>
#include <map>
#include <string>
#include <unordered_set>
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/type/Type.h"

namespace facebook::presto::operators {

/// Metadata for a single dynamic filter channel within a join.
struct DynamicFilterChannel {
  std::string filterId;
  velox::column_index_t columnIndex;
  velox::TypePtr type;
};

/// Global per-task callback registry for delivering dynamic filter data
/// to PrestoTask. Used by both the HashBuild filter extraction path and
/// the coordinator's filter collection endpoint.
class DynamicFilterCallbackRegistry {
 public:
  /// Called when dynamic filters are produced for a task.
  using FlushCallback = std::function<void(
      const std::map<std::string, protocol::TupleDomain<std::string>>&,
      const std::unordered_set<std::string>&)>;

  /// Called to register filter IDs the task will produce, so the coordinator
  /// knows when all filters have been delivered.
  using RegisterCallback =
      std::function<void(const std::unordered_set<std::string>&)>;

  static DynamicFilterCallbackRegistry& instance();

  void registerCallbacks(
      const std::string& taskId,
      FlushCallback flushCallback,
      RegisterCallback registerCallback);

  /// Registers filter IDs for a task.
  void registerFilterIds(
      const std::string& taskId,
      const std::unordered_set<std::string>& filterIds);

  /// Invokes the flush callback for the given task without removing it.
  /// Multiple join nodes within the same task can each call fire()
  /// independently.
  void fire(
      const std::string& taskId,
      std::map<std::string, protocol::TupleDomain<std::string>> filters,
      std::unordered_set<std::string> flushedFilterIds);

  void removeCallback(const std::string& taskId);

 private:
  struct Callbacks {
    FlushCallback flush;
    RegisterCallback registerIds;
  };
  folly::Synchronized<std::map<std::string, Callbacks>> callbacks_;
};

} // namespace facebook::presto::operators
