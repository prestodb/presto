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
#include "presto_cpp/main/operators/DynamicFilterSource.h"
#include <glog/logging.h>

namespace facebook::presto::operators {

DynamicFilterCallbackRegistry& DynamicFilterCallbackRegistry::instance() {
  static DynamicFilterCallbackRegistry registry;
  return registry;
}

void DynamicFilterCallbackRegistry::registerCallbacks(
    const std::string& taskId,
    FlushCallback flushCallback,
    RegisterCallback registerCallback) {
  callbacks_.wlock()->emplace(
      taskId, Callbacks{std::move(flushCallback), std::move(registerCallback)});
}

void DynamicFilterCallbackRegistry::registerFilterIds(
    const std::string& taskId,
    const std::unordered_set<std::string>& filterIds) {
  RegisterCallback callback;
  {
    auto locked = callbacks_.rlock();
    auto it = locked->find(taskId);
    if (it == locked->end()) {
      LOG(WARNING) << "registerFilterIds: no callback found for task="
                   << taskId;
      return;
    }
    callback = it->second.registerIds;
  }
  if (callback) {
    callback(filterIds);
  }
}

void DynamicFilterCallbackRegistry::fire(
    const std::string& taskId,
    std::map<std::string, protocol::TupleDomain<std::string>> filters,
    std::unordered_set<std::string> flushedFilterIds) {
  VLOG(1) << "fire: task=" << taskId << " filters=" << filters.size()
          << " flushedFilterIds=" << flushedFilterIds.size();
  FlushCallback callback;
  {
    auto locked = callbacks_.rlock();
    auto it = locked->find(taskId);
    if (it == locked->end()) {
      LOG(WARNING) << "fire: no callback found for task=" << taskId;
      return;
    }
    callback = it->second.flush;
  }
  if (callback) {
    callback(filters, flushedFilterIds);
  }
}

void DynamicFilterCallbackRegistry::removeCallback(const std::string& taskId) {
  callbacks_.wlock()->erase(taskId);
}

} // namespace facebook::presto::operators
