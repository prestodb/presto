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
#include <folly/Conv.h>
#include <string>
#include "velox/common/base/Exceptions.h"

namespace facebook::presto {
class PrestoTaskId {
 public:
  explicit PrestoTaskId(const std::string& taskId) {
    std::vector<std::string> taskIdParts;
    folly::split('.', taskId, taskIdParts);

    // TODO (adutta): Remove taskIdParts.size() < 4 after presto
    // release with new taskid format.
    if (taskIdParts.size() < 4 || taskIdParts.size() > 5) {
      VELOX_USER_FAIL("Malformed task ID: {}", taskId);
    }

    queryId_ = taskIdParts[0];
    stageId_ = folly::to<int32_t>(taskIdParts[1]);
    stageExecutionId_ = folly::to<int32_t>(taskIdParts[2]);
    id_ = folly::to<int32_t>(taskIdParts[3]);
    if (taskIdParts.size() == 5) {
      attemptNumber_ = folly::to<int32_t>(taskIdParts[4]);
    }
  }

  const std::string& queryId() const {
    return queryId_;
  }

  int32_t stageId() const {
    return stageId_;
  }

  int32_t stageExecutionId() const {
    return stageExecutionId_;
  }

  int32_t id() const {
    return id_;
  }

  int32_t attemptNumber() const {
    return attemptNumber_;
  }

 private:
  std::string queryId_;
  int32_t stageId_{0};
  int32_t stageExecutionId_{0};
  int32_t id_{0};
  int32_t attemptNumber_{0};
};
} // namespace facebook::presto
