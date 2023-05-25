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
    int start = 0;
    auto pos = nextDot(taskId, start, false);
    queryId_ = taskId.substr(0, pos);

    start = pos + 1;
    pos = nextDot(taskId, start, false);
    stageId_ = parseInt(taskId, start, pos);

    start = pos + 1;
    pos = nextDot(taskId, start, false);
    stageExecutionId_ = parseInt(taskId, start, pos);

    start = pos + 1;
    pos = nextDot(taskId, start, true);
    if (pos == -1) {
      id_ = parseInt(taskId, start, taskId.length());
      attemptNumber_ = 0;
    } else {
      id_ = parseInt(taskId, start, pos);

      start = pos + 1;
      attemptNumber_ = parseInt(taskId, start, taskId.length());
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
  int nextDot(const std::string& taskId, int start, bool allowEnd) {
    auto pos = taskId.find(".", start);
    if (pos == std::string::npos) {
      if (allowEnd) {
        return -1;
      } else {
        VELOX_USER_FAIL("Malformed task ID: {}", taskId);
      }
    }
    return pos;
  }

  int parseInt(const std::string& taskId, int start, int end) {
    return folly::to<int>(taskId.substr(start, end - start));
  }

  std::string queryId_;
  int32_t stageId_;
  int32_t stageExecutionId_;
  int32_t id_;
  int32_t attemptNumber_;
};
} // namespace facebook::presto
