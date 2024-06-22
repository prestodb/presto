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

#include "velox/common/process/ThreadDebugInfo.h"

#include <folly/experimental/symbolizer/SignalHandler.h>
#include <glog/logging.h>

namespace facebook::velox::process {
thread_local const ThreadDebugInfo* threadDebugInfo = nullptr;

// Flag to ensure that printCurrentQueryId() only invokes the callback in
// ThreadDebugInfo once. This is to prevent callback from being recursively
// called in case it induces a fatal signal which ends up calling
// printCurrentQueryId() again.
thread_local bool fatalSignalProcessed = false;

static void printCurrentQueryId() {
  const ThreadDebugInfo* info = GetThreadDebugInfo();
  if (info == nullptr) {
    const char* msg =
        "Fatal signal handler. "
        "ThreadDebugInfo object not found.";
    write(STDERR_FILENO, msg, strlen(msg));
  } else {
    const char* msg1 = "Fatal signal handler. Query Id= ";
    write(STDERR_FILENO, msg1, strlen(msg1));
    write(STDERR_FILENO, info->queryId_.c_str(), info->queryId_.length());
    const char* msg2 = " Task Id= ";
    write(STDERR_FILENO, msg2, strlen(msg2));
    write(STDERR_FILENO, info->taskId_.c_str(), info->taskId_.length());
    if (!fatalSignalProcessed && info->callback_) {
      fatalSignalProcessed = true;
      info->callback_();
    }
  }
  write(STDERR_FILENO, "\n", 1);
}

const ThreadDebugInfo* GetThreadDebugInfo() {
  return threadDebugInfo;
}

ScopedThreadDebugInfo::ScopedThreadDebugInfo(
    const ThreadDebugInfo& localDebugInfo)
    : prevThreadDebugInfo_(threadDebugInfo) {
  threadDebugInfo = &localDebugInfo;
}

ScopedThreadDebugInfo::ScopedThreadDebugInfo(
    const ThreadDebugInfo* localDebugInfo)
    : prevThreadDebugInfo_(threadDebugInfo) {
  if (localDebugInfo != nullptr) {
    threadDebugInfo = localDebugInfo;
  }
}

ScopedThreadDebugInfo::~ScopedThreadDebugInfo() {
  threadDebugInfo = prevThreadDebugInfo_;
}

void addDefaultFatalSignalHandler() {
  static bool initialized = false;
  if (!initialized) {
    folly::symbolizer::addFatalSignalCallback(&printCurrentQueryId);
    initialized = true;
  }
}

} // namespace facebook::velox::process
