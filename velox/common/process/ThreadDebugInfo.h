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

#pragma once

#include <functional>
#include <string>

namespace facebook::velox::process {

// Used to store thread local information which can be retrieved by a signal
// handler and logged.
struct ThreadDebugInfo {
  std::string queryId_;
  std::string taskId_;
  // Callback to invoke when the debug info is to be dumped. Can be empty.
  std::function<void()> callback_;
};

// A RAII class to store thread local debug information.
class ScopedThreadDebugInfo {
 public:
  explicit ScopedThreadDebugInfo(const ThreadDebugInfo& localDebugInfo);
  ~ScopedThreadDebugInfo();

 private:
  const ThreadDebugInfo* prevThreadDebugInfo_ = nullptr;
};

// Get the current thread local debug information. Used by signal handlers or
// can be accessed during a debug session. Returns nullptr if no debug info is
// set.
const ThreadDebugInfo* GetThreadDebugInfo();

// Install a signal handler to dump thread local debug information. This should
// be called before calling folly::symbolizer::installFatalSignalCallbacks()
// which is usually called at startup via folly::Init init{}. This is just a
// default implementation but you can install your own signal handler. Make sure
// to install one at the start of your program.
void addDefaultFatalSignalHandler();

} // namespace facebook::velox::process
