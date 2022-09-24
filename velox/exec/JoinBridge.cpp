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
#include "velox/exec/JoinBridge.h"

namespace facebook::velox::exec {
// static
void JoinBridge::notify(std::vector<ContinuePromise> promises) {
  for (auto& promise : promises) {
    promise.setValue();
  }
}

void JoinBridge::start() {
  std::lock_guard<std::mutex> l(mutex_);
  started_ = true;
}

void JoinBridge::cancel() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    cancelled_ = true;
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

} // namespace facebook::velox::exec
