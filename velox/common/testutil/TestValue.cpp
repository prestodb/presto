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

#include "velox/common/testutil/TestValue.h"

namespace facebook::velox::common::testutil {

std::mutex TestValue::mutex_;
bool TestValue::enabled_ = false;
std::unordered_map<std::string, TestValue::Callback> TestValue::injectionMap_;

#ifndef NDEBUG
void TestValue::enable() {
  std::lock_guard<std::mutex> l(mutex_);
  enabled_ = true;
}

void TestValue::disable() {
  std::lock_guard<std::mutex> l(mutex_);
  enabled_ = false;
}

bool TestValue::enabled() {
  std::lock_guard<std::mutex> l(mutex_);
  return enabled_;
}

void TestValue::clear(const std::string& injectionPoint) {
  std::lock_guard<std::mutex> l(mutex_);
  injectionMap_.erase(injectionPoint);
}

void TestValue::notify(
    const std::string& injectionPoint,
    const void* testData) {
  Callback injectionCb;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!enabled_ || injectionMap_.count(injectionPoint) == 0) {
      return;
    }
    injectionCb = injectionMap_[injectionPoint];
  }
  injectionCb(testData);
}
#else
void TestValue::enable() {}
void TestValue::disable() {}
bool TestValue::enabled() {
  return false;
}
void TestValue::clear(const std::string& injectionPoint) {}
void TestValue::notify(
    const std::string& injectionPoint,
    const void* testData) {}
#endif

} // namespace facebook::velox::common::testutil
