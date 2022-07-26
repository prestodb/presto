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

#include <mutex>
#include <string>
#include <unordered_map>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::common::testutil {

/// The test facility used to inject callback hook in production execution point
/// to get, change and verify various internal execution state for unit test.
/// The hook injected in production code path will be opt out in release build
/// and only be executed in debug build. Please follow the example in
/// TestValueTest.cpp for use.
class TestValue {
 public:
  /// Enable the test value injection.
  static void enable();
  /// Disable the test value injection.
  static void disable();
  /// Check if the test value injection is enabled or not.
  static bool enabled();

  /// Invoked by the test code to register a callback hook at the specified
  /// execution point. 'injectionPoint' is a string to identify the execution
  /// point which could be formed by concatenating namespace, class name, method
  /// name plus optional actual action within a method. 'injectionCb' is the
  /// injected callback hook.
  template <typename T>
  static void set(
      const std::string& injectionPoint,
      std::function<void(T*)> injectionCb);

  /// Invoked by the test code to unregister a callback hook at the specified
  /// execution point.
  static void clear(const std::string& injectionPoint);

  /// Invoked by the production code to try to invoke the test callback hook
  /// with 'testData' if there is one registered at the specified execution
  /// point. 'testData' can capture the production execution state.
  static void adjust(const std::string& injectionPoint, void* testData);

 private:
  using Callback = std::function<void(void*)>;

  static std::mutex mutex_;
  static bool enabled_;
  static std::unordered_map<std::string, Callback> injectionMap_;
};

class ScopedTestValue {
 public:
  template <typename T>
  ScopedTestValue(const std::string& point, std::function<void(T*)> callback)
      : point_(point) {
    VELOX_CHECK_NOT_NULL(callback);
    VELOX_CHECK(!point_.empty());
    TestValue::set<T>(point_, std::move(callback));
  }
  ~ScopedTestValue() {
    TestValue::clear(point_);
  }

 private:
  const std::string point_;
};

#ifndef NDEBUG
template <typename T>
void TestValue::set(
    const std::string& injectionPoint,
    std::function<void(T*)> injectionCb) {
  std::lock_guard<std::mutex> l(mutex_);
  if (!enabled_) {
    return;
  }
  injectionMap_[injectionPoint] = [injectionCb](void* testData) {
    T* typedData = static_cast<T*>(testData);
    injectionCb(typedData);
  };
}
#else
template <typename T>
void TestValue::set(
    const std::string& injectionPoint,
    std::function<void(T*)> injectionCb) {}
#endif

#define VELOX_CONCAT(x, y) __##x##y
#define VELOX_VARNAME(x) VELOX_CONCAT(x, Obj)

#define SCOPED_TESTVALUE_SET(point, ...) \
  ScopedTestValue VELOX_VARNAME(__LINE__)(point, ##__VA_ARGS__)

} // namespace facebook::velox::common::testutil
