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

#include <gtest/gtest.h>

#include "velox/common/testutil/TestValue.h"

namespace {

using namespace facebook::velox::common::testutil;

class TestObject {
 public:
  TestObject() = default;

  void set(int value) {
    ++count_;
    value_ = value;
    const std::pair<int, int> testValue(value_, count_);
    TestValue::notify(
        "facebook::velox::exec::test::TestObject::set", &testValue);
  }

 private:
  int count_ = 0;
  int value_;
};

// NOTE: we can only the testvalue tests on debug build.
#ifndef NDEBUG
TEST(TestValueTest, testValueDisabled) {
  TestValue::disable();
  EXPECT_FALSE(TestValue::enabled());
  int setCount = 0;
  int setValue = 0;
  int value = 200;
  TestValue::set<std::pair<int, int>>(
      "facebook::velox::exec::test::TestObject::set",
      [&](const std::pair<int, int>* testData) {
        setValue = testData->first;
        setCount = testData->second;
      });
  TestObject obj;
  obj.set(value);
  // Since test value has not been enabled, then both 'setValue' and 'setCount'
  // won't be set.
  EXPECT_EQ(0, setValue);
  EXPECT_EQ(0, setCount);

  TestValue::clear("facebook::velox::exec::test::TestObject::set");
  TestValue::clear("facebook::velox::exec::test::TestObject::internalSet");

  obj.set(value);
  EXPECT_EQ(0, setValue);
  EXPECT_EQ(0, setCount);
}

TEST(TestValueTest, testValueEnabled) {
  TestValue::enable();
  EXPECT_TRUE(TestValue::enabled());
  int setCount = 0;
  int setValue = 0;
  int value = 200;
  TestValue::set<std::pair<int, int>>(
      "facebook::velox::exec::test::TestObject::set",
      [&](const std::pair<int, int>* testData) {
        setValue = testData->first;
        setCount = testData->second;
      });
  TestObject obj;
  obj.set(value);
  EXPECT_EQ(value, setValue);
  EXPECT_EQ(1, setCount);

  setCount = 0;
  setValue = 0;
  obj.set(value + 1);
  EXPECT_EQ(value + 1, setValue);
  EXPECT_EQ(2, setCount);

  TestValue::clear("facebook::velox::exec::test::TestObject::set");
  setCount = 0;
  setValue = 0;
  obj.set(value + 2);
  // The outer test value has been cleared so 'setValue' will be set
  // method input value.
  EXPECT_EQ(0, setValue);
  EXPECT_EQ(0, setCount);
}

TEST(TestValueTest, scopeUsageEnabled) {
  TestValue::enable();
  EXPECT_TRUE(TestValue::enabled());
  {
    // Invalid ctor checks.
    EXPECT_ANY_THROW(ScopedTestValue(
        "", std::function<void(const void*)>([&](const void* /*unused*/) {})));
    EXPECT_ANY_THROW(
        ScopedTestValue("dummy", std::function<void(const void*)>(nullptr)));
  }
  {
    int setCount = 0;
    int setValue = 0;
    int value = 200;
    TestObject obj;
    {
      ScopedTestValue testInternalSet(
          "facebook::velox::exec::test::TestObject::set",
          std::function<void(const std::pair<int, int>*)>(
              [&](const auto* testData) {
                setValue = testData->first;
                setCount = testData->second;
              }));
      obj.set(value);
      EXPECT_EQ(value, setValue);
      EXPECT_EQ(1, setCount);
    }
    // Scoped bject dtor will clear the test value settings.
    setCount = 0;
    setValue = 0;
    obj.set(value + 1);
    EXPECT_EQ(0, setValue);
    EXPECT_EQ(0, setCount);
  }
  {
    int setCount = 0;
    int setValue = 0;
    int value = 200;
    TestObject obj;
    {
      SCOPED_TESTVALUE_SET(
          "facebook::velox::exec::test::TestObject::set",
          std::function<void(const std::pair<int, int>*)>(
              ([&](const auto* testData) {
                setValue = testData->first;
                setCount = testData->second;
              })));
      obj.set(value);
      EXPECT_EQ(value, setValue);
      EXPECT_EQ(1, setCount);
    }
    // Scoped object dtor will clear the test value settings.
    setCount = 0;
    setValue = 0;
    obj.set(value);
    EXPECT_EQ(0, setValue);
    EXPECT_EQ(0, setCount);
  }
}

TEST(TestValueTest, scopeUsageDisabled) {
  TestValue::disable();
  EXPECT_FALSE(TestValue::enabled());
  {
    int setCount = 0;
    int setValue = 0;
    int value = 200;
    TestObject obj;
    ScopedTestValue testInternalSet(
        "facebook::velox::exec::test::TestObject::set",
        std::function<void(const std::pair<int, int>*)>(
            [&](const auto* testData) {
              setValue = testData->first;
              setCount = testData->second;
            }));
    obj.set(value);
    // If test value has not been enabled, then both 'setValue' and
    // 'setCount' won't be set.
    EXPECT_EQ(0, setValue);
    EXPECT_EQ(0, setCount);
  }

  {
    int setCount = 0;
    int setValue = 0;
    int value = 200;
    TestObject obj;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::test::TestObject::set",
        std::function<void(const std::pair<int, int>*)>(
            [&](const auto* testData) {
              setValue = testData->first;
              setCount = testData->second;
            }));
    obj.set(value);
    // If test value has not been enabled, then both 'setValue' and
    // 'setCount' won't be set.
    EXPECT_EQ(0, setValue);
    EXPECT_EQ(0, setCount);
  }
}
#endif
} // namespace
