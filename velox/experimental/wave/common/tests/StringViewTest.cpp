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

#include "velox/experimental/wave/common/StringView.h"

namespace facebook::velox::wave {
namespace {

TEST(StringViewTest, empty) {
  StringView sv;
  sv.init(reinterpret_cast<const char*>(0xDEADBEEFBADEFEEDll), 0);
  ASSERT_EQ(sv.size(), 0);
  ASSERT_EQ(sv, StringView{});
}

TEST(StringViewTest, inlined) {
  StringView sv;
  sv.init("foobarquux", 3);
  ASSERT_EQ(sv.size(), 3);
  ASSERT_EQ(strncmp(sv.data(), "foo", 3), 0);
  ASSERT_LT(
      reinterpret_cast<uintptr_t>(&sv), reinterpret_cast<uintptr_t>(sv.data()));
  ASSERT_LT(
      reinterpret_cast<uintptr_t>(sv.data()),
      reinterpret_cast<uintptr_t>(&sv + 1));
  ASSERT_NE(sv, StringView{});
  StringView sv2;
  sv2.init("foobar", 3);
  ASSERT_EQ(sv, sv2);
  sv2.init("foobar", 4);
  ASSERT_NE(sv, sv2);
  sv2.init("quux", 3);
  ASSERT_NE(sv, sv2);
}

TEST(StringViewTest, nonInlined) {
  StringView sv;
  const char* data = "foobarquux";
  sv.init(data, 10);
  ASSERT_EQ(sv.size(), 10);
  ASSERT_EQ(strncmp(sv.data(), data, 10), 0);
  ASSERT_EQ(sv.data(), data);
  const char* data2 = "foobarquux2";
  StringView sv2;
  sv2.init(data2, 10);
  ASSERT_EQ(sv, sv2);
  sv2.init(data2, 11);
  ASSERT_NE(sv, sv2);
  sv2.init(data, 3);
  ASSERT_NE(sv, sv2);
}

} // namespace
} // namespace facebook::velox::wave
