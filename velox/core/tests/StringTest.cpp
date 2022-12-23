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
#include "velox/core/CoreTypeSystem.h"

using namespace facebook::velox::core;
using namespace facebook::velox;

namespace {
StringWriter createStringWriter(const std::string& str) {
  StringWriter writer;
  UDFOutputString::assign(writer, str);
  return writer;
}
} // namespace

TEST(String, StringWriter) {
  // Default constructor & empty string copy.
  {
    const std::string emptyString{""};
    StringWriter writer;
    UDFOutputString::assign(writer, emptyString);
    EXPECT_EQ(StringView(writer), StringView(emptyString));
  }

  const std::string sampleTest{"short string"};
  // Copy constructor
  {
    const auto src = createStringWriter(sampleTest);
    StringWriter dst(src);
    EXPECT_NE(dst.data(), src.data());
    EXPECT_EQ(StringView(dst), StringView(sampleTest));
  }

  // Move constructor
  {
    StringWriter dst(createStringWriter(sampleTest));
    EXPECT_EQ(StringView(dst), StringView(sampleTest));
  }

  // Copy assignment
  {
    const auto src = createStringWriter(sampleTest);
    StringWriter dst;
    dst = src;
    EXPECT_NE(dst.data(), src.data());
    EXPECT_EQ(StringView(dst), StringView(sampleTest));
  }

  // Move assignment
  {
    StringWriter dst;
    dst = createStringWriter(sampleTest);
    EXPECT_EQ(StringView(dst), StringView(sampleTest));
  }
}
