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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sstream>
#include "velox/common/base/RawVector.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/time/Timer.h"
#include "velox/type/Type.h"

using namespace facebook::velox;

TEST(StringView, basic) {
  std::string text = "We are stardust, we are golden...";
  for (int32_t i = 0; i < text.size(); ++i) {
    std::string subText(text.data(), i);

    LOG(INFO) << "Testing for i = " << i << " subText = " << subText;
    StringView view = StringView(subText.data(), subText.size());
    EXPECT_EQ(view.size(), i);
    EXPECT_EQ(view.isInline(), i <= StringView::kInlineSize);
    if (view.isInline()) {
      EXPECT_NE(view.data(), subText.data());
    } else {
      EXPECT_EQ(view.data(), subText.data());
    }
    EXPECT_EQ(view.materialize(), subText);
    EXPECT_EQ(view.getString(), subText);
    EXPECT_EQ(view, StringView(subText.data(), subText.size()));

    std::stringstream viewAsSteam;
    viewAsSteam << view;
    EXPECT_EQ(subText, viewAsSteam.str());
  }
}

TEST(StringView, comparison) {
  // Differ in prefix.
  EXPECT_LT(StringView(" ab"), StringView("ab"));
  // Differ in inlined part.
  EXPECT_GT(StringView("In hoc signo"), StringView("In hoc signO"));
  // Inlined and out of line differ.
  EXPECT_LT(
      StringView("In hoc signo"),
      StringView("in hoc signo vinces, Constantinus"));
}

TEST(StringView, container) {
  std::vector<std::string> strings = {
      "May",
      "I walk",
      "beside you",
      "I've come here to lose ",
      "the smog"
      "feel like a cog in something",
      "turning"};
  std::vector<StringView> views;
  std::unordered_map<StringView, int32_t> map;
  for (int32_t i = 0; i < strings.size(); ++i) {
    views.push_back(StringView(strings[i]));
    map[views.back()] = i;
  }
  for (int32_t i = 0; i < strings.size(); ++i) {
    auto it = map.find(StringView(strings[i].c_str(), strings[i].size()));
    EXPECT_EQ(it->second, i);
  }
  std::sort(views.begin(), views.end());
  for (int32_t i = 0; i < views.size() - 1; i++) {
    EXPECT_LE(views[i], views[i + 1]);
  }
}

TEST(StringView, selfComparison) {
  std::vector<std::string> texts{
      "USA", // Within prefix
      "CUBA", // Exactly prefix
      "ARGENTINA", // Within Inlined
      "UNITEDSTATES", // Exactly Inlined
      "UNITED STATES", // Barely Not Inlined
      "UNITED STATES OF AMERICA" // Not Inlined
  };
  std::vector<std::string> copyTexts(texts);

  // Compare same view.
  for (auto& text : texts) {
    StringView view = StringView(text.data(), text.size());
    EXPECT_EQ(view.compare(view), 0);
    EXPECT_EQ(view < view, false);
    EXPECT_EQ(view > view, false);
    EXPECT_EQ(view >= view, true);
    EXPECT_EQ(view <= view, true);
    EXPECT_EQ(view == view, true);
    EXPECT_EQ(view != view, false);
  }

  // Compare views with same content.
  for (auto i = 0; i < texts.size(); ++i) {
    StringView lhs = StringView(texts[i].data(), texts[i].size());
    StringView rhs = StringView(copyTexts[i].data(), copyTexts[i].size());
    EXPECT_EQ(lhs.compare(rhs), 0);
    EXPECT_EQ(lhs < rhs, false);
    EXPECT_EQ(lhs > rhs, false);
    EXPECT_EQ(lhs >= rhs, true);
    EXPECT_EQ(lhs <= rhs, true);
    EXPECT_EQ(lhs == rhs, true);
    EXPECT_EQ(lhs != rhs, false);
  }
}

TEST(StringView, literal) {
  EXPECT_EQ("ab"_sv, StringView("ab"));

  std::vector<StringView> vec = {"a"_sv, "b"_sv};
  EXPECT_THAT(vec, ::testing::ElementsAre("a"_sv, "b"_sv));
}

TEST(StringView, implicitConstructionAndConversion) {
  StringView sv1("literal");
  EXPECT_EQ(sv1, "literal");

  StringView sv2{"literal"};
  EXPECT_EQ(sv2, "literal");

  StringView sv3 = "literal";
  EXPECT_EQ(sv3, "literal");

  std::optional<StringView> sv4 = "literal";
  EXPECT_TRUE(sv4.has_value());
  EXPECT_EQ(*sv4, "literal");

  std::optional<StringView> sv5("literal");
  EXPECT_TRUE(sv5.has_value());
  EXPECT_EQ(*sv5, "literal");

  auto testRegularConversion = [](StringView sv) { EXPECT_EQ(sv, "literal"); };
  testRegularConversion("literal");

  auto testOptionalConversion = [](std::optional<StringView> sv) {
    EXPECT_TRUE(sv.has_value());
    EXPECT_EQ(sv, "literal");
  };
  testOptionalConversion("literal");
}

TEST(StringView, negativeSizes) {
  EXPECT_THROW(StringView("abc", -10), VeloxException);
  EXPECT_NO_THROW(StringView(nullptr, 0));
}

int32_t linearSearchSimple(
    StringView key,
    const StringView* strings,
    const int32_t* indices,
    int32_t numStrings) {
  if (indices) {
    for (auto i = 0; i < numStrings; ++i) {
      if (strings[indices[i]] == key) {
        return i;
      }
    }
  } else {
    for (auto i = 0; i < numStrings; ++i) {
      if (strings[i] == key) {
        return i;
      }
    }
  }
  return -1;
}

TEST(StringView, linearSearch) {
  constexpr int32_t kSize = 1003;
  std::vector<raw_vector<char>> data(kSize);
  std::vector<StringView> stringViews(kSize);
  // Distinct values with sizes from 0 to 50.
  for (auto i = 0; i < 1000; ++i) {
    std::string string = fmt::format("{}-", i);
    int32_t numRepeats = 1 + i % 10;
    auto item = string;
    for (auto repeat = 0; repeat < numRepeats; ++repeat) {
      string += item;
    }
    string.resize(std::min<int32_t>((i >= 50 ? 2 : 0) + string.size(), i % 50));
    data[i].resize(string.size());
    if (!string.empty()) {
      memcpy(data[i].data(), string.data(), string.size());
    }
    stringViews[i] = StringView(data[i].data(), data[i].size());
  }
  raw_vector<int32_t> indices(kSize);
  for (auto i = 0; i < kSize; ++i) {
    indices[i] = 999 - i;
  }

  uint64_t simdUsec = 0;
  uint64_t loopUsec = 0;
  uint64_t simdIndicesUsec = 0;
  uint64_t loopIndicesUsec = 0;
  for (auto counter = 0; counter < 10; ++counter) {
    {
      MicrosecondTimer t(&simdUsec);
      EXPECT_EQ(
          -1,
          StringView::linearSearch(
              stringViews[11], stringViews.data(), nullptr, 10));
      for (auto i = 0; i < kSize; ++i) {
        auto testIndex = (i * 1) % kSize;
        auto index = StringView::linearSearch(
            stringViews[testIndex],
            stringViews.data(),
            nullptr,
            stringViews.size());
        EXPECT_TRUE(stringViews[testIndex] == stringViews[index]);
      }
    }
    {
      MicrosecondTimer t(&loopUsec);
      EXPECT_EQ(
          -1,
          linearSearchSimple(stringViews[11], stringViews.data(), nullptr, 10));
      for (auto i = 0; i < kSize; ++i) {
        auto testIndex = (i * 1) % kSize;
        auto index = linearSearchSimple(
            stringViews[testIndex],
            stringViews.data(),
            nullptr,
            stringViews.size());
        EXPECT_TRUE(stringViews[testIndex] == stringViews[index]);
      }
    }

    {
      MicrosecondTimer t(&simdIndicesUsec);
      EXPECT_EQ(
          -1,
          StringView::linearSearch(
              stringViews[indices[11]],
              stringViews.data(),
              indices.data(),
              10));
      for (auto i = 0; i < kSize; ++i) {
        auto testIndex = (i * 1) % kSize;
        auto index = StringView::linearSearch(
            stringViews[testIndex],
            stringViews.data(),
            indices.data(),
            stringViews.size());
        EXPECT_TRUE(stringViews[testIndex] == stringViews[indices[index]]);
      }
    }
    {
      MicrosecondTimer t(&loopIndicesUsec);
      EXPECT_EQ(
          -1,
          linearSearchSimple(
              stringViews[indices[11]],
              stringViews.data(),
              indices.data(),
              10));
      for (auto i = 0; i < kSize; ++i) {
        auto testIndex = (i * 1) % kSize;
        auto index = linearSearchSimple(
            stringViews[testIndex],
            stringViews.data(),
            indices.data(),
            stringViews.size());
        EXPECT_TRUE(stringViews[testIndex] == stringViews[indices[index]]);
      }
    }
  }
  LOG(INFO) << "StringView search: SIMD: " << simdUsec << " / "
            << simdIndicesUsec << " scalar: " << loopUsec << " / "
            << loopIndicesUsec;
}
