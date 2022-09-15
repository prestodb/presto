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
#include <iterator>

#include "velox/dwio/dwrf/common/Encryption.h"
#include "velox/dwio/dwrf/writer/Writer.h"

using namespace ::testing;

namespace facebook::velox::dwrf {
TEST(TestEncodingIter, Ctor) {
  proto::StripeFooter footer;
  std::vector<proto::StripeEncryptionGroup> encryptionGroups;
  // footer []
  // encryption groups []
  {
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().begin(),
        footer.encoding().end()};
    EXPECT_EQ(footer.encoding().begin(), iter.current_);
    EXPECT_EQ(footer.encoding().end(), iter.currentEnd_);
  }
  {
    // A valid end iterator.
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().end(),
        footer.encoding().end()};
    EXPECT_EQ(footer.encoding().end(), iter.current_);
    EXPECT_EQ(footer.encoding().end(), iter.currentEnd_);
  }
  footer.add_encoding();
  // footer [e]
  // encryption groups []
  {
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().begin(),
        footer.encoding().end()};
    EXPECT_EQ(footer.encoding().begin(), iter.current_);
    EXPECT_EQ(footer.encoding().end(), iter.currentEnd_);
  }
  {
    // A valid end iterator.
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().end(),
        footer.encoding().end()};
    EXPECT_EQ(footer.encoding().end(), iter.current_);
    EXPECT_EQ(footer.encoding().end(), iter.currentEnd_);
  }
  proto::StripeEncryptionGroup group1;
  proto::StripeEncryptionGroup group2;
  encryptionGroups.push_back(group1);
  encryptionGroups.push_back(group2);
  // footer [e]
  // encryption groups [[], []]
  {
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().begin(),
        footer.encoding().end()};
    EXPECT_EQ(footer.encoding().begin(), iter.current_);
    EXPECT_EQ(footer.encoding().end(), iter.currentEnd_);
  }
  {
    // A valid end iterator.
    EncodingIter iter{
        footer,
        encryptionGroups,
        1,
        encryptionGroups.at(1).encoding().end(),
        encryptionGroups.at(1).encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
  {
    // An adjusted end iterator.
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().end(),
        footer.encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
  encryptionGroups[1].add_encoding();
  // footer [e]
  // encryption groups [[], [e]]
  {
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().begin(),
        footer.encoding().end()};
    EXPECT_EQ(footer.encoding().begin(), iter.current_);
    EXPECT_EQ(footer.encoding().end(), iter.currentEnd_);
  }
  {
    // An adjusted iterator.
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().end(),
        footer.encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().begin(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
  {
    // An adjusted iterator.
    EncodingIter iter{
        footer,
        encryptionGroups,
        0,
        encryptionGroups.at(0).encoding().begin(),
        encryptionGroups.at(0).encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().begin(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
  {
    // A valid end iterator.
    EncodingIter iter{
        footer,
        encryptionGroups,
        1,
        encryptionGroups.at(1).encoding().end(),
        encryptionGroups.at(1).encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
  footer.Clear();
  // footer []
  // encryption groups [[], [e]]
  {
    // An adjusted iterator further back.
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().begin(),
        footer.encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().begin(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
  {
    // An adjusted iterator further back.
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().end(),
        footer.encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().begin(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
  encryptionGroups.at(1).Clear();
  // footer []
  // encryption groups [[], []]
  {
    // An adjusted end iterator.
    EncodingIter iter{
        footer,
        encryptionGroups,
        0,
        encryptionGroups.at(0).encoding().end(),
        encryptionGroups.at(0).encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
  {
    // An adjusted end iterator further back.
    EncodingIter iter{
        footer,
        encryptionGroups,
        -1,
        footer.encoding().end(),
        footer.encoding().end()};
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.current_);
    EXPECT_EQ(encryptionGroups.at(1).encoding().end(), iter.currentEnd_);
  }
}

// The Ctor test has covered most iterator adjustments. Suffices
// to then just sanity test pass-in values.
TEST(TestEncodingIter, EncodingIterBeginAndEnd) {
  proto::StripeFooter footer;
  footer.add_encoding();
  std::vector<proto::StripeEncryptionGroup> encryptionGroups;
  proto::StripeEncryptionGroup group1;
  group1.add_encoding();
  proto::StripeEncryptionGroup group2;
  group2.add_encoding();
  group2.add_encoding();
  encryptionGroups.push_back(group1);
  encryptionGroups.push_back(group2);
  EncodingIter begin{
      footer,
      encryptionGroups,
      -1,
      footer.encoding().begin(),
      footer.encoding().end()};
  EXPECT_EQ(begin, EncodingIter::begin(footer, encryptionGroups));
  EncodingIter end{
      footer,
      encryptionGroups,
      1,
      encryptionGroups.at(1).encoding().end(),
      encryptionGroups.at(1).encoding().end()};
  EXPECT_EQ(end, EncodingIter::end(footer, encryptionGroups));
}

namespace {
void testEncodingIter(
    const std::vector<std::pair<uint32_t, uint32_t>>& footerEncodingNodes,
    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>&
        encryptionGroupNodes) {
  proto::StripeFooter footer;
  std::vector<proto::StripeEncryptionGroup> encryptionGroups;
  std::vector<std::pair<uint32_t, uint32_t>> allEncoding;
  for (const auto& pair : footerEncodingNodes) {
    auto encoding = footer.add_encoding();
    encoding->set_node(pair.first);
    encoding->set_sequence(pair.second);
    allEncoding.push_back(pair);
  }

  for (const auto& groupNodes : encryptionGroupNodes) {
    proto::StripeEncryptionGroup group;
    for (const auto& pair : groupNodes) {
      auto encoding = group.add_encoding();
      encoding->set_node(pair.first);
      encoding->set_sequence(pair.second);
      allEncoding.push_back(pair);
    }
    encryptionGroups.push_back(group);
  }

  auto iter = EncodingIter::begin(footer, encryptionGroups);
  auto end = EncodingIter::end(footer, encryptionGroups);
  ASSERT_NE(iter, end);
  std::vector<std::pair<uint32_t, uint32_t>> iteratedEncodings;
  for (; iter != end; ++iter) {
    iteratedEncodings.push_back({iter->node(), iter->sequence()});
  }

  EXPECT_THAT(
      iteratedEncodings,
      ElementsAreArray(allEncoding.data(), allEncoding.size()));
}
} // namespace

TEST(TestEncodingManager, EncodingIter) {
  testEncodingIter({{1, 0}}, {});
  testEncodingIter({}, {{{1, 0}}});
  testEncodingIter({{1, 0}}, {{{2, 1}, {2, 3}}});
  testEncodingIter({{2, 1}, {2, 3}}, {{{1, 0}}});
  testEncodingIter(
      {{1, 0}}, {{{2, 1}, {2, 3}}, {{3, 0}, {4, 0}, {5, 1}, {5, 2}, {5, 4}}});
  testEncodingIter(
      {{2, 1}, {2, 3}, {3, 0}, {4, 0}, {5, 1}, {5, 2}, {5, 4}}, {{{1, 0}}});
}
} // namespace facebook::velox::dwrf
