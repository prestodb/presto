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

#include "velox/dwio/dwrf/writer/WriterShared.h"

using namespace ::testing;

namespace facebook::velox::dwrf {
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

  EncodingIter iter{footer, encryptionGroups};
  ASSERT_FALSE(iter.empty());
  std::vector<std::pair<uint32_t, uint32_t>> iteratedEncodings;
  do {
    iteratedEncodings.push_back(
        {iter.current().node(), iter.current().sequence()});
  } while (iter.next());

  EXPECT_THAT(
      iteratedEncodings,
      ElementsAreArray(allEncoding.data(), allEncoding.size()));
}

TEST(TestWriterContext, EmptyEncodingIter) {
  proto::StripeFooter footer;
  std::vector<proto::StripeEncryptionGroup> encryptionGroups;
  {
    EncodingIter iter{footer, encryptionGroups};
    ASSERT_TRUE(iter.empty());
  }

  footer.add_encoding();
  {
    EncodingIter iter{footer, encryptionGroups};
    ASSERT_FALSE(iter.empty());
  }

  footer.Clear();
  {
    EncodingIter iter{footer, encryptionGroups};
    ASSERT_TRUE(iter.empty());
  }

  proto::StripeEncryptionGroup group1;
  proto::StripeEncryptionGroup group2;
  encryptionGroups.push_back(group1);
  encryptionGroups.push_back(group2);
  {
    EncodingIter iter{footer, encryptionGroups};
    ASSERT_TRUE(iter.empty());
  }

  encryptionGroups[1].add_encoding();
  {
    EncodingIter iter{footer, encryptionGroups};
    ASSERT_FALSE(iter.empty());
  }

  encryptionGroups.clear();
  {
    EncodingIter iter{footer, encryptionGroups};
    ASSERT_TRUE(iter.empty());
  }
}

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
