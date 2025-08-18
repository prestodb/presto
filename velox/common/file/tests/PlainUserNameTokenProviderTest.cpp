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

#include "velox/common/file/PlainUserNameTokenProvider.h"
#include <gtest/gtest.h>
#include "velox/common/file/TokenProvider.h"

using namespace ::testing;
namespace facebook::velox::filesystems {
TEST(PlainUserNameTokenProviderTest, testTokenProviderUserName) {
  filesystems::AccessTokenKey key;
  auto tokenProvider =
      std::make_shared<PlainUserNameTokenProvider>("test_user");
  auto baseToken = tokenProvider->getToken(key);
  auto userToken =
      std::dynamic_pointer_cast<PlainUserNameAccessToken>(baseToken);
  ASSERT_EQ(userToken->getUser(), "test_user");
}

TEST(PlainUserNameTokenProviderTest, testTokenProviderEquals) {
  auto tokenProvider1 =
      std::make_shared<PlainUserNameTokenProvider>("test_user_1");
  auto tokenProvider2 = tokenProvider1;
  auto tokenProvider3 =
      std::make_shared<PlainUserNameTokenProvider>("test_user_1");
  auto tokenProvider4 =
      std::make_shared<PlainUserNameTokenProvider>("test_user_2");
  ASSERT_TRUE(tokenProvider1->equals(*tokenProvider2));
  ASSERT_TRUE(tokenProvider1->equals(*tokenProvider3));
  ASSERT_FALSE(tokenProvider1->equals(*tokenProvider4));
}

TEST(PlainUserNameTokenProviderTest, testTokenProviderHash) {
  auto tokenProvider1 =
      std::make_shared<PlainUserNameTokenProvider>("test_user_1");
  auto tokenProvider2 =
      std::make_shared<PlainUserNameTokenProvider>("test_user_1");
  auto tokenProvider3 =
      std::make_shared<PlainUserNameTokenProvider>("test_user_2");
  ASSERT_EQ(tokenProvider1->hash(), tokenProvider2->hash());
  ASSERT_NE(tokenProvider1->hash(), tokenProvider3->hash());
}
} // namespace facebook::velox::filesystems
