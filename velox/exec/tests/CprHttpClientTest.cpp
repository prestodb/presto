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

#include "velox/common/base/tests/GTestUtils.h"

#include <cpr/cpr.h>
#include <map>

class CprHttpClientTest : public testing::Test {};

// This test requires open access to internet and most places test runners might
// be closed off from the general internet. And this test case is just an
// illustration of how to use cpr, so disable it by default.
TEST_F(CprHttpClientTest, DISABLED_basic) {
  auto response = cpr::Get(
      cpr::Url{"https://facebookincubator.github.io/velox/"},
      cpr::Timeout{std::chrono::seconds{3}});
  ASSERT_EQ(response.status_code, 200);
  ASSERT_FALSE(response.text.empty());

  response = cpr::Get(cpr::Url{"null"});
  ASSERT_NE(response.status_code, 200);
  ASSERT_TRUE(response.text.empty());

  response = cpr::Post(
      cpr::Url{"https://facebookincubator.github.io/velox/"},
      cpr::Body{"select * from nation limit 1"},
      cpr::Header({{"Content-Type", "text/plain"}}));
  ASSERT_EQ(response.status_code, 405);
  ASSERT_FALSE(response.text.empty());

  response = cpr::Post(
      cpr::Url{"null"},
      cpr::Body{"select * from nation limit 1"},
      cpr::Header({{"Content-Type", "text/plain"}}));
  ASSERT_NE(response.status_code, 200);
  ASSERT_TRUE(response.text.empty());
}
