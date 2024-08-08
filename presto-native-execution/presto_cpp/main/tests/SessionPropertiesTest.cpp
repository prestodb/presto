/*
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

#include "presto_cpp/main/SessionProperties.h"

using namespace facebook::presto;

class SessionPropertiesTest : public testing::Test {};

TEST_F(SessionPropertiesTest, validateMapping) {
  SessionProperties sessionProperties;
  const std::vector<std::string> names = {
      "legacy_timestamp",
      "driver_cpu_time_slice_limit_ms",
      "native_spill_compression_codec"};
  const std::vector<std::shared_ptr<SessionProperty>> expectedSessionProps = {
      std::make_shared<SessionProperty>(
          "legacy_timestamp", "adjust_timestamp_to_session_timezone", "true"),
      std::make_shared<SessionProperty>(
          "driver_cpu_time_slice_limit_ms",
          "driver_cpu_time_slice_limit_ms",
          "1000"),
      std::make_shared<SessionProperty>(
          "native_spill_compression_codec", "spill_compression_codec", "none")};

  for (auto i = 0; i < names.size(); i++) {
    auto property = sessionProperties.getSessionProperty(names[i]);
    EXPECT_EQ(*expectedSessionProps[i], *property);
  }
}
