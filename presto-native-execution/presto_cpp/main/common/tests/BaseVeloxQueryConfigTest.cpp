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
#include "presto_cpp/main/common/Configs.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/QueryConfig.h"

namespace facebook::presto::test {

using namespace velox;

class BaseVeloxQueryConfigTest : public testing::Test {
 protected:
  void setUpSystemConfig(bool isMutable) {
    std::unordered_map<std::string, std::string> properties;
    if (isMutable) {
      properties[std::string{SystemConfig::kMutableConfig}] = "true";
    }

    SystemConfig::instance()->initialize(
        std::make_unique<core::MemConfig>(std::move(properties)));

    propName = std::string{core::QueryConfig::kSessionTimezone};
  }

  std::string propName;
};

TEST_F(BaseVeloxQueryConfigTest, defaultConfig) {
  setUpSystemConfig(false);
  auto cfg = std::make_unique<BaseVeloxQueryConfig>();

  ASSERT_FALSE(cfg->isMutable());
  ASSERT_FALSE(cfg->getValue(propName).has_value());
  ASSERT_THROW(cfg->setValue(propName, "TZ1"), VeloxException);
}

TEST_F(BaseVeloxQueryConfigTest, mutableConfig) {
  setUpSystemConfig(true);
  auto cfg = std::make_unique<BaseVeloxQueryConfig>();

  ASSERT_TRUE(cfg->isMutable());
  ASSERT_FALSE(cfg->getValue(propName).has_value());
  auto ret = cfg->setValue(propName, "TZ1");
  ASSERT_EQ(folly::Optional<std::string>{}, ret);
  ASSERT_EQ(folly::Optional<std::string>{"TZ1"}, ret = cfg->getValue(propName));
}

} // namespace facebook::presto::test
