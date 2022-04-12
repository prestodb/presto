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
#include "velox/dwio/common/MetricsLog.h"

using namespace facebook::velox::dwio::common;

namespace {
class TestingDwioMetricsLog : public MetricsLog {
 public:
  TestingDwioMetricsLog(const std::string& filePath, bool ioLogging)
      : MetricsLog(filePath) {
    ioLogging_ = ioLogging;
  }

  const std::string& filePath() const {
    return file_;
  }

  bool ioLogging() const {
    return ioLogging_;
  }

  void logRead(const ReadMetrics& /* unused */) const override {}

  void logColumnFilter(
      const ColumnFilter& /* unused */,
      uint64_t /* unused */,
      uint64_t /* unused */,
      bool /* unused */) const override {}

  void logMapKeyFilter(
      const FilterNode& /* unused */,
      std::string_view /* unused */) const override {}

  void logWrite(
      const std::string& /* unused */,
      size_t /* unused */,
      size_t /* unused */) const override {}

  void logStripeFlush(const StripeFlushMetrics& /* unused */) const override {}

  void logFileClose(const FileCloseMetrics& /* unused */) const override {}

 private:
  bool ioLogging_;
};

class TestingDwioMetricsLogFactory : public DwioMetricsLogFactory {
  MetricsLogPtr create(const std::string& filePath, bool ioLogging) override {
    return std::make_shared<TestingDwioMetricsLog>(filePath, ioLogging);
  }
};
} // namespace

TEST(DwioMetricsLogFactoryTest, basic) {
  auto defaultLog = getMetricsLogFactory().create("path/to/data.orc", false);
  ASSERT_TRUE(defaultLog != nullptr);
  registerMetricsLogFactory(std::make_shared<TestingDwioMetricsLogFactory>());

  {
    auto customLog = getMetricsLogFactory().create("path/to/data.orc", false);
    ASSERT_TRUE(customLog != nullptr);
    auto testingLog =
        std::dynamic_pointer_cast<const TestingDwioMetricsLog>(customLog);
    ASSERT_TRUE(testingLog != nullptr);
    ASSERT_EQ("path/to/data.orc", testingLog->filePath());
    ASSERT_FALSE(testingLog->ioLogging());
  }

  {
    auto customLog = getMetricsLogFactory().create("path/to/data2.orc", true);
    ASSERT_TRUE(customLog != nullptr);
    auto testingLog =
        std::dynamic_pointer_cast<const TestingDwioMetricsLog>(customLog);
    ASSERT_TRUE(testingLog != nullptr);
    ASSERT_EQ("path/to/data2.orc", testingLog->filePath());
    ASSERT_TRUE(testingLog->ioLogging());
  }
}
