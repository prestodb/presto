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
#include "presto_cpp/main/common/JavaWriterConfigKeys.h"

#include <gtest/gtest.h>
#include <string>
#include <unordered_map>

namespace facebook::presto::util {
namespace {

using ConfigMap = std::unordered_map<std::string, std::string>;

TEST(JavaWriterConfigKeysTest, iceberg) {
  ConfigMap configs{
      {"iceberg.target-max-file-size", "512MB"},
      {"hive.orc.writer.max-target-file-size", "1GB"},
      {"hive.parquet.writer.block-size", "64MB"},
  };
  mapJavaWriterConnectorConfigs("iceberg", configs);

  EXPECT_EQ(configs["hive.parquet.writer.max-target-file-size"], "512MB");
  EXPECT_EQ(configs["hive.nimble.writer.max-target-file-size"], "512MB");
  // An explicitly set Velox key wins.
  EXPECT_EQ(configs["hive.orc.writer.max-target-file-size"], "1GB");
  EXPECT_EQ(configs["hive.parquet.writer.row-group-size"], "64MB");
  // The Java keys stay for anything else that reads them.
  EXPECT_EQ(configs["iceberg.target-max-file-size"], "512MB");
  EXPECT_EQ(configs["hive.parquet.writer.block-size"], "64MB");
}

TEST(JavaWriterConfigKeysTest, hive) {
  ConfigMap configs{
      {"iceberg.target-max-file-size", "512MB"},
      {"hive.parquet.writer.block-size", "64MB"},
  };
  mapJavaWriterConnectorConfigs("hive", configs);

  EXPECT_EQ(configs["hive.parquet.writer.row-group-size"], "64MB");
  // Hive has no iceberg.target-max-file-size.
  EXPECT_EQ(configs.count("hive.parquet.writer.max-target-file-size"), 0);

  // An explicitly set Velox key wins.
  ConfigMap explicitConfigs{
      {"hive.parquet.writer.block-size", "64MB"},
      {"hive.parquet.writer.row-group-size", "32MB"},
  };
  mapJavaWriterConnectorConfigs("hive", explicitConfigs);
  EXPECT_EQ(explicitConfigs["hive.parquet.writer.row-group-size"], "32MB");
}

TEST(JavaWriterConfigKeysTest, otherConnectorsAreUnchanged) {
  const ConfigMap original{
      {"iceberg.target-max-file-size", "512MB"},
      {"hive.parquet.writer.block-size", "64MB"},
  };
  ConfigMap configs = original;
  mapJavaWriterConnectorConfigs("tpch", configs);
  EXPECT_EQ(configs, original);
}

} // namespace
} // namespace facebook::presto::util
