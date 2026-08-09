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
#include "presto_cpp/main/common/LegacyHiveConfigKeys.h"

#include <gtest/gtest.h>
#include <string>
#include <unordered_map>

namespace facebook::presto::util {
namespace {

using ConfigMap = std::unordered_map<std::string, std::string>;

TEST(LegacyHiveConfigKeysTest, configRename) {
  ConfigMap configs{
      {"parquet.use-column-names", "true"},
      {"parquet.allow-int32-narrowing", "true"},
      {"parquet.footer-speculative-io-size", "1048576"},
      {"parquet-footer-memory-tracking-threshold", "2048"},
      {"connector.name", "hive"},
  };
  migrateLegacyHiveParquetKeys("hive", configs);

  EXPECT_EQ(configs["hive.parquet.use-column-names"], "true");
  EXPECT_EQ(configs["hive.parquet.allow-int32-narrowing"], "true");
  EXPECT_EQ(configs["hive.parquet.footer-speculative-io-size"], "1048576");
  EXPECT_EQ(configs["hive.parquet.footer-memory-tracking-threshold"], "2048");
  EXPECT_EQ(configs.count("parquet.use-column-names"), 0);
  EXPECT_EQ(configs.count("parquet.allow-int32-narrowing"), 0);
  EXPECT_EQ(configs.count("parquet.footer-speculative-io-size"), 0);
  EXPECT_EQ(configs.count("parquet-footer-memory-tracking-threshold"), 0);
}

TEST(LegacyHiveConfigKeysTest, configMaxTargetFileSizeFanOut) {
  ConfigMap configs{{"max-target-file-size", "1MB"}};
  migrateLegacyHiveParquetKeys("hive", configs);

  EXPECT_EQ(configs["hive.parquet.writer.max-target-file-size"], "1MB");
  EXPECT_EQ(configs["hive.orc.writer.max-target-file-size"], "1MB");
  EXPECT_EQ(configs["hive.nimble.writer.max-target-file-size"], "1MB");
  EXPECT_EQ(configs.count("max-target-file-size"), 0);
}

TEST(LegacyHiveConfigKeysTest, configIcebergGate) {
  ConfigMap configs{{"max-target-file-size", "2MB"}};
  migrateLegacyHiveParquetKeys("iceberg", configs);

  EXPECT_EQ(configs["hive.parquet.writer.max-target-file-size"], "2MB");
  EXPECT_EQ(configs.count("max-target-file-size"), 0);
}

TEST(LegacyHiveConfigKeysTest, configNonHiveConnectorNoOp) {
  ConfigMap configs{
      {"max-target-file-size", "1MB"},
      {"parquet.use-column-names", "true"},
  };
  migrateLegacyHiveParquetKeys("tpch", configs);

  EXPECT_EQ(configs["max-target-file-size"], "1MB");
  EXPECT_EQ(configs["parquet.use-column-names"], "true");
  EXPECT_EQ(configs.count("hive.parquet.writer.max-target-file-size"), 0);
  EXPECT_EQ(configs.count("hive.parquet.use-column-names"), 0);
}

TEST(LegacyHiveConfigKeysTest, configNewKeyWins) {
  ConfigMap configs{
      {"parquet.use-column-names", "true"},
      {"hive.parquet.use-column-names", "false"},
      {"max-target-file-size", "1MB"},
      {"hive.orc.writer.max-target-file-size", "8MB"},
  };
  migrateLegacyHiveParquetKeys("hive", configs);

  EXPECT_EQ(configs["hive.parquet.use-column-names"], "false");
  EXPECT_EQ(configs.count("parquet.use-column-names"), 0);
  EXPECT_EQ(configs["hive.parquet.writer.max-target-file-size"], "1MB");
  EXPECT_EQ(configs["hive.orc.writer.max-target-file-size"], "8MB");
  EXPECT_EQ(configs["hive.nimble.writer.max-target-file-size"], "1MB");
  EXPECT_EQ(configs.count("max-target-file-size"), 0);
}

TEST(LegacyHiveConfigKeysTest, sessionRename) {
  ConfigMap session{{"allow_int32_narrowing", "true"}};
  migrateLegacyHiveParquetSessionKeys(session);

  EXPECT_EQ(session["parquet_allow_int32_narrowing"], "true");
  EXPECT_EQ(session.count("allow_int32_narrowing"), 0);
}

TEST(LegacyHiveConfigKeysTest, sessionMaxTargetFileSizeFanOut) {
  ConfigMap session{{"max_target_file_size", "4KB"}};
  migrateLegacyHiveParquetSessionKeys(session);

  EXPECT_EQ(session["parquet_writer_max_target_file_size"], "4KB");
  EXPECT_EQ(session["orc_writer_max_target_file_size"], "4KB");
  EXPECT_EQ(session["nimble_writer_max_target_file_size"], "4KB");
  EXPECT_EQ(session.count("max_target_file_size"), 0);
}

TEST(LegacyHiveConfigKeysTest, sessionNoLegacyKeysIsNoOp) {
  ConfigMap session{
      {"parquet_use_column_names", "true"},
      {"parquet_writer_max_target_file_size", "8KB"},
  };
  const ConfigMap before = session;
  migrateLegacyHiveParquetSessionKeys(session);
  EXPECT_EQ(session, before);
}

TEST(LegacyHiveConfigKeysTest, sessionNewKeyWins) {
  ConfigMap session{
      {"max_target_file_size", "4KB"},
      {"orc_writer_max_target_file_size", "16KB"},
      {"allow_int32_narrowing", "true"},
      {"parquet_allow_int32_narrowing", "false"},
  };
  migrateLegacyHiveParquetSessionKeys(session);

  EXPECT_EQ(session["parquet_writer_max_target_file_size"], "4KB");
  EXPECT_EQ(session["orc_writer_max_target_file_size"], "16KB");
  EXPECT_EQ(session["nimble_writer_max_target_file_size"], "4KB");
  EXPECT_EQ(session.count("max_target_file_size"), 0);
  EXPECT_EQ(session["parquet_allow_int32_narrowing"], "false");
  EXPECT_EQ(session.count("allow_int32_narrowing"), 0);
}

} // namespace
} // namespace facebook::presto::util
