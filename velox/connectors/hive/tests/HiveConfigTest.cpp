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

#include "velox/connectors/hive/HiveConfig.h"
#include "gtest/gtest.h"
#include "velox/core/Config.h"

using namespace facebook::velox::connector::hive;
using namespace facebook::velox::core;
using facebook::velox::connector::hive::HiveConfig;

TEST(HiveConfigTest, defaultConfig) {
  HiveConfig hiveConfig(std::make_shared<MemConfig>());
  const auto emptySession = std::make_unique<MemConfig>();
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(emptySession.get()),
      facebook::velox::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kError);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(emptySession.get()), 100);
  ASSERT_EQ(hiveConfig.immutablePartitions(), false);
  ASSERT_EQ(hiveConfig.s3UseVirtualAddressing(), true);
  ASSERT_EQ(hiveConfig.s3GetLogLevel(), "FATAL");
  ASSERT_EQ(hiveConfig.s3UseSSL(), true);
  ASSERT_EQ(hiveConfig.s3UseInstanceCredentials(), false);
  ASSERT_EQ(hiveConfig.s3Endpoint(), "");
  ASSERT_EQ(hiveConfig.s3AccessKey(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3SecretKey(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3IAMRole(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3IAMRoleSessionName(), "velox-session");
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "");
  ASSERT_EQ(hiveConfig.gcsScheme(), "https");
  ASSERT_EQ(hiveConfig.gcsCredentials(), "");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(emptySession.get()), false);
  ASSERT_EQ(
      hiveConfig.isFileColumnNamesReadAsLowerCase(emptySession.get()), false);

  ASSERT_EQ(hiveConfig.maxCoalescedBytes(), 128 << 20);
  ASSERT_EQ(hiveConfig.maxCoalescedDistanceBytes(), 512 << 10);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 20'000);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), true);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(emptySession.get()),
      64L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(emptySession.get()),
      16L * 1024L * 1024L);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(emptySession.get()), 1024);
  ASSERT_EQ(
      hiveConfig.sortWriterMaxOutputBytes(emptySession.get()), 10UL << 20);
  ASSERT_EQ(hiveConfig.isPartitionPathAsLowerCase(emptySession.get()), true);
  ASSERT_EQ(hiveConfig.orcWriterMinCompressionSize(emptySession.get()), 1024);
  ASSERT_EQ(
      hiveConfig.orcWriterCompressionLevel(emptySession.get()), std::nullopt);
  ASSERT_EQ(
      hiveConfig.orcWriterLinearStripeSizeHeuristics(emptySession.get()), true);
  ASSERT_FALSE(hiveConfig.cacheNoRetention(emptySession.get()));
}

TEST(HiveConfigTest, overrideConfig) {
  const std::unordered_map<std::string, std::string> configFromFile = {
      {HiveConfig::kInsertExistingPartitionsBehavior, "OVERWRITE"},
      {HiveConfig::kMaxPartitionsPerWriters, "120"},
      {HiveConfig::kImmutablePartitions, "true"},
      {HiveConfig::kS3PathStyleAccess, "true"},
      {HiveConfig::kS3LogLevel, "Warning"},
      {HiveConfig::kS3SSLEnabled, "false"},
      {HiveConfig::kS3UseInstanceCredentials, "true"},
      {HiveConfig::kS3Endpoint, "hey"},
      {HiveConfig::kS3AwsAccessKey, "hello"},
      {HiveConfig::kS3AwsSecretKey, "hello"},
      {HiveConfig::kS3IamRole, "hello"},
      {HiveConfig::kS3IamRoleSessionName, "velox"},
      {HiveConfig::kGCSEndpoint, "hey"},
      {HiveConfig::kGCSScheme, "http"},
      {HiveConfig::kGCSCredentials, "hey"},
      {HiveConfig::kOrcUseColumnNames, "true"},
      {HiveConfig::kFileColumnNamesReadAsLowerCase, "true"},
      {HiveConfig::kMaxCoalescedBytes, "100"},
      {HiveConfig::kMaxCoalescedDistanceBytes, "100"},
      {HiveConfig::kNumCacheFileHandles, "100"},
      {HiveConfig::kEnableFileHandleCache, "false"},
      {HiveConfig::kOrcWriterMaxStripeSize, "100MB"},
      {HiveConfig::kOrcWriterMaxDictionaryMemory, "100MB"},
      {HiveConfig::kSortWriterMaxOutputRows, "100"},
      {HiveConfig::kSortWriterMaxOutputBytes, "100MB"},
      {HiveConfig::kOrcWriterLinearStripeSizeHeuristics, "false"},
      {HiveConfig::kOrcWriterMinCompressionSize, "512"},
      {HiveConfig::kOrcWriterCompressionLevel, "1"},
      {HiveConfig::kCacheNoRetention, "true"}};
  HiveConfig hiveConfig(std::make_shared<MemConfig>(configFromFile));
  auto emptySession = std::make_unique<MemConfig>();
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(emptySession.get()),
      facebook::velox::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kOverwrite);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(emptySession.get()), 120);
  ASSERT_EQ(hiveConfig.immutablePartitions(), true);
  ASSERT_EQ(hiveConfig.s3UseVirtualAddressing(), false);
  ASSERT_EQ(hiveConfig.s3GetLogLevel(), "Warning");
  ASSERT_EQ(hiveConfig.s3UseSSL(), false);
  ASSERT_EQ(hiveConfig.s3UseInstanceCredentials(), true);
  ASSERT_EQ(hiveConfig.s3Endpoint(), "hey");
  ASSERT_EQ(hiveConfig.s3AccessKey(), std::optional("hello"));
  ASSERT_EQ(hiveConfig.s3SecretKey(), std::optional("hello"));
  ASSERT_EQ(hiveConfig.s3IAMRole(), std::optional("hello"));
  ASSERT_EQ(hiveConfig.s3IAMRoleSessionName(), "velox");
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "hey");
  ASSERT_EQ(hiveConfig.gcsScheme(), "http");
  ASSERT_EQ(hiveConfig.gcsCredentials(), "hey");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(emptySession.get()), true);
  ASSERT_EQ(
      hiveConfig.isFileColumnNamesReadAsLowerCase(emptySession.get()), true);
  ASSERT_EQ(hiveConfig.maxCoalescedBytes(), 100);
  ASSERT_EQ(hiveConfig.maxCoalescedDistanceBytes(), 100);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 100);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), false);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(emptySession.get()),
      100L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(emptySession.get()),
      100L * 1024L * 1024L);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(emptySession.get()), 100);
  ASSERT_EQ(
      hiveConfig.sortWriterMaxOutputBytes(emptySession.get()), 100UL << 20);
  ASSERT_EQ(hiveConfig.orcWriterMinCompressionSize(emptySession.get()), 512);
  ASSERT_EQ(hiveConfig.orcWriterCompressionLevel(emptySession.get()), 1);
  ASSERT_EQ(
      hiveConfig.orcWriterLinearStripeSizeHeuristics(emptySession.get()),
      false);
  ASSERT_TRUE(hiveConfig.cacheNoRetention(emptySession.get()));
}

TEST(HiveConfigTest, overrideSession) {
  HiveConfig hiveConfig(std::make_shared<MemConfig>());
  const std::unordered_map<std::string, std::string> sessionOverride = {
      {HiveConfig::kInsertExistingPartitionsBehaviorSession, "OVERWRITE"},
      {HiveConfig::kOrcUseColumnNamesSession, "true"},
      {HiveConfig::kFileColumnNamesReadAsLowerCaseSession, "true"},
      {HiveConfig::kOrcWriterMaxStripeSizeSession, "22MB"},
      {HiveConfig::kOrcWriterMaxDictionaryMemorySession, "22MB"},
      {HiveConfig::kSortWriterMaxOutputRowsSession, "20"},
      {HiveConfig::kSortWriterMaxOutputBytesSession, "20MB"},
      {HiveConfig::kPartitionPathAsLowerCaseSession, "false"},
      {HiveConfig::kIgnoreMissingFilesSession, "true"},
      {HiveConfig::kOrcWriterMinCompressionSizeSession, "512"},
      {HiveConfig::kOrcWriterCompressionLevelSession, "1"},
      {HiveConfig::kOrcWriterLinearStripeSizeHeuristicsSession, "false"},
      {HiveConfig::kCacheNoRetentionSession, "true"}};
  const auto session = std::make_unique<MemConfig>(sessionOverride);
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(session.get()),
      facebook::velox::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kOverwrite);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(session.get()), 100);
  ASSERT_EQ(hiveConfig.immutablePartitions(), false);
  ASSERT_EQ(hiveConfig.s3UseVirtualAddressing(), true);
  ASSERT_EQ(hiveConfig.s3GetLogLevel(), "FATAL");
  ASSERT_EQ(hiveConfig.s3UseSSL(), true);
  ASSERT_EQ(hiveConfig.s3UseInstanceCredentials(), false);
  ASSERT_EQ(hiveConfig.s3Endpoint(), "");
  ASSERT_EQ(hiveConfig.s3AccessKey(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3SecretKey(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3IAMRole(), std::nullopt);
  ASSERT_EQ(hiveConfig.s3IAMRoleSessionName(), "velox-session");
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "");
  ASSERT_EQ(hiveConfig.gcsScheme(), "https");
  ASSERT_EQ(hiveConfig.gcsCredentials(), "");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(session.get()), true);
  ASSERT_EQ(hiveConfig.isFileColumnNamesReadAsLowerCase(session.get()), true);

  ASSERT_EQ(hiveConfig.maxCoalescedBytes(), 128 << 20);
  ASSERT_EQ(hiveConfig.maxCoalescedDistanceBytes(), 512 << 10);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 20'000);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), true);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(session.get()), 22L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(session.get()),
      22L * 1024L * 1024L);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(session.get()), 20);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputBytes(session.get()), 20UL << 20);
  ASSERT_EQ(hiveConfig.isPartitionPathAsLowerCase(session.get()), false);
  ASSERT_EQ(hiveConfig.ignoreMissingFiles(session.get()), true);
  ASSERT_EQ(
      hiveConfig.orcWriterLinearStripeSizeHeuristics(session.get()), false);
  ASSERT_EQ(hiveConfig.orcWriterMinCompressionSize(session.get()), 512);
  ASSERT_EQ(hiveConfig.orcWriterCompressionLevel(session.get()), 1);
  ASSERT_TRUE(hiveConfig.cacheNoRetention(session.get()));
}
