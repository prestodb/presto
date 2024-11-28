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
#include "velox/common/config/Config.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;
using facebook::velox::connector::hive::HiveConfig;

TEST(HiveConfigTest, defaultConfig) {
  HiveConfig hiveConfig(std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>()));
  const auto emptySession = std::make_unique<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(emptySession.get()),
      facebook::velox::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kError);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(emptySession.get()), 100);
  ASSERT_EQ(hiveConfig.immutablePartitions(), false);
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "");
  ASSERT_EQ(hiveConfig.gcsCredentialsPath(), "");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(emptySession.get()), false);
  ASSERT_EQ(
      hiveConfig.isFileColumnNamesReadAsLowerCase(emptySession.get()), false);

  ASSERT_EQ(hiveConfig.maxCoalescedBytes(), 128 << 20);
  ASSERT_EQ(
      hiveConfig.maxCoalescedDistanceBytes(emptySession.get()), 512 << 10);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 20'000);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), true);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(emptySession.get()),
      64L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(emptySession.get()),
      16L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.isOrcWriterIntegerDictionaryEncodingEnabled(
          emptySession.get()),
      true);
  ASSERT_EQ(
      hiveConfig.isOrcWriterStringDictionaryEncodingEnabled(emptySession.get()),
      true);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(emptySession.get()), 1024);
  ASSERT_EQ(
      hiveConfig.sortWriterMaxOutputBytes(emptySession.get()), 10UL << 20);
  ASSERT_EQ(
      hiveConfig.sortWriterFinishTimeSliceLimitMs(emptySession.get()), 5'000);
  ASSERT_EQ(hiveConfig.isPartitionPathAsLowerCase(emptySession.get()), true);
  ASSERT_EQ(hiveConfig.allowNullPartitionKeys(emptySession.get()), true);
  ASSERT_EQ(hiveConfig.orcWriterMinCompressionSize(emptySession.get()), 1024);
  ASSERT_EQ(
      hiveConfig.orcWriterCompressionLevel(emptySession.get()), std::nullopt);
  ASSERT_EQ(
      hiveConfig.orcWriterLinearStripeSizeHeuristics(emptySession.get()), true);
  ASSERT_FALSE(hiveConfig.cacheNoRetention(emptySession.get()));
}

TEST(HiveConfigTest, overrideConfig) {
  std::unordered_map<std::string, std::string> configFromFile = {
      {HiveConfig::kInsertExistingPartitionsBehavior, "OVERWRITE"},
      {HiveConfig::kMaxPartitionsPerWriters, "120"},
      {HiveConfig::kImmutablePartitions, "true"},
      {HiveConfig::kGcsEndpoint, "hey"},
      {HiveConfig::kGcsCredentialsPath, "hey"},
      {HiveConfig::kOrcUseColumnNames, "true"},
      {HiveConfig::kFileColumnNamesReadAsLowerCase, "true"},
      {HiveConfig::kAllowNullPartitionKeys, "false"},
      {HiveConfig::kMaxCoalescedBytes, "100"},
      {HiveConfig::kMaxCoalescedDistance, "100kB"},
      {HiveConfig::kNumCacheFileHandles, "100"},
      {HiveConfig::kEnableFileHandleCache, "false"},
      {HiveConfig::kOrcWriterMaxStripeSize, "100MB"},
      {HiveConfig::kOrcWriterMaxDictionaryMemory, "100MB"},
      {HiveConfig::kOrcWriterIntegerDictionaryEncodingEnabled, "false"},
      {HiveConfig::kOrcWriterStringDictionaryEncodingEnabled, "false"},
      {HiveConfig::kSortWriterMaxOutputRows, "100"},
      {HiveConfig::kSortWriterMaxOutputBytes, "100MB"},
      {HiveConfig::kSortWriterFinishTimeSliceLimitMs, "400"},
      {HiveConfig::kOrcWriterLinearStripeSizeHeuristics, "false"},
      {HiveConfig::kOrcWriterMinCompressionSize, "512"},
      {HiveConfig::kOrcWriterCompressionLevel, "1"},
      {HiveConfig::kCacheNoRetention, "true"}};
  HiveConfig hiveConfig(
      std::make_shared<config::ConfigBase>(std::move(configFromFile)));
  auto emptySession = std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>());
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(emptySession.get()),
      facebook::velox::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kOverwrite);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(emptySession.get()), 120);
  ASSERT_EQ(hiveConfig.immutablePartitions(), true);
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "hey");
  ASSERT_EQ(hiveConfig.gcsCredentialsPath(), "hey");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(emptySession.get()), true);
  ASSERT_EQ(
      hiveConfig.isFileColumnNamesReadAsLowerCase(emptySession.get()), true);
  ASSERT_EQ(hiveConfig.allowNullPartitionKeys(emptySession.get()), false);
  ASSERT_EQ(hiveConfig.maxCoalescedBytes(), 100);
  ASSERT_EQ(
      hiveConfig.maxCoalescedDistanceBytes(emptySession.get()), 100 << 10);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 100);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), false);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(emptySession.get()),
      100L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(emptySession.get()),
      100L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.isOrcWriterIntegerDictionaryEncodingEnabled(
          emptySession.get()),
      false);
  ASSERT_EQ(
      hiveConfig.isOrcWriterStringDictionaryEncodingEnabled(emptySession.get()),
      false);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(emptySession.get()), 100);
  ASSERT_EQ(
      hiveConfig.sortWriterMaxOutputBytes(emptySession.get()), 100UL << 20);
  ASSERT_EQ(
      hiveConfig.sortWriterFinishTimeSliceLimitMs(emptySession.get()), 400);
  ASSERT_EQ(hiveConfig.orcWriterMinCompressionSize(emptySession.get()), 512);
  ASSERT_EQ(hiveConfig.orcWriterCompressionLevel(emptySession.get()), 1);
  ASSERT_EQ(
      hiveConfig.orcWriterLinearStripeSizeHeuristics(emptySession.get()),
      false);
  ASSERT_TRUE(hiveConfig.cacheNoRetention(emptySession.get()));
}

TEST(HiveConfigTest, overrideSession) {
  HiveConfig hiveConfig(std::make_shared<config::ConfigBase>(
      std::unordered_map<std::string, std::string>()));
  std::unordered_map<std::string, std::string> sessionOverride = {
      {HiveConfig::kInsertExistingPartitionsBehaviorSession, "OVERWRITE"},
      {HiveConfig::kOrcUseColumnNamesSession, "true"},
      {HiveConfig::kFileColumnNamesReadAsLowerCaseSession, "true"},
      {HiveConfig::kOrcWriterMaxStripeSizeSession, "22MB"},
      {HiveConfig::kOrcWriterMaxDictionaryMemorySession, "22MB"},
      {HiveConfig::kOrcWriterIntegerDictionaryEncodingEnabledSession, "false"},
      {HiveConfig::kOrcWriterStringDictionaryEncodingEnabledSession, "false"},
      {HiveConfig::kSortWriterMaxOutputRowsSession, "20"},
      {HiveConfig::kSortWriterMaxOutputBytesSession, "20MB"},
      {HiveConfig::kMaxCoalescedDistanceSession, "3MB"},
      {HiveConfig::kSortWriterFinishTimeSliceLimitMsSession, "300"},
      {HiveConfig::kPartitionPathAsLowerCaseSession, "false"},
      {HiveConfig::kAllowNullPartitionKeysSession, "false"},
      {HiveConfig::kIgnoreMissingFilesSession, "true"},
      {HiveConfig::kOrcWriterMinCompressionSizeSession, "512"},
      {HiveConfig::kOrcWriterCompressionLevelSession, "1"},
      {HiveConfig::kOrcWriterLinearStripeSizeHeuristicsSession, "false"},
      {HiveConfig::kCacheNoRetentionSession, "true"}};
  const auto session =
      std::make_unique<config::ConfigBase>(std::move(sessionOverride));
  ASSERT_EQ(
      hiveConfig.insertExistingPartitionsBehavior(session.get()),
      facebook::velox::connector::hive::HiveConfig::
          InsertExistingPartitionsBehavior::kOverwrite);
  ASSERT_EQ(hiveConfig.maxPartitionsPerWriters(session.get()), 100);
  ASSERT_EQ(hiveConfig.immutablePartitions(), false);
  ASSERT_EQ(hiveConfig.gcsEndpoint(), "");
  ASSERT_EQ(hiveConfig.gcsCredentialsPath(), "");
  ASSERT_EQ(hiveConfig.isOrcUseColumnNames(session.get()), true);
  ASSERT_EQ(hiveConfig.isFileColumnNamesReadAsLowerCase(session.get()), true);

  ASSERT_EQ(hiveConfig.maxCoalescedBytes(), 128 << 20);
  ASSERT_EQ(hiveConfig.maxCoalescedDistanceBytes(session.get()), 3 << 20);
  ASSERT_EQ(hiveConfig.numCacheFileHandles(), 20'000);
  ASSERT_EQ(hiveConfig.isFileHandleCacheEnabled(), true);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxStripeSize(session.get()), 22L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.orcWriterMaxDictionaryMemory(session.get()),
      22L * 1024L * 1024L);
  ASSERT_EQ(
      hiveConfig.isOrcWriterIntegerDictionaryEncodingEnabled(session.get()),
      false);
  ASSERT_EQ(
      hiveConfig.isOrcWriterStringDictionaryEncodingEnabled(session.get()),
      false);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputRows(session.get()), 20);
  ASSERT_EQ(hiveConfig.sortWriterMaxOutputBytes(session.get()), 20UL << 20);
  ASSERT_EQ(hiveConfig.sortWriterFinishTimeSliceLimitMs(session.get()), 300);
  ASSERT_EQ(hiveConfig.isPartitionPathAsLowerCase(session.get()), false);
  ASSERT_EQ(hiveConfig.allowNullPartitionKeys(session.get()), false);
  ASSERT_EQ(hiveConfig.ignoreMissingFiles(session.get()), true);
  ASSERT_EQ(
      hiveConfig.orcWriterLinearStripeSizeHeuristics(session.get()), false);
  ASSERT_EQ(hiveConfig.orcWriterMinCompressionSize(session.get()), 512);
  ASSERT_EQ(hiveConfig.orcWriterCompressionLevel(session.get()), 1);
  ASSERT_TRUE(hiveConfig.cacheNoRetention(session.get()));
}
