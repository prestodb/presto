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
#include <folly/init/Init.h>

#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Counters.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3WriteFile.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/S3Test.h"

#include <gtest/gtest.h>

namespace facebook::velox::filesystems {
namespace {
class S3TestReporter : public BaseStatsReporter {
 public:
  mutable std::mutex m;
  mutable std::map<std::string, size_t> counterMap;
  mutable std::unordered_map<std::string, StatType> statTypeMap;
  mutable std::unordered_map<std::string, std::vector<int32_t>>
      histogramPercentilesMap;

  void clear() {
    std::lock_guard<std::mutex> l(m);
    counterMap.clear();
    statTypeMap.clear();
    histogramPercentilesMap.clear();
  }
  void registerMetricExportType(const char* key, StatType statType)
      const override {
    statTypeMap[key] = statType;
  }

  void registerMetricExportType(folly::StringPiece key, StatType statType)
      const override {
    statTypeMap[key.str()] = statType;
  }

  void registerHistogramMetricExportType(
      const char* key,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& pcts) const override {
    histogramPercentilesMap[key] = pcts;
  }

  void registerHistogramMetricExportType(
      folly::StringPiece key,
      int64_t /* bucketWidth */,
      int64_t /* min */,
      int64_t /* max */,
      const std::vector<int32_t>& pcts) const override {
    histogramPercentilesMap[key.str()] = pcts;
  }

  void addMetricValue(const std::string& key, const size_t value)
      const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] += value;
  }

  void addMetricValue(const char* key, const size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key] += value;
  }

  void addMetricValue(folly::StringPiece key, size_t value) const override {
    std::lock_guard<std::mutex> l(m);
    counterMap[key.str()] += value;
  }

  void addHistogramMetricValue(const std::string& key, size_t value)
      const override {
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramMetricValue(const char* key, size_t value) const override {
    counterMap[key] = std::max(counterMap[key], value);
  }

  void addHistogramMetricValue(folly::StringPiece key, size_t value)
      const override {
    counterMap[key.str()] = std::max(counterMap[key.str()], value);
  }

  std::string fetchMetrics() override {
    std::stringstream ss;
    ss << "[";
    auto sep = "";
    for (const auto& [key, value] : counterMap) {
      ss << sep << key << ":" << value;
      sep = ",";
    }
    ss << "]";
    return ss.str();
  }
};

folly::Singleton<BaseStatsReporter> reporter([]() {
  return new S3TestReporter();
});

class S3FileSystemMetricsTest : public S3Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    S3Test::SetUp();
    filesystems::initializeS3("Info");
    s3Reporter = std::dynamic_pointer_cast<S3TestReporter>(
        folly::Singleton<BaseStatsReporter>::try_get());
    s3Reporter->clear();
  }

  static void TearDownTestSuite() {
    filesystems::finalizeS3();
  }
  std::shared_ptr<S3TestReporter> s3Reporter;
};

} // namespace

TEST_F(S3FileSystemMetricsTest, metrics) {
  registerS3Metrics();

  const auto bucketName = "metrics";
  const auto file = "test.txt";
  const auto filename = localPath(bucketName) + "/" + file;
  const auto s3File = s3URI(bucketName, file);
  auto hiveConfig = minioServer_->hiveConfig();
  S3FileSystem s3fs(bucketName, hiveConfig);
  auto pool = memory::memoryManager()->addLeafPool("S3FileSystemMetricsTest");

  auto writeFile =
      s3fs.openFileForWrite(s3File, {{}, pool.get(), std::nullopt});
  EXPECT_EQ(1, s3Reporter->counterMap[std::string{kMetricS3MetadataCalls}]);
  EXPECT_EQ(1, s3Reporter->counterMap[std::string{kMetricS3GetMetadataErrors}]);

  constexpr std::string_view kDataContent =
      "Dance me to your beauty with a burning violin"
      "Dance me through the panic till I'm gathered safely in"
      "Lift me like an olive branch and be my homeward dove"
      "Dance me to the end of love";
  writeFile->append(kDataContent);
  writeFile->close();
  EXPECT_EQ(1, s3Reporter->counterMap[std::string{kMetricS3StartedUploads}]);
  EXPECT_EQ(1, s3Reporter->counterMap[std::string{kMetricS3SuccessfulUploads}]);

  auto readFile = s3fs.openFileForRead(s3File);
  EXPECT_EQ(2, s3Reporter->counterMap[std::string{kMetricS3MetadataCalls}]);
  readFile->pread(0, kDataContent.length());
  EXPECT_EQ(1, s3Reporter->counterMap[std::string{kMetricS3GetObjectCalls}]);
}

} // namespace facebook::velox::filesystems

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  BaseStatsReporter::registered = true;
  return RUN_ALL_TESTS();
}
