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

#include "velox/common/file/benchmark/ReadBenchmark.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"

DECLARE_string(s3_config);

namespace facebook::velox {

std::shared_ptr<Config> readConfig(const std::string& filePath);

class S3ReadBenchmark : public ReadBenchmark {
 public:
  // Initialize a S3ReadFile instance for the specified 'path'.
  void initialize() override {
    executor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_num_threads);

    filesystems::registerS3FileSystem();
    std::shared_ptr<Config> config;
    if (!FLAGS_s3_config.empty()) {
      config = readConfig(FLAGS_s3_config);
    }
    auto s3fs = filesystems::getFileSystem(FLAGS_path, config);
    readFile_ = s3fs->openFileForRead(FLAGS_path);

    fileSize_ = readFile_->size();
    if (FLAGS_file_size_gb) {
      fileSize_ = std::min<uint64_t>(FLAGS_file_size_gb << 30, fileSize_);
    }

    if (fileSize_ <= FLAGS_measurement_size) {
      LOG(ERROR) << "File size " << fileSize_
                 << " is <= then --measurement_size " << FLAGS_measurement_size;
      exit(1);
    }
    if (FLAGS_seed) {
      rng_.seed(FLAGS_seed);
    }
  }
};

} // namespace facebook::velox
