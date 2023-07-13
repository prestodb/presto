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
#include "velox/connectors/hive/storage_adapters/gcs/GCSFileSystem.h"

DECLARE_string(gcs_config);

namespace facebook::velox {

std::shared_ptr<Config> readConfig(const std::string& filePath);

class GCSReadBenchmark : public ReadBenchmark {
 public:
  // Initialize a GCSReadFile instance for the specified 'path'.
  void initialize() override {
    executor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_num_threads);

    filesystems::registerGCSFileSystem();
    std::shared_ptr<Config> config;
    if (!FLAGS_gcs_config.empty()) {
      config = readConfig(FLAGS_gcs_config);
    }
    auto gcsfs = filesystems::getFileSystem(FLAGS_path, config);
    readFile_ = gcsfs->openFileForRead(FLAGS_path);

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
