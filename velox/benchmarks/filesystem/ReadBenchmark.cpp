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

#include "velox/benchmarks/filesystem/ReadBenchmark.h"

#include "velox/connectors/hive/storage_adapters/abfs/RegisterAbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/gcs/RegisterGCSFileSystem.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#include "velox/core/Config.h"

DEFINE_string(path, "", "Path of the input file");
DEFINE_int64(
    file_size_gb,
    0,
    "Limits the test to the first --file_size_gb "
    "of --path. 0 means use the whole file");
DEFINE_int32(num_threads, 16, "Test paralelism");
DEFINE_int32(seed, 0, "Random seed, 0 means no seed");
DEFINE_bool(odirect, false, "Use O_DIRECT");

DEFINE_int32(
    bytes,
    0,
    "If 0, runs through a set of predefined read patterns. "
    "If non-0, this is the size of a single read. The reads are "
    "made in --num_in_run consecutive batchhes with --gap bytes between each read");
DEFINE_int32(gap, 0, "Gap between consecutive reads if --bytes is non-0");
DEFINE_int32(
    num_in_run,
    10,
    "Number of consecutive reads of --bytes separated by --gap bytes");
DEFINE_int32(
    measurement_size,
    100 << 20,
    "Total reads per thread when throughput for a --bytes/--gap/--/gap/"
    "--num_in_run combination");
DEFINE_string(config, "", "Path of the config file");

namespace {
static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}
} // namespace

DEFINE_validator(path, &notEmpty);

namespace facebook::velox {

std::shared_ptr<Config> readConfig(const std::string& filePath) {
  std::ifstream configFile(filePath);
  if (!configFile.is_open()) {
    throw std::runtime_error(
        fmt::format("Couldn't open config file {} for reading.", filePath));
  }

  std::unordered_map<std::string, std::string> properties;
  std::string line;
  while (getline(configFile, line)) {
    line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
    if (line[0] == '#' || line.empty()) {
      continue;
    }
    auto delimiterPos = line.find('=');
    auto name = line.substr(0, delimiterPos);
    auto value = line.substr(delimiterPos + 1);
    properties.emplace(name, value);
  }

  return std::make_shared<facebook::velox::core::MemConfig>(properties);
}

// Initialize a LocalReadFile instance for the specified 'path'.
void ReadBenchmark::initialize() {
  executor_ = std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_num_threads);
  if (FLAGS_odirect) {
    int32_t o_direct =
#ifdef linux
        O_DIRECT;
#else
        0;
#endif
    fd_ = open(
        FLAGS_path.c_str(),
        O_CREAT | O_RDWR | (FLAGS_odirect ? o_direct : 0),
        S_IRUSR | S_IWUSR);
    if (fd_ < 0) {
      LOG(ERROR) << "Could not open " << FLAGS_path;
      exit(1);
    }
    readFile_ = std::make_unique<LocalReadFile>(fd_);
  } else {
    filesystems::registerLocalFileSystem();
    filesystems::registerS3FileSystem();
    filesystems::registerGCSFileSystem();
    filesystems::registerHdfsFileSystem();
    filesystems::abfs::registerAbfsFileSystem();
    std::shared_ptr<Config> config;
    if (!FLAGS_config.empty()) {
      config = readConfig(FLAGS_config);
    }
    auto fs = filesystems::getFileSystem(FLAGS_path, config);
    readFile_ = fs->openFileForRead(FLAGS_path);
    fileSize_ = readFile_->size();
    if (FLAGS_file_size_gb) {
      fileSize_ = std::min<uint64_t>(FLAGS_file_size_gb << 30, fileSize_);
    }
  }

  if (fileSize_ <= FLAGS_measurement_size) {
    LOG(ERROR) << "File size " << fileSize_ << " is <= then --measurement_size "
               << FLAGS_measurement_size;
    exit(1);
  }
  if (FLAGS_seed) {
    rng_.seed(FLAGS_seed);
  }
}

void ReadBenchmark::finalize() {
  filesystems::finalizeS3FileSystem();
}

void ReadBenchmark::run() {
  if (FLAGS_bytes) {
    modes(FLAGS_bytes, FLAGS_gap, FLAGS_num_in_run);
    return;
  }
  modes(1100, 0, 10);
  modes(1100, 1200, 10);
  modes(16 * 1024, 0, 10);
  modes(16 * 1024, 10000, 10);
  modes(1000000, 0, 8);
  modes(1000000, 100000, 8);
}
} // namespace facebook::velox
