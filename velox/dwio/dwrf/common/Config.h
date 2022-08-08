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

#pragma once

#include <functional>
#include <unordered_map>
#include "folly/Conv.h"
#include "velox/dwio/common/Common.h"
#include "velox/dwio/dwrf/common/Common.h"

namespace facebook::velox::dwrf {

class Config {
 public:
  template <typename T>
  class Entry {
    Entry(
        const std::string& key,
        const T& val,
        std::function<std::string(const T&)> toStr =
            [](const T& val) { return folly::to<std::string>(val); },
        std::function<T(const std::string&)> toT =
            [](const std::string& val) { return folly::to<T>(val); })
        : key_{key}, default_{val}, toStr_{toStr}, toT_{toT} {}

    const std::string key_;
    const T default_;
    const std::function<std::string(const T&)> toStr_;
    const std::function<T(const std::string&)> toT_;

    friend Config;
  };

  template <typename T>
  Config& set(const Entry<T>& entry, const T& val) {
    configs_[entry.key_] = entry.toStr_(val);
    return *this;
  }

  template <typename T>
  Config& unset(const Entry<T>& entry) {
    auto iter = configs_.find(entry.key_);
    if (iter != configs_.end()) {
      configs_.erase(iter);
    }
    return *this;
  }

  Config& reset() {
    configs_.clear();
    return *this;
  }

  template <typename T>
  T get(const Entry<T>& entry) const {
    auto iter = configs_.find(entry.key_);
    return iter != configs_.end() ? entry.toT_(iter->second) : entry.default_;
  }

  static std::shared_ptr<Config> fromMap(
      std::map<std::string, std::string> map) {
    auto ret = std::make_shared<Config>();
    ret->configs_.insert(map.begin(), map.end());
    return ret;
  }

  static Entry<WriterVersion> WRITER_VERSION;
  static Entry<dwio::common::CompressionKind> COMPRESSION;
  static Entry<int32_t> ZLIB_COMPRESSION_LEVEL;
  static Entry<int32_t> ZSTD_COMPRESSION_LEVEL;
  static Entry<uint64_t> COMPRESSION_BLOCK_SIZE;
  static Entry<uint64_t> COMPRESSION_BLOCK_SIZE_MIN;
  static Entry<float> COMPRESSION_BLOCK_SIZE_EXTEND_RATIO;
  static Entry<uint32_t> COMPRESSION_THRESHOLD;
  static Entry<bool> CREATE_INDEX;
  static Entry<uint32_t> ROW_INDEX_STRIDE;
  static Entry<proto::ChecksumAlgorithm> CHECKSUM_ALGORITHM;
  static Entry<StripeCacheMode> STRIPE_CACHE_MODE;
  static Entry<uint32_t> STRIPE_CACHE_SIZE;
  static Entry<uint32_t> DICTIONARY_ENCODING_INTERVAL;
  static Entry<bool> USE_VINTS;
  static Entry<float> DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD;
  static Entry<float> DICTIONARY_STRING_KEY_SIZE_THRESHOLD;
  static Entry<bool> DICTIONARY_SORT_KEYS;
  static Entry<float> ENTROPY_KEY_STRING_SIZE_THRESHOLD;
  static Entry<uint32_t> ENTROPY_STRING_MIN_SAMPLES;
  static Entry<float> ENTROPY_STRING_DICT_SAMPLE_FRACTION;
  static Entry<uint32_t> ENTROPY_STRING_THRESHOLD;
  static Entry<uint32_t> STRING_STATS_LIMIT;
  static Entry<bool> FLATTEN_MAP;
  static Entry<bool> MAP_FLAT_DISABLE_DICT_ENCODING;
  static Entry<bool> MAP_FLAT_DISABLE_DICT_ENCODING_STRING;
  static Entry<bool> MAP_FLAT_DICT_SHARE;
  static Entry<const std::vector<uint32_t>> MAP_FLAT_COLS;
  static Entry<const std::vector<std::vector<std::string>>>
      MAP_FLAT_COLS_STRUCT_KEYS;
  static Entry<uint32_t> MAP_FLAT_MAX_KEYS;
  static Entry<uint64_t> MAX_DICTIONARY_SIZE;
  static Entry<uint64_t> STRIPE_SIZE;
  // With this config, we don't even try the more memory intensive encodings
  // on writer start up.
  static Entry<bool> FORCE_LOW_MEMORY_MODE;
  // Disable low memory mode mostly for test purposes.
  static Entry<bool> DISABLE_LOW_MEMORY_MODE;
  // Fail the writer, when Stream size is above threshold
  // Streams greater than 2GB will be failed to be read by Jolly/Presto reader.
  static Entry<bool> STREAM_SIZE_ABOVE_THRESHOLD_CHECK_ENABLED;
  // Limit the raw data size per batch to avoid being forced
  // to write oversized stripes.
  static Entry<uint64_t> RAW_DATA_SIZE_PER_BATCH;
  static Entry<bool> MAP_STATISTICS;

 private:
  std::unordered_map<std::string, std::string> configs_;
};

} // namespace facebook::velox::dwrf
