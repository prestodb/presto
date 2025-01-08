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
#include "velox/common/compression/Compression.h"
#include "velox/common/config/Config.h"
#include "velox/dwio/dwrf/common/Common.h"

namespace facebook::velox::dwrf {

class Config : public config::ConfigBase {
 public:
  template <typename T>
  using Entry = config::ConfigBase::Entry<T>;

  static Entry<WriterVersion> WRITER_VERSION;
  static Entry<common::CompressionKind> COMPRESSION;
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
  static Entry<bool> INTEGER_DICTIONARY_ENCODING_ENABLED;
  static Entry<bool> STRING_DICTIONARY_ENCODING_ENABLED;
  static Entry<uint64_t> STRIPE_SIZE;
  static Entry<bool> LINEAR_STRIPE_SIZE_HEURISTICS;
  /// With this config, we don't even try the more memory intensive encodings on
  /// writer start up.
  static Entry<bool> FORCE_LOW_MEMORY_MODE;
  /// Disable low memory mode mostly for test purposes.
  static Entry<bool> DISABLE_LOW_MEMORY_MODE;
  /// Fail the writer, when Stream size is above threshold. Streams greater than
  /// 2GB will be failed to be read by Jolly/Presto reader.
  static Entry<bool> STREAM_SIZE_ABOVE_THRESHOLD_CHECK_ENABLED;
  /// Limit the raw data size per batch to avoid being forced to write oversized
  /// stripes.
  static Entry<uint64_t> RAW_DATA_SIZE_PER_BATCH;
  static Entry<bool> MAP_STATISTICS;

  /// Maximum stripe size in orc writer.
  static constexpr const char* kOrcWriterMaxStripeSize =
      "hive.orc.writer.stripe-max-size";
  static constexpr const char* kOrcWriterMaxStripeSizeSession =
      "orc_optimized_writer_max_stripe_size";

  /// Maximum dictionary memory that can be used in orc writer.
  static constexpr const char* kOrcWriterMaxDictionaryMemory =
      "hive.orc.writer.dictionary-max-memory";
  static constexpr const char* kOrcWriterMaxDictionaryMemorySession =
      "orc_optimized_writer_max_dictionary_memory";

  /// Configs to control dictionary encoding.
  static constexpr const char* kOrcWriterIntegerDictionaryEncodingEnabled =
      "hive.orc.writer.integer-dictionary-encoding-enabled";
  static constexpr const char*
      kOrcWriterIntegerDictionaryEncodingEnabledSession =
          "orc_optimized_writer_integer_dictionary_encoding_enabled";
  static constexpr const char* kOrcWriterStringDictionaryEncodingEnabled =
      "hive.orc.writer.string-dictionary-encoding-enabled";
  static constexpr const char*
      kOrcWriterStringDictionaryEncodingEnabledSession =
          "orc_optimized_writer_string_dictionary_encoding_enabled";

  /// Enables historical based stripe size estimation after compression.
  static constexpr const char* kOrcWriterLinearStripeSizeHeuristics =
      "hive.orc.writer.linear-stripe-size-heuristics";
  static constexpr const char* kOrcWriterLinearStripeSizeHeuristicsSession =
      "orc_writer_linear_stripe_size_heuristics";

  /// Minimal number of items in an encoded stream.
  static constexpr const char* kOrcWriterMinCompressionSize =
      "hive.orc.writer.min-compression-size";
  static constexpr const char* kOrcWriterMinCompressionSizeSession =
      "orc_writer_min_compression_size";

  /// The compression level to use with ZLIB and ZSTD.
  static constexpr const char* kOrcWriterCompressionLevel =
      "hive.orc.writer.compression-level";
  static constexpr const char* kOrcWriterCompressionLevelSession =
      "orc_optimized_writer_compression_level";

  static std::shared_ptr<Config> fromMap(
      const std::map<std::string, std::string>& map) {
    auto config = std::make_shared<Config>();
    for (const auto& pair : map) {
      config->set(pair.first, pair.second);
    }
    return config;
  }

  Config() : ConfigBase({}, true) {}

  std::map<std::string, std::string> toSerdeParams() {
    return std::map{configs_.cbegin(), configs_.cend()};
  }
};

} // namespace facebook::velox::dwrf
