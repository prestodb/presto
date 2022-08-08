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

#include "velox/dwio/dwrf/common/Config.h"

#include "folly/String.h"

namespace facebook::velox::dwrf {

Config::Entry<WriterVersion> Config::WRITER_VERSION(
    "orc.writer.version",
    WriterVersion_CURRENT);

Config::Entry<dwio::common::CompressionKind> Config::COMPRESSION(
    "hive.exec.orc.compress",
    dwio::common::CompressionKind::CompressionKind_ZSTD);

Config::Entry<int32_t> Config::ZLIB_COMPRESSION_LEVEL(
    "hive.exec.orc.compress.zlib.level",
    4);

Config::Entry<int32_t> Config::ZSTD_COMPRESSION_LEVEL(
    "hive.exec.orc.compress.zstd.level",
    7);

Config::Entry<uint64_t> Config::COMPRESSION_BLOCK_SIZE{
    "hive.exec.orc.compress.size",
    256 * 1024};

Config::Entry<uint64_t> Config::COMPRESSION_BLOCK_SIZE_MIN(
    "hive.exec.orc.compress.size.min",
    1024);

Config::Entry<float> Config::COMPRESSION_BLOCK_SIZE_EXTEND_RATIO(
    "hive.exec.orc.compress.size.extend.ratio",
    2.0f);

Config::Entry<uint32_t> Config::COMPRESSION_THRESHOLD(
    "orc.compression.threshold",
    256);

Config::Entry<bool> Config::CREATE_INDEX{"hive.exec.orc.create.index", true};

Config::Entry<uint32_t> Config::ROW_INDEX_STRIDE{
    "hive.exec.orc.row.index.stride",
    10000};

Config::Entry<proto::ChecksumAlgorithm> Config::CHECKSUM_ALGORITHM{
    "orc.checksum.algorithm",
    proto::ChecksumAlgorithm::XXHASH};

Config::Entry<StripeCacheMode> Config::STRIPE_CACHE_MODE{
    "orc.stripe.cache.mode",
    StripeCacheMode::BOTH};

Config::Entry<uint32_t> Config::STRIPE_CACHE_SIZE{
    "orc.stripe.cache.size",
    8 * 1024 * 1024};

Config::Entry<uint32_t> Config::DICTIONARY_ENCODING_INTERVAL{
    "hive.exec.orc.encoding.interval",
    30};

Config::Entry<bool> Config::USE_VINTS{"hive.exec.orc.use.vints", true};

Config::Entry<float> Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD{
    "hive.exec.orc.dictionary.key.numeric.size.threshold",
    0.7f};

Config::Entry<float> Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD{
    "hive.exec.orc.dictionary.key.string.size.threshold",
    0.8f};

Config::Entry<bool> Config::DICTIONARY_SORT_KEYS{
    "hive.exec.orc.dictionary.key.sorted",
    false};

Config::Entry<float> Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD{
    "hive.exec.orc.entropy.key.string.size.threshold",
    0.9f};

Config::Entry<uint32_t> Config::ENTROPY_STRING_MIN_SAMPLES{
    "hive.exec.orc.entropy.string.min.samples",
    100};

Config::Entry<float> Config::ENTROPY_STRING_DICT_SAMPLE_FRACTION{
    "hive.exec.orc.entropy.string.dict.sample.fraction",
    0.001f};

Config::Entry<uint32_t> Config::ENTROPY_STRING_THRESHOLD{
    "hive.exec.orc.entropy.string.threshold",
    20};

Config::Entry<uint32_t> Config::STRING_STATS_LIMIT(
    "hive.orc.string.stats.limit",
    64);

Config::Entry<bool> Config::FLATTEN_MAP("orc.flatten.map", false);

Config::Entry<bool> Config::MAP_FLAT_DISABLE_DICT_ENCODING(
    "orc.map.flat.disable.dict.encoding",
    true);

Config::Entry<bool> Config::MAP_FLAT_DISABLE_DICT_ENCODING_STRING(
    "orc.map.flat.disable.dict.encoding.string",
    true);

Config::Entry<bool> Config::MAP_FLAT_DICT_SHARE(
    "orc.map.flat.dict.share",
    true);

Config::Entry<const std::vector<uint32_t>> Config::MAP_FLAT_COLS(
    "orc.map.flat.cols",
    {},
    [](const std::vector<uint32_t>& val) { return folly::join(",", val); },
    [](const std::string& val) {
      std::vector<uint32_t> result;
      if (!val.empty()) {
        std::vector<folly::StringPiece> pieces;
        folly::split(',', val, pieces, true);
        for (auto& p : pieces) {
          const auto& trimmedCol = folly::trimWhitespace(p);
          if (!trimmedCol.empty()) {
            result.push_back(folly::to<uint32_t>(trimmedCol));
          }
        }
      }
      return result;
    });

Config::Entry<const std::vector<std::vector<std::string>>>
    Config::MAP_FLAT_COLS_STRUCT_KEYS(
        "orc.map.flat.cols.struct.keys",
        {},
        [](const std::vector<std::vector<std::string>>& val) {
          std::vector<std::string> columns;
          columns.reserve(val.size());
          std::transform(
              val.cbegin(),
              val.cend(),
              std::back_inserter(columns),
              [](const auto& v) { return folly::join(",", v); });
          return folly::join(";", columns);
        },
        [](const std::string& val) {
          std::vector<std::string> partialResult;
          folly::split(";", val, partialResult);
          std::vector<std::vector<std::string>> result;
          std::transform(
              partialResult.cbegin(),
              partialResult.cend(),
              std::back_inserter(result),
              [](const auto& str) {
                std::vector<std::string> res;
                folly::split(",", str, res);
                return res;
              });
          return result;
        });

Config::Entry<uint32_t> Config::MAP_FLAT_MAX_KEYS(
    "orc.map.flat.max.keys",
    20000);

Config::Entry<uint64_t> Config::MAX_DICTIONARY_SIZE(
    "hive.exec.orc.max.dictionary.size",
    80L * 1024L * 1024L);

Config::Entry<uint64_t> Config::STRIPE_SIZE(
    "hive.exec.orc.stripe.size",
    256L * 1024L * 1024L);

Config::Entry<bool> Config::FORCE_LOW_MEMORY_MODE(
    "hive.exec.orc.low.memory",
    false);

Config::Entry<bool> Config::DISABLE_LOW_MEMORY_MODE(
    "hive.exec.orc.disable.low.memory.mode",
    false);

Config::Entry<bool> Config::STREAM_SIZE_ABOVE_THRESHOLD_CHECK_ENABLED(
    "orc.stream.size.above.threshold.check.enabled",
    true);

Config::Entry<uint64_t> Config::RAW_DATA_SIZE_PER_BATCH(
    "hive.exec.orc.raw.data.size.per.batch",
    50UL * 1024 * 1024);

Config::Entry<bool> Config::MAP_STATISTICS("orc.map.statistics", false);
} // namespace facebook::velox::dwrf
