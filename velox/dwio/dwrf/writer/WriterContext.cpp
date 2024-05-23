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

#include "velox/dwio/dwrf/writer/WriterContext.h"
#include "velox/common/compression/Compression.h"
#include "velox/exec/MemoryReclaimer.h"

namespace facebook::velox::dwrf {
namespace {
constexpr uint32_t MIN_INDEX_STRIDE = 1000;
}

WriterContext::WriterContext(
    const std::shared_ptr<const Config>& config,
    std::shared_ptr<memory::MemoryPool> pool,
    const dwio::common::MetricsLogPtr& metricLogger,
    std::unique_ptr<encryption::EncryptionHandler> handler)
    : config_{config},
      pool_{std::move(pool)},
      dictionaryPool_{
          pool_->addLeafChild(fmt::format("{}.dictionary", pool_->name()))},
      outputStreamPool_{
          pool_->addLeafChild(fmt::format("{}.compression", pool_->name()))},
      generalPool_{
          pool_->addLeafChild(fmt::format("{}.general", pool_->name()))},
      indexEnabled_{getConfig(Config::CREATE_INDEX)},
      indexStride_{getConfig(Config::ROW_INDEX_STRIDE)},
      compression_{getConfig(Config::COMPRESSION)},
      compressionBlockSize_{getConfig(Config::COMPRESSION_BLOCK_SIZE)},
      shareFlatMapDictionaries_{getConfig(Config::MAP_FLAT_DICT_SHARE)},
      stripeSizeFlushThreshold_{getConfig(Config::STRIPE_SIZE)},
      dictionarySizeFlushThreshold_{getConfig(Config::MAX_DICTIONARY_SIZE)},
      linearStripeSizeHeuristics_{
          getConfig(Config::LINEAR_STRIPE_SIZE_HEURISTICS)},
      streamSizeAboveThresholdCheckEnabled_{
          getConfig(Config::STREAM_SIZE_ABOVE_THRESHOLD_CHECK_ENABLED)},
      rawDataSizePerBatch_{getConfig(Config::RAW_DATA_SIZE_PER_BATCH)},
      // Currently logging with no metadata. Might consider populating
      // metadata with dwio::common::request::AccessDescriptor upstream and
      // pass down the metric log.
      metricLogger_{metricLogger},
      handler_{std::move(handler)} {
  const bool forceLowMemoryMode{getConfig(Config::FORCE_LOW_MEMORY_MODE)};
  const bool disableLowMemoryMode{getConfig(Config::DISABLE_LOW_MEMORY_MODE)};
  VELOX_CHECK(!(forceLowMemoryMode && disableLowMemoryMode));
  checkLowMemoryMode_ = !forceLowMemoryMode && !disableLowMemoryMode;
  if (forceLowMemoryMode) {
    setLowMemoryMode();
  }
  if (handler_ == nullptr) {
    handler_ = std::make_unique<encryption::EncryptionHandler>();
  }
  validateConfigs();
  VLOG(2) << fmt::format(
      "Compression config: {}", common::compressionKindToString(compression_));
}

WriterContext::~WriterContext() {
  releaseMemoryReservation();
}

void WriterContext::validateConfigs() const {
  // the writer is implemented with strong assumption that index is enabled.
  // Things like dictionary encoding will fail if not. Before we clean that up,
  // always require index to be enabled.
  VELOX_CHECK(indexEnabled(), "index is required");
  if (indexEnabled()) {
    VELOX_CHECK_GE(indexStride_, MIN_INDEX_STRIDE);
    // Java works with signed integer and setting anything above the int32_max
    // will make the java reader fail.
    VELOX_CHECK_LE(indexStride_, INT32_MAX);
  }
  VELOX_CHECK_GE(
      compressionBlockSize_, getConfig(Config::COMPRESSION_BLOCK_SIZE_MIN));
  VELOX_CHECK_GE(
      getConfig(Config::COMPRESSION_BLOCK_SIZE_EXTEND_RATIO),
      dwio::common::MIN_PAGE_GROW_RATIO);
}

void WriterContext::initBuffer() {
  VELOX_CHECK_NULL(compressionBuffer_);
  if (compression_ != common::CompressionKind_NONE) {
    compressionBuffer_ = std::make_unique<dwio::common::DataBuffer<char>>(
        *generalPool_, compressionBlockSize_ + PAGE_HEADER_SIZE);
  }
}

memory::MemoryPool& WriterContext::getMemoryPool(
    const MemoryUsageCategory& category) {
  switch (category) {
    case MemoryUsageCategory::DICTIONARY:
      return *dictionaryPool_;
    case MemoryUsageCategory::OUTPUT_STREAM:
      return *outputStreamPool_;
    case MemoryUsageCategory::GENERAL:
      return *generalPool_;
    default:
      VELOX_FAIL("Unreachable: {}", static_cast<int>(category));
  }
}

int64_t WriterContext::getMemoryUsage(
    const MemoryUsageCategory& category) const {
  switch (category) {
    case MemoryUsageCategory::DICTIONARY:
      return dictionaryPool_->usedBytes();
    case MemoryUsageCategory::OUTPUT_STREAM:
      return outputStreamPool_->usedBytes();
    case MemoryUsageCategory::GENERAL:
      return generalPool_->usedBytes();
    default:
      VELOX_FAIL("Unreachable: {}", static_cast<int>(category));
  }
}

int64_t WriterContext::getTotalMemoryUsage() const {
  return getMemoryUsage(MemoryUsageCategory::OUTPUT_STREAM) +
      getMemoryUsage(MemoryUsageCategory::DICTIONARY) +
      getMemoryUsage(MemoryUsageCategory::GENERAL);
}

int64_t WriterContext::availableMemoryReservation() const {
  return dictionaryPool_->availableReservation() +
      outputStreamPool_->availableReservation() +
      generalPool_->availableReservation();
}

void WriterContext::releaseMemoryReservation() {
  dictionaryPool_->release();
  outputStreamPool_->release();
  generalPool_->release();
}

void WriterContext::abort() {
  compressionBuffer_.reset();
  physicalSizeAggregators_.clear();
  streams_.clear();
  dictEncoders_.clear();
  decodedVectorPool_.clear();
  decodedVectorPool_.shrink_to_fit();
  selectivityVector_.reset();
  releaseMemoryReservation();
}
} // namespace facebook::velox::dwrf
