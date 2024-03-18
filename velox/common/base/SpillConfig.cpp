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

#include "velox/common/base/SpillConfig.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::common {
SpillConfig::SpillConfig(
    GetSpillDirectoryPathCB _getSpillDirPathCb,
    UpdateAndCheckSpillLimitCB _updateAndCheckSpillLimitCb,
    std::string _fileNamePrefix,
    uint64_t _maxFileSize,
    uint64_t _writeBufferSize,
    uint64_t _minSpillRunSize,
    folly::Executor* _executor,
    int32_t _minSpillableReservationPct,
    int32_t _spillableReservationGrowthPct,
    uint8_t _startPartitionBit,
    uint8_t _numPartitionBits,
    int32_t _maxSpillLevel,
    uint64_t _maxSpillRunRows,
    uint64_t _writerFlushThresholdSize,
    const std::string& _compressionKind,
    const std::string& _fileCreateConfig)
    : getSpillDirPathCb(std::move(_getSpillDirPathCb)),
      updateAndCheckSpillLimitCb(std::move(_updateAndCheckSpillLimitCb)),
      fileNamePrefix(std::move(_fileNamePrefix)),
      maxFileSize(
          _maxFileSize == 0 ? std::numeric_limits<int64_t>::max()
                            : _maxFileSize),
      writeBufferSize(_writeBufferSize),
      minSpillRunSize(_minSpillRunSize),
      executor(_executor),
      minSpillableReservationPct(_minSpillableReservationPct),
      spillableReservationGrowthPct(_spillableReservationGrowthPct),
      startPartitionBit(_startPartitionBit),
      numPartitionBits(_numPartitionBits),
      maxSpillLevel(_maxSpillLevel),
      maxSpillRunRows(_maxSpillRunRows),
      writerFlushThresholdSize(_writerFlushThresholdSize),
      compressionKind(common::stringToCompressionKind(_compressionKind)),
      fileCreateConfig(_fileCreateConfig) {
  VELOX_USER_CHECK_GE(
      spillableReservationGrowthPct,
      minSpillableReservationPct,
      "Spillable memory reservation growth pct should not be lower than minimum available pct");
}

int32_t SpillConfig::spillLevel(uint8_t startBitOffset) const {
  VELOX_CHECK_LE(
      startBitOffset + numPartitionBits,
      64,
      "startBitOffset:{} numPartitionsBits:{}",
      startBitOffset,
      numPartitionBits);
  const int32_t deltaBits = startBitOffset - startPartitionBit;
  VELOX_CHECK_GE(deltaBits, 0, "deltaBits:{}", deltaBits);
  VELOX_CHECK_EQ(
      deltaBits % numPartitionBits,
      0,
      "deltaBits:{} numPartitionsBits{}",
      deltaBits,
      numPartitionBits);
  return deltaBits / numPartitionBits;
}

bool SpillConfig::exceedSpillLevelLimit(uint8_t startBitOffset) const {
  if (startBitOffset + numPartitionBits > 64) {
    return true;
  }
  if (maxSpillLevel == -1) {
    return false;
  }
  return spillLevel(startBitOffset) > maxSpillLevel;
}
} // namespace facebook::velox::common
