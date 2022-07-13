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

#include <cstdint>

namespace facebook::velox::dwio::common {

constexpr uint32_t SHORT_BYTE_SIZE = 2;
constexpr uint32_t INT_BYTE_SIZE = 4;
constexpr uint32_t LONG_BYTE_SIZE = 8;

constexpr uint64_t BASE_128_MASK = 0x7f;
constexpr uint64_t BASE_256_MASK = 0xff;

// Timezone constants
constexpr int64_t SECONDS_PER_HOUR = 60 * 60;
// Timezone offset of PST from UTC.
constexpr int64_t TIMEZONE_OFFSET = (8 * SECONDS_PER_HOUR);
// ORC base epoch: 2015-01-01 00:00:00 in UTC, as seconds from UNIX epoch.
constexpr int64_t UTC_EPOCH_OFFSET = 1420070400;
// ORC actual epoch: 2015-01-01 00:00:00 in PST
// (2015-01-01 08:00:00 in GMT)
constexpr int64_t EPOCH_OFFSET = UTC_EPOCH_OFFSET + TIMEZONE_OFFSET;

// Maximum nanos possible.
constexpr int64_t MAX_NANOS = 999'999'999;
// Minimum seconds that can be written using the Timestamp Writer.
// 1 is reduced to epoch, as writer adds 1 for negative seconds.
constexpr int64_t MIN_SECONDS = INT64_MIN + (EPOCH_OFFSET - 1);

} // namespace facebook::velox::dwio::common
