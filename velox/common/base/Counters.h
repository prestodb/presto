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

#include <folly/Range.h>

namespace facebook::velox {

/// Velox Counter Registration
void registerVeloxCounters();

constexpr folly::StringPiece kCounterHiveFileHandleGenerateLatencyMs{
    "velox.hive_file_handle_generate_latency_ms"};

constexpr folly::StringPiece kCounterCacheShrinkCount{
    "velox.cache_shrink_count"};

constexpr folly::StringPiece kCounterCacheShrinkTimeMs{"velox.cache_shrink_ms"};

constexpr folly::StringPiece kCounterMemoryReclaimExecTimeMs{
    "velox.memory_reclaim_exec_ms"};

constexpr folly::StringPiece kCounterMemoryReclaimedBytes{
    "velox.memory_reclaim_bytes"};

constexpr folly::StringPiece kCounterMemoryReclaimWaitTimeMs{
    "velox.memory_reclaim_wait_ms"};

constexpr folly::StringPiece kCounterMemoryReclaimWaitTimeoutCount{
    "velox.memory_reclaim_wait_timeout_count"};
} // namespace facebook::velox
