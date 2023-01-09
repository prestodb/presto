/*
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

// Here we have all the counters presto cpp worker would export.
namespace facebook::presto {

// Sets up all the counters in the presto cpp, but specifying their types.
// See velox/common/base/StatsReporter.h for the interface.
void registerPrestoCppCounters();

constexpr folly::StringPiece kCounterDriverCPUExecutorQueueSize{
    "presto_cpp.driver_cpu_executor_queue_size"};
constexpr folly::StringPiece kCounterDriverCPUExecutorLatencyMs{
    "presto_cpp.driver_cpu_executor_latency_ms"};

constexpr folly::StringPiece kCounterHTTPExecutorLatencyMs{
    "presto_cpp.http_executor_latency_ms"};
constexpr folly::StringPiece kCounterNumHTTPRequest{
    "presto_cpp.num_http_request"};
constexpr folly::StringPiece kCounterNumHTTPRequestError{
    "presto_cpp.num_http_request_error"};
constexpr folly::StringPiece kCounterHTTPRequestLatencyMs{
    "presto_cpp.http_request_latency_ms"};

constexpr folly::StringPiece kCounterNumQueryContexts{
    "presto_cpp.num_query_contexts"};

constexpr folly::StringPiece kCounterNumTasks{"presto_cpp.num_tasks"};
constexpr folly::StringPiece kCounterNumTasksRunning{
    "presto_cpp.num_tasks_running"};
constexpr folly::StringPiece kCounterNumTasksFinished{
    "presto_cpp.num_tasks_finished"};
constexpr folly::StringPiece kCounterNumTasksCancelled{
    "presto_cpp.num_tasks_cancelled"};
constexpr folly::StringPiece kCounterNumTasksAborted{
    "presto_cpp.num_tasks_aborted"};
constexpr folly::StringPiece kCounterNumTasksFailed{
    "presto_cpp.num_tasks_failed"};
constexpr folly::StringPiece kCounterNumZombieTasks{
    "presto_cpp.num_zombie_tasks"};
constexpr folly::StringPiece kCounterNumZombiePrestoTasks{
    "presto_cpp.num_zombie_presto_tasks"};
constexpr folly::StringPiece kCounterNumRunningDrivers{
    "presto_cpp.num_running_drivers"};
constexpr folly::StringPiece kCounterNumBlockedDrivers{
    "presto_cpp.num_blocked_drivers"};

// Number of total PartitionedOutputBuffer managed by all
// PartitionedOutputBufferManager
constexpr folly::StringPiece kCounterTotalPartitionedOutputBuffer{
    "presto_cpp.num_partitioned_output_buffer"};

// ================== Memory Counters =================

// Number of bytes of memory MappedMemory currently maps (RSS). It also includes
// memory that was freed and currently not in use.
constexpr folly::StringPiece kCounterMappedMemoryBytes{
    "presto_cpp.mapped_memory_bytes"};
// Number of bytes of memory MappedMemory currently allocates. Memories in use
constexpr folly::StringPiece kCounterAllocatedMemoryBytes{
    "presto_cpp.allocated_memory_bytes"};
// Number of bytes currently allocated from MappedMemory directly from raw
// allocateBytes() interface, and internally allocated by malloc. Only small
// chunks of memory are delegated to malloc
constexpr folly::StringPiece kCounterMappedMemoryRawAllocBytesSmall{
    "presto_cpp.mapped_memory_raw_alloc_bytes_small"};
// Number of bytes currently allocated from MappedMemory directly from raw
// allocateBytes() interface, and internally allocated using SizeClass that are
// managed by MappedMemory. Only chunks of memory that are large enough to be
// efficiently fitted in the smallest SizeClass and not too large that even
// largest SizeClass cannot accommodate, are counted towards this counter.
constexpr folly::StringPiece kCounterMappedMemoryRawAllocBytesSizeClass{
    "presto_cpp.mapped_memory_raw_alloc_bytes_size_class"};
// Number of bytes currently allocated from MappedMemory directly from raw
// allocateBytes() interface, and internally allocated using SizeClass that are
// managed by MappedMemory. Only chunks of memory that are too large that even
// largest SizeClass cannot accommodate, are counted towards this counter.
constexpr folly::StringPiece kCounterMappedMemoryRawAllocBytesLarge{
    "presto_cpp.mapped_memory_raw_alloc_bytes_large"};

// ================== Cache Counters ==================

// Total number of cache entries.
constexpr folly::StringPiece kCounterMemoryCacheNumEntries{
    "presto_cpp.memory_cache_num_entries"};
// Total number of cache entries that do not cache anything.
constexpr folly::StringPiece kCounterMemoryCacheNumEmptyEntries{
    "presto_cpp.memory_cache_num_empty_entries"};
// Total number of cache entries that are pinned for shared access.
constexpr folly::StringPiece kCounterMemoryCacheNumSharedEntries{
    "presto_cpp.memory_cache_num_shared_entries"};
// Total number of cache entries that are pinned for exclusive access.
constexpr folly::StringPiece kCounterMemoryCacheNumExclusiveEntries{
    "presto_cpp.memory_cache_num_exclusive_entries"};
// Total number of cache entries that are being or have been prefetched but have
// not been hit.
constexpr folly::StringPiece kCounterMemoryCacheNumPrefetchedEntries{
    "presto_cpp.memory_cache_num_prefetched_entries"};
// Total number of bytes of the cached data that is much smaller than a
// 'MappedMemory' page (AsyncDataCacheEntry::kTinyDataSize).
constexpr folly::StringPiece kCounterMemoryCacheTotalTinyBytes{
    "presto_cpp.memory_cache_total_tiny_bytes"};
// Total number of bytes of the cached data excluding
// 'kCounterMemoryCacheTotalTinyBytes'.
constexpr folly::StringPiece kCounterMemoryCacheTotalLargeBytes{
    "presto_cpp.memory_cache_total_large_bytes"};
// Total unused capacity bytes in 'kCounterMemoryCacheTotalTinyBytes'.
constexpr folly::StringPiece kCounterMemoryCacheTotalTinyPaddingBytes{
    "presto_cpp.memory_cache_total_tiny_padding_bytes"};
// Total unused capacity bytes in 'kCounterMemoryCacheTotalLargeBytes'.
constexpr folly::StringPiece kCounterMemoryCacheTotalLargePaddingBytes{
    "presto_cpp.memory_cache_total_large_padding_bytes"};
// Total bytes of cache entries in prefetch state.
constexpr folly::StringPiece kCounterMemoryCacheTotalPrefetchBytes{
    "presto_cpp.memory_cache_total_prefetched_bytes"};
// Sum of scores of evicted entries. This serves to infer an average lifetime
// for entries in cache.
constexpr folly::StringPiece kCounterMemoryCacheSumEvictScore{
    "presto_cpp.memory_cache_sum_evict_score"};
// Cumulated number of hits (saved IO). The first hit to a prefetched entry does
// not count.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeHit{
    "presto_cpp.memory_cache_num_cumulative_hit"};
// Number of hits (saved IO) since last counter retrieval. The first hit to a
// prefetched entry does not count.
constexpr folly::StringPiece kCounterMemoryCacheNumHit{
    "presto_cpp.memory_cache_num_hit"};
// Cumulated number of new entries created.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeNew{
    "presto_cpp.memory_cache_num_cumulative_new"};
// Number of new entries created since last counter retrieval.
constexpr folly::StringPiece kCounterMemoryCacheNumNew{
    "presto_cpp.memory_cache_num_new"};
// Cumulated number of times a valid entry was removed in order to make space.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeEvict{
    "presto_cpp.memory_cache_num_cumulative_evict"};
// Number of times a valid entry was removed in order to make space, since last
// counter retrieval.
constexpr folly::StringPiece kCounterMemoryCacheNumEvict{
    "presto_cpp.memory_cache_num_evict"};
// Cumulated number of entries considered for evicting.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeEvictChecks{
    "presto_cpp.memory_cache_num_cumulative_evict_checks"};
// Number of entries considered for evicting, since last counter retrieval.
constexpr folly::StringPiece kCounterMemoryCacheNumEvictChecks{
    "presto_cpp.memory_cache_num_evict_checks"};
// Cumulated number of times a user waited for an entry to transit from
// exclusive to shared mode.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeWaitExclusive{
    "presto_cpp.memory_cache_num_cumulative_wait_exclusive"};
// Number of times a user waited for an entry to transit from exclusive to
// shared mode, since last counter retrieval.
constexpr folly::StringPiece kCounterMemoryCacheNumWaitExclusive{
    "presto_cpp.memory_cache_num_wait_exclusive"};
// Cumulative clocks spent in allocating or freeing memory for backing cache
// entries.
constexpr folly::StringPiece kCounterMemoryCacheNumCumulativeAllocClocks{
    "presto_cpp.memory_cache_num_cumulative_alloc_clocks"};
// Clocks spent in allocating or freeing memory for backing cache
// entries, since last counter retrieval
constexpr folly::StringPiece kCounterMemoryCacheNumAllocClocks{
    "presto_cpp.memory_cache_num_alloc_clocks"};
} // namespace facebook::presto
