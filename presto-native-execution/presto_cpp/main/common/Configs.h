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

#include <folly/SocketAddress.h>
#include <memory>
#include <string>
#include <unordered_map>
#include "velox/common/config/Config.h"

namespace facebook::presto {

class ConfigBase {
 public:
  /// Setting this to 'true' makes configs modifiable via server operations.
  static constexpr std::string_view kMutableConfig{"mutable-config"};

  /// Reads configuration properties from the specified file. Must be called
  /// before calling any of the getters below.
  /// @param filePath Path to configuration file.
  /// @param optionalConfig Specify if the configuration file is optional.
  void initialize(const std::string& filePath, bool optionalConfig = false);

  /// Allows individual config to manipulate just-loaded-from-file key-value map
  /// before it is used to initialize the config.
  virtual void updateLoadedValues(
      std::unordered_map<std::string, std::string>& values) const {}

  /// Uses a config object already materialized.
  void initialize(std::unique_ptr<velox::config::ConfigBase>&& config) {
    config_ = std::move(config);
  }

  /// DO NOT DELETE THIS METHOD!
  /// The method is used to register new properties after the config class is created.
  /// Returns true if succeeded, false if failed (due to the property already
  /// registered).
  bool registerProperty(
    const std::string& propertyName,
    const folly::Optional<std::string>& defaultValue = {});

  /// Adds or replaces value at the given key. Can be used by debugging or
  /// testing code.
  /// Returns previous value if there was any.
  folly::Optional<std::string> setValue(
      const std::string& propertyName,
      const std::string& value);

  /// Returns a required value of the requested type. Fails if no value found.
  template <typename T>
  T requiredProperty(const std::string& propertyName) const {
    auto propertyValue = config_->get<T>(propertyName);
    if (propertyValue.has_value()) {
      return propertyValue.value();
    } else {
      VELOX_USER_FAIL(
          "{} is required in the {} file.", propertyName, filePath_);
    }
  }

  /// Returns a required value of the requested type. Fails if no value found.
  template <typename T>
  T requiredProperty(std::string_view propertyName) const {
    return requiredProperty<T>(std::string{propertyName});
  }

  /// Returns a required value of the string type. Fails if no value found.
  std::string requiredProperty(const std::string& propertyName) const {
    auto propertyValue = config_->get<std::string>(propertyName);
    if (propertyValue.has_value()) {
      return propertyValue.value();
    } else {
      VELOX_USER_FAIL(
          "{} is required in the {} file.", propertyName, filePath_);
    }
  }

  /// Returns a required value of the requested type. Fails if no value found.
  std::string requiredProperty(std::string_view propertyName) const {
    return requiredProperty(std::string{propertyName});
  }

  /// Returns optional value of the requested type. Can return folly::none.
  template <typename T>
  folly::Optional<T> optionalProperty(const std::string& propertyName) const {
    auto valOpt = config_->get<T>(propertyName);
    if (valOpt.hasValue()) {
      return valOpt.value();
    }
    const auto it = registeredProps_.find(propertyName);
    if (it != registeredProps_.end() && it->second.has_value()) {
      return folly::Optional<T>(folly::to<T>(it->second.value()));
    }
    return folly::none;
  }

  /// Returns optional value of the requested type. Can return folly::none.
  template <typename T>
  folly::Optional<T> optionalProperty(std::string_view propertyName) const {
    return optionalProperty<T>(std::string{propertyName});
  }

  /// Returns optional value of the string type. Can return folly::none.
  folly::Optional<std::string> optionalProperty(
      const std::string& propertyName) const {
    auto val = config_->get<std::string>(propertyName);
    if (val.hasValue()) {
      return val;
    }
    const auto it = registeredProps_.find(propertyName);
    if (it != registeredProps_.end()) {
      return it->second;
    }
    return folly::none;
  }

  /// Returns optional value of the string type. Can return folly::none.
  folly::Optional<std::string> optionalProperty(
      std::string_view propertyName) const {
    return optionalProperty(std::string{propertyName});
  }

  /// Returns "N<capacity_unit>" as string containing capacity in bytes.
  std::string capacityPropertyAsBytesString(
      std::string_view propertyName) const;

  /// Returns copy of the config values map.
  std::unordered_map<std::string, std::string> values() const {
    return config_->rawConfigsCopy();
  }

  virtual ~ConfigBase() = default;

 protected:
  ConfigBase()
      : config_(std::make_unique<velox::config::ConfigBase>(
            std::unordered_map<std::string, std::string>())){};

  // Check if all properties are registered.
  void checkRegisteredProperties(
      const std::unordered_map<std::string, std::string>& values);

  std::unique_ptr<velox::config::ConfigBase> config_;
  std::string filePath_;
  // Map of registered properties with their default values.
  std::unordered_map<std::string, folly::Optional<std::string>>
      registeredProps_;
};

/// Provides access to system properties defined in config.properties file.
class SystemConfig : public ConfigBase {
 public:
  static constexpr std::string_view kPrestoVersion{"presto.version"};
  static constexpr std::string_view kHttpServerHttpPort{
      "http-server.http.port"};

  /// This option allows a port closed in TIME_WAIT state to be reused
  /// immediately upon worker startup. This property is mainly used by batch
  /// processing. For interactive query, the worker uses a dynamic port upon
  /// startup.
  static constexpr std::string_view kHttpServerReusePort{
      "http-server.reuse-port"};
  /// By default the server binds to 0.0.0.0
  /// With this option enabled the server will bind strictly to the
  /// address set in node.internal-address property
  static constexpr std::string_view
      kHttpServerBindToNodeInternalAddressOnlyEnabled{
          "http-server.bind-to-node-internal-address-only-enabled"};
  static constexpr std::string_view kDiscoveryUri{"discovery.uri"};
  static constexpr std::string_view kMaxDriversPerTask{
      "task.max-drivers-per-task"};
  static constexpr std::string_view kTaskWriterCount{"task.writer-count"};
  static constexpr std::string_view kTaskPartitionedWriterCount{
      "task.partitioned-writer-count"};
  static constexpr std::string_view kConcurrentLifespansPerTask{
      "task.concurrent-lifespans-per-task"};
  static constexpr std::string_view kTaskMaxPartialAggregationMemory{
      "task.max-partial-aggregation-memory"};

  /// Floating point number used in calculating how many threads we would use
  /// for HTTP IO executor: hw_concurrency x multiplier. 1.0 is default.
  static constexpr std::string_view kHttpServerNumIoThreadsHwMultiplier{
      "http-server.num-io-threads-hw-multiplier"};

  /// Floating point number used in calculating how many threads we would use
  /// for HTTP CPU executor: hw_concurrency x multiplier. 1.0 is default.
  static constexpr std::string_view kHttpServerNumCpuThreadsHwMultiplier{
      "http-server.num-cpu-threads-hw-multiplier"};

  static constexpr std::string_view kHttpServerHttpsPort{
      "http-server.https.port"};
  static constexpr std::string_view kHttpServerHttpsEnabled{
      "http-server.https.enabled"};
  /// List of comma separated ciphers the client can use.
  ///
  /// NOTE: the client needs to have at least one cipher shared with server
  /// to communicate.
  static constexpr std::string_view kHttpsSupportedCiphers{
      "https-supported-ciphers"};
  static constexpr std::string_view kHttpsCertPath{"https-cert-path"};
  static constexpr std::string_view kHttpsKeyPath{"https-key-path"};
  /// Path to a .PEM file with certificate and key concatenated together.
  static constexpr std::string_view kHttpsClientCertAndKeyPath{
      "https-client-cert-key-path"};

  /// Floating point number used in calculating how many threads we would use
  /// for CPU executor for connectors mainly for async operators:
  /// hw_concurrency x multiplier.
  /// If 0.0 then connector CPU executor would not be created.
  /// 0.0 is default.
  static constexpr std::string_view kConnectorNumCpuThreadsHwMultiplier{
      "connector.num-cpu-threads-hw-multiplier"};

  /// Floating point number used in calculating how many threads we would use
  /// for IO executor for connectors mainly to do preload/prefetch:
  /// hw_concurrency x multiplier.
  /// If 0.0 then connector preload/prefetch is disabled.
  /// 0.0 is default.
  static constexpr std::string_view kConnectorNumIoThreadsHwMultiplier{
      "connector.num-io-threads-hw-multiplier"};

  /// Floating point number used in calculating how many threads we would use
  /// for Driver CPU executor: hw_concurrency x multiplier. 4.0 is default.
  static constexpr std::string_view kDriverNumCpuThreadsHwMultiplier{
      "driver.num-cpu-threads-hw-multiplier"};

  /// Run driver threads with the SCHED_BATCH scheduling policy. Linux only.
  /// https://man7.org/linux/man-pages/man7/sched.7.html
  static constexpr std::string_view kDriverThreadsBatchSchedulingEnabled{
      "driver.threads-batch-scheduling-enabled"};

  /// Time duration threshold used to detect if an operator call in driver is
  /// stuck or not.  If any of the driver thread is detected as stuck by this
  /// standard, we take the worker offline and further investigation on the
  /// worker is required.
  static constexpr std::string_view kDriverStuckOperatorThresholdMs{
      "driver.stuck-operator-threshold-ms"};

  /// Immediately cancels any Task when it is detected that it has at least one
  /// stuck Operator for at least the time specified by this threshold.
  /// Use zero to disable canceling.
  static constexpr std::string_view
      kDriverCancelTasksWithStuckOperatorsThresholdMs{
          "driver.cancel-tasks-with-stuck-operators-threshold-ms"};

  /// The number of stuck operators (effectively stuck driver threads) when we
  /// detach the worker from the cluster in an attempt to keep the cluster
  /// operational.
  static constexpr std::string_view kDriverNumStuckOperatorsToDetachWorker{
      "driver.num-stuck-operators-to-detach-worker"};

  /// Floating point number used in calculating how many threads we would use
  /// for Spiller CPU executor: hw_concurrency x multiplier.
  /// If 0.0 then spilling is disabled.
  /// 1.0 is default.
  static constexpr std::string_view kSpillerNumCpuThreadsHwMultiplier{
      "spiller.num-cpu-threads-hw-multiplier"};
  /// Config used to create spill files. This config is provided to underlying
  /// file system and the config is free form. The form should be defined by the
  /// underlying file system.
  static constexpr std::string_view kSpillerFileCreateConfig{
      "spiller.file-create-config"};

  /// Config used to create spill directories. This config is provided to
  /// underlying file system and the config is free form. The form should be
  /// defined by the underlying file system.
  static constexpr std::string_view kSpillerDirectoryCreateConfig{
      "spiller.directory-create-config"};

  static constexpr std::string_view kSpillerSpillPath{
      "experimental.spiller-spill-path"};
  static constexpr std::string_view kShutdownOnsetSec{"shutdown-onset-sec"};

  /// Memory allocation limit enforced via internal memory allocator.
  static constexpr std::string_view kSystemMemoryGb{"system-memory-gb"};

  /// Indicates if the process is configured as a sidecar.
  static constexpr std::string_view kNativeSidecar{"native-sidecar"};

  /// If true, enable memory pushback when the server is under low memory
  /// condition. This only applies if 'system-mem-limit-gb' is set.
  static constexpr std::string_view kSystemMemPushbackEnabled{
      "system-mem-pushback-enabled"};
  /// Specifies the system memory limit. Used to trigger memory pushback or heap
  /// dump. A value of zero means no limit is set.
  static constexpr std::string_view kSystemMemLimitGb{"system-mem-limit-gb"};
  /// Specifies the memory to shrink when memory pushback is triggered to help
  /// get the server out of low memory condition. This only applies if
  /// 'system-mem-pushback-enabled' is true.
  static constexpr std::string_view kSystemMemShrinkGb{"system-mem-shrink-gb"};
  /// If true, memory pushback will abort queries with the largest memory
  /// usage under low memory condition. This only applies if
  /// 'system-mem-pushback-enabled' is set.
  static constexpr std::string_view kSystemMemPushbackAbortEnabled{
      "system-mem-pushback-abort-enabled"};

  /// Memory threshold in GB above which the worker is considered overloaded.
  /// Ignored if zero. Default is zero.
  static constexpr std::string_view kWorkerOverloadedThresholdMemGb{
      "worker-overloaded-threshold-mem-gb"};
  /// CPU threshold in % above which the worker is considered overloaded.
  /// Ignored if zero. Default is zero.
  static constexpr std::string_view kWorkerOverloadedThresholdCpuPct{
      "worker-overloaded-threshold-cpu-pct"};
  /// Specifies how many seconds worker has to be not overloaded (in terms of
  /// memory and CPU) before its status changes to not overloaded.
  /// This is to prevent spiky fluctuation of the overloaded status.
  static constexpr std::string_view kWorkerOverloadedCooldownPeriodSec{
      "worker-overloaded-cooldown-period-sec"};
  /// If true, the worker starts queuing new tasks when overloaded, and
  /// starts them gradually when it stops being overloaded.
  static constexpr std::string_view kWorkerOverloadedTaskQueuingEnabled{
      "worker-overloaded-task-queuing-enabled"};

  /// If true, memory allocated via malloc is periodically checked and a heap
  /// profile is dumped if usage exceeds 'malloc-heap-dump-gb-threshold'.
  static constexpr std::string_view kMallocMemHeapDumpEnabled{
      "malloc-mem-heap-dump-enabled"};

  /// Specifies the threshold in GigaBytes of memory allocated via malloc, above
  /// which a heap dump will be triggered. This only applies if
  /// 'malloc-mem-heap-dump-enabled' is true.
  static constexpr std::string_view kMallocHeapDumpThresholdGb{
      "malloc-heap-dump-threshold-gb"};

  /// Specifies the min interval in seconds between consecutive heap dumps. This
  /// only applies if 'malloc-mem-heap-dump-enabled' is true.
  static constexpr std::string_view kMallocMemMinHeapDumpInterval{
      "malloc-mem-min-heap-dump-interval"};

  /// Specifies the max number of latest heap profiles to keep. This only
  /// applies if 'malloc-mem-heap-dump-enabled' is true.
  static constexpr std::string_view kMallocMemMaxHeapDumpFiles{
      "malloc-mem-max-heap-dump-files"};

  static constexpr std::string_view kAsyncDataCacheEnabled{
      "async-data-cache-enabled"};
  static constexpr std::string_view kAsyncCacheSsdGb{"async-cache-ssd-gb"};
  static constexpr std::string_view kAsyncCacheSsdCheckpointGb{
      "async-cache-ssd-checkpoint-gb"};
  static constexpr std::string_view kAsyncCacheSsdPath{"async-cache-ssd-path"};

  /// The max ratio of the number of in-memory cache entries being written to
  /// SSD cache over the total number of cache entries. This is to control SSD
  /// cache write rate, and once the ratio exceeds this threshold, then we
  /// stop writing to SSD cache.
  static constexpr std::string_view kAsyncCacheMaxSsdWriteRatio{
      "async-cache-max-ssd-write-ratio"};

  /// The min ratio of SSD savable (in-memory) cache space over the total
  /// cache space. Once the ratio exceeds this limit, we start writing SSD
  /// savable cache entries into SSD cache.
  static constexpr std::string_view kAsyncCacheSsdSavableRatio{
      "async-cache-ssd-savable-ratio"};

  /// Min SSD savable (in-memory) cache space to start writing SSD savable
  /// cache entries into SSD cache.
  /// NOTE: we only write to SSD cache when both above conditions are satisfied.
  static constexpr std::string_view kAsyncCacheMinSsdSavableBytes{
      "async-cache-min-ssd-savable-bytes"};

  /// The interval for persisting in-memory cache to SSD. Setting this config
  /// to a non-zero value will activate periodic cache persistence.
  static constexpr std::string_view kAsyncCachePersistenceInterval{
      "async-cache-persistence-interval"};

  /// In file systems, such as btrfs, supporting cow (copy on write), the ssd
  /// cache can use all ssd space and stop working. To prevent that, use this
  /// option to disable cow for cache files.
  static constexpr std::string_view kAsyncCacheSsdDisableFileCow{
      "async-cache-ssd-disable-file-cow"};
  /// When enabled, a CRC-based checksum is calculated for each cache entry
  /// written to SSD. The checksum is stored in the next checkpoint file.
  static constexpr std::string_view kSsdCacheChecksumEnabled{
      "ssd-cache-checksum-enabled"};
  /// When enabled, the checksum is recalculated and verified against the stored
  /// value when cache data is loaded from the SSD.
  static constexpr std::string_view kSsdCacheReadVerificationEnabled{
      "ssd-cache-read-verification-enabled"};
  static constexpr std::string_view kEnableSerializedPageChecksum{
      "enable-serialized-page-checksum"};

  /// Enable TTL for AsyncDataCache and SSD cache.
  static constexpr std::string_view kCacheVeloxTtlEnabled{
      "cache.velox.ttl-enabled"};
  /// TTL duration for AsyncDataCache and SSD cache entries.
  static constexpr std::string_view kCacheVeloxTtlThreshold{
      "cache.velox.ttl-threshold"};
  /// The periodic duration to apply cache TTL and evict AsyncDataCache and SSD
  /// cache entries.
  static constexpr std::string_view kCacheVeloxTtlCheckInterval{
      "cache.velox.ttl-check-interval"};
  static constexpr std::string_view kUseMmapAllocator{"use-mmap-allocator"};

  /// Number of pages in largest size class in MallocAllocator. This is used to
  /// optimize MmapAllocator performance for query workloads with large memory
  /// allocation size.
  static constexpr std::string_view kLargestSizeClassPages{
      "largest-size-class-pages"};

  static constexpr std::string_view kEnableRuntimeMetricsCollection{
      "runtime-metrics-collection-enabled"};

  /// Specifies the total amount of memory in GB that the queries can use on a
  /// single worker node. It should be configured to be less than the total
  /// system memory capacity ('system-memory-gb') such that there is enough room
  /// left for the system (as opposed to for the queries), such as disk spilling
  /// and cache prefetch.
  ///
  /// NOTE: the query memory capacity is enforced by memory arbitrator so that
  /// this config only applies if the memory arbitration has been enabled.
  static constexpr std::string_view kQueryMemoryGb{"query-memory-gb"};

  /// Specifies the memory arbitrator kind. If it is empty, then there is no
  /// memory arbitration.
  static constexpr std::string_view kMemoryArbitratorKind{
      "memory-arbitrator-kind"};

  /// Specifies the total amount of memory reserved for the queries on a single
  /// worker node. A query can only allocate from this reserved space if 1) the
  /// non-reserved space in "query-memory-gb" is used up; and 2) the amount it
  /// tries to get is less than
  /// 'shared-arbitrator.memory-pool-reserved-capacity'.
  ///
  /// NOTE: the reserved query memory capacity is enforced by shared arbitrator
  /// so that this config only applies if memory arbitration is enabled.
  static constexpr std::string_view kSharedArbitratorReservedCapacity{
      "shared-arbitrator.reserved-capacity"};

  /// The initial memory pool capacity in bytes allocated on creation.
  static constexpr std::string_view kSharedArbitratorMemoryPoolInitialCapacity{
      "shared-arbitrator.memory-pool-initial-capacity"};

  /// If true, it allows shared arbitrator to reclaim used memory across query
  /// memory pools.
  static constexpr std::string_view kSharedArbitratorGlobalArbitrationEnabled{
      "shared-arbitrator.global-arbitration-enabled"};

  /// The amount of memory in bytes reserved for each query memory pool. When
  /// a query tries to allocate memory from the reserved space whose size is
  /// specified by 'shared-arbitrator.reserved-capacity', it cannot allocate
  /// more than the value specified in
  /// 'shared-arbitrator.memory-pool-reserved-capacity'.
  static constexpr std::string_view kSharedArbitratorMemoryPoolReservedCapacity{
      "shared-arbitrator.memory-pool-reserved-capacity"};

  /// Specifies the max time to wait for memory reclaim by arbitration. The
  /// memory reclaim might fail if the max wait time has exceeded. If it is
  /// zero, then there is no timeout.
  static constexpr std::string_view kSharedArbitratorMaxMemoryArbitrationTime{
      "shared-arbitrator.max-memory-arbitration-time"};

  /// When shared arbitrator grows memory pool's capacity, the growth bytes will
  /// be adjusted in the following way:
  ///  - If 2 * current capacity is less than or equal to
  ///    'shared-arbitrator.fast-exponential-growth-capacity-limit', grow
  ///    through fast path by at least doubling the current capacity, when
  ///    conditions allow (see below NOTE section).
  ///  - If 2 * current capacity is greater than
  ///    'shared-arbitrator.fast-exponential-growth-capacity-limit', grow
  ///    through slow path by growing capacity by at least
  ///    'shared-arbitrator.slow-capacity-grow-pct' * current capacity if
  ///    allowed (see below NOTE section).
  ///
  /// NOTE: If original requested growth bytes is larger than the adjusted
  /// growth bytes or adjusted growth bytes reaches max capacity limit, the
  /// adjusted growth bytes will not be respected.
  ///
  /// NOTE: Capacity growth adjust is only enabled if both
  /// 'shared-arbitrator.fast-exponential-growth-capacity-limit' and
  /// 'shared-arbitrator.slow-capacity-grow-pct' are set, otherwise it is
  /// disabled.
  static constexpr std::string_view
      kSharedArbitratorFastExponentialGrowthCapacityLimit{
          "shared-arbitrator.fast-exponential-growth-capacity-limit"};
  static constexpr std::string_view kSharedArbitratorSlowCapacityGrowPct{
      "shared-arbitrator.slow-capacity-grow-pct"};

  /// When shared arbitrator shrinks memory pool's capacity, the shrink bytes
  /// will be adjusted in a way such that AFTER shrink, the stricter (whichever
  /// is smaller) of the following conditions is met, in order to better fit the
  /// pool's current memory usage:
  /// - Free capacity is greater or equal to capacity *
  /// 'shared-arbitrator.memory-pool-min-free-capacity-pct'
  /// - Free capacity is greater or equal to
  /// 'shared-arbitrator.memory-pool-min-free-capacity'
  ///
  /// NOTE: In the conditions when original requested shrink bytes ends up
  /// with more free capacity than above 2 conditions, the adjusted shrink
  /// bytes is not respected.
  ///
  /// NOTE: Capacity shrink adjustment is enabled when both
  /// 'shared-arbitrator.memory-pool-min-free-capacity-pct' and
  /// 'shared-arbitrator.memory-pool-min-free-capacity' are set.
  static constexpr std::string_view kSharedArbitratorMemoryPoolMinFreeCapacity{
      "shared-arbitrator.memory-pool-min-free-capacity"};
  static constexpr std::string_view
      kSharedArbitratorMemoryPoolMinFreeCapacityPct{
          "shared-arbitrator.memory-pool-min-free-capacity-pct"};

  /// Specifies the starting memory capacity limit for global arbitration to
  /// search for victim participant to reclaim used memory by abort. For
  /// participants with capacity larger than the limit, the global arbitration
  /// choose to abort the youngest participant which has the largest
  /// participant id. This helps to let the old queries to run to completion.
  /// The abort capacity limit is reduced by half if could not find a victim
  /// participant until this reaches to zero.
  ///
  /// NOTE: the limit must be zero or a power of 2.
  static constexpr std::string_view
      kSharedArbitratorMemoryPoolAbortCapacityLimit{
          "shared-arbitrator.memory-pool-abort-capacity-limit"};

  /// Specifies the minimum bytes to reclaim from a participant at a time. The
  /// global arbitration also avoids to reclaim from a participant if its
  /// reclaimable used capacity is less than this threshold. This is to
  /// prevent inefficient memory reclaim operations on a participant with
  /// small reclaimable used capacity which could causes a large number of
  /// small spilled file on disk.
  static constexpr std::string_view kSharedArbitratorMemoryPoolMinReclaimBytes{
      "shared-arbitrator.memory-pool-min-reclaim-bytes"};

  /// Floating point number used in calculating how many threads we would use
  /// for memory reclaim execution: hw_concurrency x multiplier. 0.5 is
  /// default.
  static constexpr std::string_view
      kSharedArbitratorMemoryReclaimThreadsHwMultiplier{
          "shared-arbitrator.memory-reclaim-threads-hw-multiplier"};

  /// If not zero, specifies the minimum amount of memory to reclaim by global
  /// memory arbitration as percentage of total arbitrator memory capacity.
  static constexpr std::string_view
      kSharedArbitratorGlobalArbitrationMemoryReclaimPct{
          "shared-arbitrator.global-arbitration-memory-reclaim-pct"};

  /// The ratio used with 'shared-arbitrator.memory-reclaim-max-wait-time',
  /// beyond which, global arbitration will no longer reclaim memory by
  /// spilling, but instead directly abort. It is only in effect when
  /// 'global-arbitration-enabled' is true
  static constexpr std::string_view
      kSharedArbitratorGlobalArbitrationAbortTimeRatio{
          "shared-arbitrator.global-arbitration-abort-time-ratio"};

  /// If true, global arbitration will not reclaim memory by spilling, but
  /// only by aborting. This flag is only effective if
  /// 'shared-arbitrator.global-arbitration-enabled' is true
  static constexpr std::string_view
      kSharedArbitratorGlobalArbitrationWithoutSpill{
          "shared-arbitrator.global-arbitration-without-spill"};

  /// Enables the memory usage tracking for the system memory pool used for
  /// cases such as disk spilling.
  static constexpr std::string_view kEnableSystemMemoryPoolUsageTracking{
      "enable_system_memory_pool_usage_tracking"};
  static constexpr std::string_view kEnableVeloxTaskLogging{
      "enable_velox_task_logging"};
  static constexpr std::string_view kEnableVeloxExprSetLogging{
      "enable_velox_expression_logging"};
  static constexpr std::string_view kLocalShuffleMaxPartitionBytes{
      "shuffle.local.max-partition-bytes"};
  static constexpr std::string_view kShuffleName{"shuffle.name"};
  static constexpr std::string_view kHttpEnableAccessLog{
      "http-server.enable-access-log"};
  static constexpr std::string_view kHttpEnableStatsFilter{
      "http-server.enable-stats-filter"};
  static constexpr std::string_view kHttpEnableEndpointLatencyFilter{
      "http-server.enable-endpoint-latency-filter"};

  /// The options to configure the max quantized memory allocation size to store
  /// the received http response data.
  static constexpr std::string_view kHttpMaxAllocateBytes{
      "http-server.max-response-allocate-bytes"};
  static constexpr std::string_view kQueryMaxMemoryPerNode{
      "query.max-memory-per-node"};

  /// This system property is added for not crashing the cluster when memory
  /// leak is detected. The check should be disabled in production cluster.
  static constexpr std::string_view kEnableMemoryLeakCheck{
      "enable-memory-leak-check"};

  /// Terminates the process and generates a core file on an allocation failure
  static constexpr std::string_view kCoreOnAllocationFailureEnabled{
      "core-on-allocation-failure-enabled"};

  /// Do not include runtime stats in the returned task info if the task is
  /// in running state.
  static constexpr std::string_view kSkipRuntimeStatsInRunningTaskInfo{
      "skip-runtime-stats-in-running-task-info"};

  static constexpr std::string_view kLogZombieTaskInfo{"log-zombie-task-info"};
  static constexpr std::string_view kLogNumZombieTasks{"log-num-zombie-tasks"};

  /// Time (ms) since the task execution ended, when task is considered old for
  /// cleanup.
  static constexpr std::string_view kOldTaskCleanUpMs{"old-task-cleanup-ms"};

  /// Enable periodic old task clean up. Typically enabled for presto (default)
  /// and disabled for presto-on-spark.
  static constexpr std::string_view kEnableOldTaskCleanUp{
      "enable-old-task-cleanup"};

  static constexpr std::string_view kAnnouncementMaxFrequencyMs{
      "announcement-max-frequency-ms"};

  /// Time (ms) after which we periodically send heartbeats to discovery
  /// endpoint.
  static constexpr std::string_view kHeartbeatFrequencyMs{
      "heartbeat-frequency-ms"};

  static constexpr std::string_view kExchangeMaxErrorDuration{
      "exchange.max-error-duration"};

  /// If true, copy proxygen iobufs to velox memory pool, otherwise not. The
  /// presto exchange source builds the serialized presto page from proxygen
  /// iobufs directly.
  static constexpr std::string_view kExchangeEnableBufferCopy{
      "exchange.enable-buffer-copy"};

  /// Enable to make immediate buffer memory transfer in the handling IO threads
  /// as soon as exchange gets its response back. Otherwise the memory transfer
  /// will happen later in driver thread pool.
  ///
  /// NOTE: this only applies if 'exchange.no-buffer-copy' is false.
  static constexpr std::string_view kExchangeImmediateBufferTransfer{
      "exchange.immediate-buffer-transfer"};

  /// Specifies the timeout duration from exchange client's http connect
  /// success to response reception.
  static constexpr std::string_view kExchangeRequestTimeout{
      "exchange.http-client.request-timeout"};

  /// Specifies the timeout duration from exchange client's http connect
  /// initiation to connect success. Set to 0 to have no timeout.
  static constexpr std::string_view kExchangeConnectTimeout{
      "exchange.http-client.connect-timeout"};

  /// Whether connection pool should be enabled for exchange HTTP client.
  static constexpr std::string_view kExchangeEnableConnectionPool{
      "exchange.http-client.enable-connection-pool"};

  /// Floating point number used in calculating how many threads we would use
  /// for Exchange HTTP client IO executor: hw_concurrency x multiplier.
  /// 1.0 is default.
  static constexpr std::string_view kExchangeHttpClientNumIoThreadsHwMultiplier{
      "exchange.http-client.num-io-threads-hw-multiplier"};

  /// Floating point number used in calculating how many threads we would use
  /// for Exchange HTTP client CPU executor: hw_concurrency x multiplier.
  /// 1.0 is default.
  static constexpr std::string_view
      kExchangeHttpClientNumCpuThreadsHwMultiplier{
          "exchange.http-client.num-cpu-threads-hw-multiplier"};

  /// The maximum timeslice for a task on thread if there are threads queued.
  static constexpr std::string_view kTaskRunTimeSliceMicros{
      "task-run-timeslice-micros"};

  static constexpr std::string_view kIncludeNodeInSpillPath{
      "include-node-in-spill-path"};

  /// Remote function server configs.

  /// Port used by the remote function thrift server.
  static constexpr std::string_view kRemoteFunctionServerThriftPort{
      "remote-function-server.thrift.port"};

  /// Address (ip or hostname) used by the remote function thrift server
  /// (fallback to localhost if not specified).
  static constexpr std::string_view kRemoteFunctionServerThriftAddress{
      "remote-function-server.thrift.address"};

  /// UDS (unix domain socket) path used by the remote function thrift server.
  static constexpr std::string_view kRemoteFunctionServerThriftUdsPath{
      "remote-function-server.thrift.uds-path"};

  /// Path where json files containing signatures for remote functions can be
  /// found.
  static constexpr std::string_view
      kRemoteFunctionServerSignatureFilesDirectoryPath{
          "remote-function-server.signature.files.directory.path"};

  /// Optional catalog name to be added as a prefix to the function names
  /// registered. The pattern registered is `catalog.schema.function_name`.
  static constexpr std::string_view kRemoteFunctionServerCatalogName{
      "remote-function-server.catalog-name"};

  /// Optional string containing the serialization/deserialization format to be
  /// used when communicating with the remote server. Supported types are
  /// "spark_unsafe_row" or "presto_page" ("presto_page" by default).
  static constexpr std::string_view kRemoteFunctionServerSerde{
      "remote-function-server.serde"};

  /// Options to configure the internal (in-cluster) JWT authentication.
  static constexpr std::string_view kInternalCommunicationJwtEnabled{
      "internal-communication.jwt.enabled"};
  static constexpr std::string_view kInternalCommunicationSharedSecret{
      "internal-communication.shared-secret"};
  static constexpr std::string_view kInternalCommunicationJwtExpirationSeconds{
      "internal-communication.jwt.expiration-seconds"};

  /// Below are the Presto properties from config.properties that get converted
  /// to their velox counterparts in BaseVeloxQueryConfig and used solely from
  /// BaseVeloxQueryConfig.

  /// Uses legacy version of array_agg which ignores nulls.
  static constexpr std::string_view kUseLegacyArrayAgg{
      "deprecated.legacy-array-agg"};
  static constexpr std::string_view kSinkMaxBufferSize{"sink.max-buffer-size"};
  static constexpr std::string_view kDriverMaxPagePartitioningBufferSize{
      "driver.max-page-partitioning-buffer-size"};
  static constexpr std::string_view kPlanValidatorFailOnNestedLoopJoin{
      "velox-plan-validator-fail-on-nested-loop-join"};

  // Specifies the default Presto namespace prefix.
  static constexpr std::string_view kPrestoDefaultNamespacePrefix{
      "presto.default-namespace"};

  // Specifies the type of worker pool
  static constexpr std::string_view kPoolType{"pool-type"};

  // Spill related configs
  static constexpr std::string_view kSpillEnabled{"spill-enabled"};
  static constexpr std::string_view kJoinSpillEnabled{"join-spill-enabled"};
  static constexpr std::string_view kAggregationSpillEnabled{
      "aggregation-spill-enabled"};
  static constexpr std::string_view kOrderBySpillEnabled{
      "order-by-spill-enabled"};

  // Max wait time for exchange request in seconds.
  static constexpr std::string_view kRequestDataSizesMaxWaitSec{
    "exchange.http-client.request-data-sizes-max-wait-sec"};

  SystemConfig();

  virtual ~SystemConfig() = default;

  static SystemConfig* instance();

  int httpServerHttpPort() const;

  bool httpServerReusePort() const;

  bool httpServerBindToNodeInternalAddressOnlyEnabled() const;

  bool httpServerHttpsEnabled() const;

  int httpServerHttpsPort() const;

  /// A list of ciphers (comma separated) that are supported by
  /// server and client. Note Java and folly::SSLContext use different names to
  /// refer to the same cipher. For e.g. TLS_RSA_WITH_AES_256_GCM_SHA384 in Java
  /// and AES256-GCM-SHA384 in folly::SSLContext. More details can be found
  /// here: https://www.openssl.org/docs/manmaster/man1/openssl-ciphers.html.
  /// The ciphers enable worker to worker, worker to coordinator and coordinator
  /// to worker communication. At least one cipher needs to be shared for the
  /// above 3 communication to work.
  std::string httpsSupportedCiphers() const;

  /// Note: Java packages cert and key in combined JKS file. But CPP requires
  /// them separately. The HTTPS provides integrity and not
  /// security(authentication/authorization). But the HTTPS will protect against
  /// data corruption by bad router and man in middle attacks.
  folly::Optional<std::string> httpsCertPath() const;

  folly::Optional<std::string> httpsKeyPath() const;

  /// Http client expects the cert and key file to be packed into a single file
  /// (most commonly .pem format) The file should not be password protected. If
  /// required, break this down to 3 configs one for cert,key and password
  /// later.
  folly::Optional<std::string> httpsClientCertAndKeyPath() const;

  bool mutableConfig() const;

  std::string prestoVersion() const;

  folly::Optional<std::string> discoveryUri() const;

  folly::Optional<folly::SocketAddress> remoteFunctionServerLocation() const;

  folly::Optional<std::string> remoteFunctionServerSignatureFilesDirectoryPath()
      const;

  std::string remoteFunctionServerCatalogName() const;

  std::string remoteFunctionServerSerde() const;

  int32_t maxDriversPerTask() const;

  folly::Optional<int32_t> taskWriterCount() const;

  folly::Optional<int32_t> taskPartitionedWriterCount() const;

  int32_t concurrentLifespansPerTask() const;

  double httpServerNumIoThreadsHwMultiplier() const;

  double httpServerNumCpuThreadsHwMultiplier() const;

  double exchangeHttpClientNumIoThreadsHwMultiplier() const;

  double exchangeHttpClientNumCpuThreadsHwMultiplier() const;

  double connectorNumCpuThreadsHwMultiplier() const;

  double connectorNumIoThreadsHwMultiplier() const;

  double driverNumCpuThreadsHwMultiplier() const;

  bool driverThreadsBatchSchedulingEnabled() const;

  size_t driverStuckOperatorThresholdMs() const;

  size_t driverCancelTasksWithStuckOperatorsThresholdMs() const;

  size_t driverNumStuckOperatorsToDetachWorker() const;

  double spillerNumCpuThreadsHwMultiplier() const;

  std::string spillerFileCreateConfig() const;

  std::string spillerDirectoryCreateConfig() const;

  folly::Optional<std::string> spillerSpillPath() const;

  int32_t shutdownOnsetSec() const;

  uint32_t systemMemoryGb() const;

  bool systemMemPushbackEnabled() const;

  uint32_t systemMemLimitGb() const;

  uint32_t systemMemShrinkGb() const;

  bool systemMemPushBackAbortEnabled() const;

  uint64_t workerOverloadedThresholdMemGb() const;

  uint32_t workerOverloadedThresholdCpuPct() const;

  uint32_t workerOverloadedCooldownPeriodSec() const;

  bool workerOverloadedTaskQueuingEnabled() const;

  bool mallocMemHeapDumpEnabled() const;

  uint32_t mallocHeapDumpThresholdGb() const;

  uint32_t mallocMemMinHeapDumpInterval() const;

  uint32_t mallocMemMaxHeapDumpFiles() const;

  bool asyncDataCacheEnabled() const;

  bool queryDataCacheEnabledDefault() const;

  uint64_t asyncCacheSsdGb() const;

  uint64_t asyncCacheSsdCheckpointGb() const;

  uint64_t localShuffleMaxPartitionBytes() const;

  std::string asyncCacheSsdPath() const;

  double asyncCacheMaxSsdWriteRatio() const;

  double asyncCacheSsdSavableRatio() const;

  int32_t asyncCacheMinSsdSavableBytes() const;

  std::chrono::duration<double> asyncCachePersistenceInterval() const;

  bool asyncCacheSsdDisableFileCow() const;

  bool ssdCacheChecksumEnabled() const;

  bool ssdCacheReadVerificationEnabled() const;

  std::string shuffleName() const;

  bool enableSerializedPageChecksum() const;

  bool enableVeloxTaskLogging() const;

  bool enableVeloxExprSetLogging() const;

  bool useMmapAllocator() const;

  std::string memoryArbitratorKind() const;

  std::string sharedArbitratorGlobalArbitrationEnabled() const;

  std::string sharedArbitratorReservedCapacity() const;

  std::string sharedArbitratorMemoryPoolReservedCapacity() const;

  std::string sharedArbitratorMaxMemoryArbitrationTime() const;

  std::string sharedArbitratorMemoryPoolInitialCapacity() const;

  std::string sharedArbitratorFastExponentialGrowthCapacityLimit() const;

  std::string sharedArbitratorSlowCapacityGrowPct() const;

  std::string sharedArbitratorMemoryPoolMinFreeCapacity() const;

  std::string sharedArbitratorMemoryPoolMinFreeCapacityPct() const;

  std::string sharedArbitratorMemoryPoolAbortCapacityLimit() const;

  std::string sharedArbitratorMemoryPoolMinReclaimBytes() const;

  std::string sharedArbitratorMemoryReclaimThreadsHwMultiplier() const;

  std::string sharedArbitratorGlobalArbitrationMemoryReclaimPct() const;

  std::string sharedArbitratorGlobalArbitrationAbortTimeRatio() const;

  std::string sharedArbitratorGlobalArbitrationWithoutSpill() const;

  int32_t queryMemoryGb() const;

  bool enableSystemMemoryPoolUsageTracking() const;

  bool enableHttpAccessLog() const;

  bool enableHttpStatsFilter() const;

  bool enableHttpEndpointLatencyFilter() const;

  bool registerTestFunctions() const;

  uint64_t httpMaxAllocateBytes() const;

  uint64_t queryMaxMemoryPerNode() const;

  bool enableMemoryLeakCheck() const;

  bool coreOnAllocationFailureEnabled() const;

  bool skipRuntimeStatsInRunningTaskInfo() const;

  bool logZombieTaskInfo() const;

  uint32_t logNumZombieTasks() const;

  uint64_t announcementMaxFrequencyMs() const;

  uint64_t heartbeatFrequencyMs() const;

  std::chrono::duration<double> exchangeMaxErrorDuration() const;

  std::chrono::duration<double> exchangeRequestTimeoutMs() const;

  std::chrono::duration<double> exchangeConnectTimeoutMs() const;

  bool exchangeEnableConnectionPool() const;

  bool exchangeEnableBufferCopy() const;

  bool exchangeImmediateBufferTransfer() const;

  int32_t taskRunTimeSliceMicros() const;

  bool includeNodeInSpillPath() const;

  int32_t oldTaskCleanUpMs() const;

  bool enableOldTaskCleanUp() const;

  bool internalCommunicationJwtEnabled() const;

  std::string internalCommunicationSharedSecret() const;

  int32_t internalCommunicationJwtExpirationSeconds() const;

  bool useLegacyArrayAgg() const;

  bool cacheVeloxTtlEnabled() const;

  std::chrono::duration<double> cacheVeloxTtlThreshold() const;

  std::chrono::duration<double> cacheVeloxTtlCheckInterval() const;

  int32_t largestSizeClassPages() const;

  bool enableRuntimeMetricsCollection() const;

  bool prestoNativeSidecar() const;

  std::string prestoDefaultNamespacePrefix() const;

  std::string poolType() const;

  bool spillEnabled() const;

  bool joinSpillEnabled() const;

  bool aggregationSpillEnabled() const;

  bool orderBySpillEnabled() const;

  int requestDataSizesMaxWaitSec() const;
};

/// Provides access to node properties defined in node.properties file.
class NodeConfig : public ConfigBase {
 public:
  static constexpr std::string_view kNodeEnvironment{"node.environment"};
  static constexpr std::string_view kNodeId{"node.id"};
  // "node.ip" is Legacy Config. It is replaced with "node.internal-address"
  static constexpr std::string_view kNodeIp{"node.ip"};
  static constexpr std::string_view kNodeInternalAddress{
      "node.internal-address"};
  static constexpr std::string_view kNodeLocation{"node.location"};
  static constexpr std::string_view kNodePrometheusExecutorThreads{"node.prometheus.executor-threads"};

  NodeConfig();

  virtual ~NodeConfig() = default;

  static NodeConfig* instance();

  std::string nodeEnvironment() const;

  int prometheusExecutorThreads() const;

  std::string nodeId() const;

  std::string nodeInternalAddress(
      const std::function<std::string()>& defaultIp = nullptr) const;

  std::string nodeLocation() const;
};

/// Used only in the single instance as the source of the initial properties for
/// velox::QueryConfig. Not designed for actual property access during a query
/// run.
class BaseVeloxQueryConfig : public ConfigBase {
 public:
  BaseVeloxQueryConfig();

  virtual ~BaseVeloxQueryConfig() = default;

  void updateLoadedValues(
      std::unordered_map<std::string, std::string>& values) const override;

  static BaseVeloxQueryConfig* instance();
};

} // namespace facebook::presto
