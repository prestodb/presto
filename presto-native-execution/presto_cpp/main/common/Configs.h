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
#include "velox/core/Config.h"

namespace facebook::presto {

#define SYS_CONF_BASE_PROPS(V)                                                \
  /* Setting this to 'true' makes configs modifiable via server operations.*/ \
  V(BOOL, kMutableConfig, "mutable-config", false)

#define SYS_CONF_PROPS(V)                                                         \
  V(NONE, kPrestoVersion, "presto.version", _)                                    \
  V(NONE, kHttpServerHttpPort, "http-server.http.port", _)                        \
  /*                                                                              \
    This option allows a port closed in TIME_WAIT state to be reused              \
    immediately upon worker startup. This property is mainly used by batch        \
    processing. For interactive query, the worker uses a dynamic port upon        \
    startup.                                                                      \
  */                                                                              \
  V(BOOL, kHttpServerReusePort, "http-server.reuse-port", false)                  \
  /*                                                                              \
    By default the server binds to 0.0.0.0                                        \
    With this option enabled the server will bind strictly to the                 \
    address set in node.internal-address property                                 \
  */                                                                              \
  V(BOOL,                                                                         \
    kHttpServerBindToNodeInternalAddressOnlyEnabled,                              \
    "http-server.bind-to-node-internal-address-only-enabled",                     \
    false)                                                                        \
  V(NONE, kDiscoveryUri, "discovery.uri", _)                                      \
  V(NUM, kMaxDriversPerTask, "task.max-drivers-per-task", 16)                     \
  V(NUM, kConcurrentLifespansPerTask, "task.concurrent-lifespans-per-task", 1)    \
  V(STR,                                                                          \
    kTaskMaxPartialAggregationMemory,                                             \
    "task.max-partial-aggregation-memory",                                        \
    "16MB")                                                                       \
  /*                                                                              \
    Floating point number used in calculating how many threads we would use       \
    for HTTP IO executor: hw_concurrency x multiplier. 1.0 is default.            \
  */                                                                              \
  V(NUM,                                                                          \
    kHttpServerNumIoThreadsHwMultiplier,                                          \
    "http-server.num-io-threads-hw-multiplier",                                   \
    1.0)                                                                          \
  /*                                                                              \
    Floating point number used in calculating how many threads we would use       \
    for HTTP CPU executor: hw_concurrency x multiplier. 1.0 is default.           \
  */                                                                              \
  V(NUM,                                                                          \
    kHttpServerNumCpuThreadsHwMultiplier,                                         \
    "http-server.num-cpu-threads-hw-multiplier",                                  \
    1.0)                                                                          \
  V(NONE, kHttpServerHttpsPort, "http-server.https.port", _)                      \
  V(BOOL, kHttpServerHttpsEnabled, "http-server.https.enabled", false)            \
  /*                                                                              \
    List of comma separated ciphers the client can use.                           \
    NOTE: the client needs to have at least one cipher shared with server         \
          to communicate.                                                         \
  */                                                                              \
  V(STR,                                                                          \
    kHttpsSupportedCiphers,                                                       \
    "https-supported-ciphers",                                                    \
    "ECDHE-ECDSA-AES256-GCM-SHA384,AES256-GCM-SHA384")                            \
  V(NONE, kHttpsCertPath, "https-cert-path", _)                                   \
  V(NONE, kHttpsKeyPath, "https-key-path", _)                                     \
  /* Path to a .PEM file with certificate and key concatenated together. */       \
  V(NONE, kHttpsClientCertAndKeyPath, "https-client-cert-key-path", _)            \
  /*                                                                              \
    Floating point number used in calculating how many threads we would use       \
    for IO executor for connectors mainly to do preload/prefetch:                 \
    hw_concurrency x multiplier.                                                  \
    If 0.0 then connector preload/prefetch is disabled.                           \
    0.0 is default.                                                               \
  */                                                                              \
  V(NUM,                                                                          \
    kConnectorNumIoThreadsHwMultiplier,                                           \
    "connector.num-io-threads-hw-multiplier",                                     \
    1.0)                                                                          \
  /*                                                                              \
    Floating point number used in calculating how many threads we would use       \
    for Driver CPU executor: hw_concurrency x multiplier. 4.0 is default.         \
  */                                                                              \
  V(NUM,                                                                          \
    kDriverNumCpuThreadsHwMultiplier,                                             \
    "driver.num-cpu-threads-hw-multiplier",                                       \
    4.0)                                                                          \
  /*                                                                              \
    Time duration threshold used to detect if an operator call in driver is       \
    stuck or not.  If any of the driver thread is detected as stuck by this       \
    standard, we take the worker offline and further investigation on the         \
    worker is required.                                                           \
  */                                                                              \
  V(NUM,                                                                          \
    kDriverStuckOperatorThresholdMs,                                              \
    "driver.stuck-operator-threshold-ms",                                         \
    30 * 60 * 1000)                                                               \
  /*                                                                              \
    Floating point number used in calculating how many threads we would use       \
    for Spiller CPU executor: hw_concurrency x multiplier.                        \
    If 0.0 then spilling is disabled.                                             \
    1.0 is default.                                                               \
  */                                                                              \
  V(NUM,                                                                          \
    kSpillerNumCpuThreadsHwMultiplier,                                            \
    "spiller.num-cpu-threads-hw-multiplier",                                      \
    1.0)                                                                          \
  /*                                                                              \
    Config used to create spill files. This config is provided to underlying      \
    file system and the config is free form. The form should be defined by the    \
    underlying file system.                                                       \
  */                                                                              \
  V(STR, kSpillerFileCreateConfig, "spiller.file-create-config", "")              \
  V(NONE, kSpillerSpillPath, "experimental.spiller-spill-path", _)                \
  V(NUM, kShutdownOnsetSec, "shutdown-onset-sec", 10)                             \
  /* Memory allocation limit enforced via internal memory allocator. */           \
  V(NUM, kSystemMemoryGb, "system-memory-gb", 40)                                 \
  /*                                                                              \
    Specifies the total memory capacity that can be used by query execution in    \
    GB. The query memory capacity should be configured less than the system       \
    memory capacity ('system-memory-gb') to reserve memory for system usage       \
    such as disk spilling and cache prefetch which are not counted in query       \
    memory usage.                                                                 \
                                                                                \ \
    NOTE: the query memory capacity is enforced by memory arbitrator so that      \
    this config only applies if the memory arbitration has been enabled.          \
  */                                                                              \
  V(NUM, kQueryMemoryGb, "query-memory-gb", 38)                                   \
  /*                                                                              \
    If true, enable memory pushback when the server is under low memory           \
    condition. This only applies if 'system-mem-limit-gb' is set.                 \
  */                                                                              \
  V(BOOL, kSystemMemPushbackEnabled, "system-mem-pushback-enabled", false)        \
  /*                                                                              \
    Specifies the system memory limit. Used to trigger memory pushback or heap    \
    dump. A value of zero means no limit is set.                                  \
  */                                                                              \
  V(NUM, kSystemMemLimitGb, "system-mem-limit-gb", 55)                            \
  /*                                                                              \
    Specifies the memory to shrink when memory pushback is triggered to help      \
    get the server out of low memory condition. This only applies if              \
    'system-mem-pushback-enabled' is true.                                        \
  */                                                                              \
  V(NUM, kSystemMemShrinkGb, "system-mem-shrink-gb", 8)                           \
  /*                                                                              \
    If true, memory pushback will quickly abort queries with the most memory      \
    usage under low memory condition. This only applies if                        \
    'system-mem-pushback-enabled' is set.                                         \
  */                                                                              \
  V(BOOL,                                                                         \
    kSystemMemPushbackAbortEnabled,                                               \
    "system-mem-pushback-abort-enabled",                                          \
    false)                                                                        \
  /*                                                                              \
    If true, memory allocated via malloc is periodically checked and a heap       \
    profile is dumped if usage exceeds 'malloc-heap-dump-gb-threshold'.           \
  */                                                                              \
  V(BOOL, kMallocMemHeapDumpEnabled, "malloc-mem-heap-dump-enabled", false)       \
  /*                                                                              \
    Specifies the threshold in GigaBytes of memory allocated via malloc, above    \
    which a heap dump will be triggered. This only applies if                     \
    'malloc-mem-heap-dump-enabled' is true.                                       \
  */                                                                              \
  V(NUM, kMallocHeapDumpThresholdGb, "malloc-heap-dump-threshold-gb", 20)         \
  /*                                                                              \
    Specifies the min interval in seconds between consecutive heap dumps. This    \
    only applies if 'malloc-mem-heap-dump-enabled' is true.                       \
  */                                                                              \
  V(NUM,                                                                          \
    kMallocMemMinHeapDumpInterval,                                                \
    "malloc-mem-min-heap-dump-interval",                                          \
    10)                                                                           \
  /*                                                                              \
    Specifies the max number of latest heap profiles to keep. This only           \
    applies if 'malloc-mem-heap-dump-enabled' is true.                            \
  */                                                                              \
  V(NUM, kMallocMemMaxHeapDumpFiles, "malloc-mem-max-heap-dump-files", 5)         \
  V(BOOL, kAsyncDataCacheEnabled, "async-data-cache-enabled", true)               \
  V(NUM, kAsyncCacheSsdGb, "async-cache-ssd-gb", 0)                               \
  V(NUM, kAsyncCacheSsdCheckpointGb, "async-cache-ssd-checkpoint-gb", 0)          \
  V(STR,                                                                          \
    kAsyncCacheSsdPath,                                                           \
    "async-cache-ssd-path",                                                       \
    "/mnt/flash/async_cache.")                                                    \
  /*                                                                              \
    In file systems, such as btrfs, supporting cow (copy on write), the ssd       \
    cache can use all ssd space and stop working. To prevent that, use this       \
    option to disable cow for cache files.                                        \
  */                                                                              \
  V(BOOL,                                                                         \
    kAsyncCacheSsdDisableFileCow,                                                 \
    "async-cache-ssd-disable-file-cow",                                           \
    false)                                                                        \
  V(BOOL,                                                                         \
    kEnableSerializedPageChecksum,                                                \
    "enable-serialized-page-checksum",                                            \
    true)                                                                         \
  /* Enable TTL for AsyncDataCache and SSD cache. */                              \
  V(BOOL, kCacheVeloxTtlEnabled, "cache.velox.ttl-enabled", false)                \
  /* TTL duration for AsyncDataCache and SSD cache entries. */                    \
  V(STR, kCacheVeloxTtlThreshold, "cache.velox.ttl-threshold", "2d")              \
  /*                                                                              \
    The periodic duration to apply cache TTL and evict AsyncDataCache and SSD     \
    cache entries.                                                                \
  */                                                                              \
  V(STR, kCacheVeloxTtlCheckInterval, "cache.velox.ttl-check-interval", "1h")     \
  V(BOOL, kUseMmapAllocator, "use-mmap-allocator", true)                          \
  V(BOOL,                                                                         \
    kEnableRuntimeMetricsCollection,                                              \
    "runtime-metrics-collection-enabled",                                         \
    false)                                                                        \
  /*                                                                              \
    Specifies the memory arbitrator kind. If it is empty, then there is no        \
    memory arbitration.                                                           \
  */                                                                              \
  V(STR, kMemoryArbitratorKind, "memory-arbitrator-kind", "")                     \
  /*                                                                              \
    The initial memory pool capacity in bytes allocated on creation.              \
    NOTE: this config only applies if the memory arbitration has been enabled.    \
  */                                                                              \
  V(NUM, kMemoryPoolInitCapacity, "memory-pool-init-capacity", 128 << 20)         \
  /*                                                                              \
    The minimal memory capacity in bytes transferred between memory pools         \
    during memory arbitration.                                                    \
    NOTE: this config only applies if the memory arbitration has been enabled.    \
  */                                                                              \
  V(NUM,                                                                          \
    kMemoryPoolTransferCapacity,                                                  \
    "memory-pool-transfer-capacity",                                              \
    32 << 20)                                                                     \
  /*                                                                              \
    Specifies the max time to wait for memory reclaim by arbitration. The         \
    memory reclaim might fail if the max wait time has exceeded. If it is         \
    zero, then there is no timeout.                                               \
    NOTE: this config only applies if the memory arbitration has been enabled.    \
  */                                                                              \
  V(NUM, kMemoryReclaimWaitMs, "memory-reclaim-wait-ms", 300'000)                 \
  /*                                                                              \
    Enables the memory usage tracking for the system memory pool used for         \
    cases such as disk spilling.                                                  \
  */                                                                              \
  V(BOOL,                                                                         \
    kEnableSystemMemoryPoolUsageTracking,                                         \
    "enable_system_memory_pool_usage_tracking",                                   \
    false)                                                                        \
  V(BOOL, kEnableVeloxTaskLogging, "enable_velox_task_logging", false)            \
  V(BOOL,                                                                         \
    kEnableVeloxExprSetLogging,                                                   \
    "enable_velox_expression_logging",                                            \
    false)                                                                        \
  V(NUM,                                                                          \
    kLocalShuffleMaxPartitionBytes,                                               \
    "shuffle.local.max-partition-bytes",                                          \
    268435456)                                                                    \
  V(STR, kShuffleName, "shuffle.name", "")                                        \
  V(BOOL, kHttpEnableAccessLog, "http-server.enable-access-log", false)           \
  V(BOOL, kHttpEnableStatsFilter, "http-server.enable-stats-filter", false)       \
  V(BOOL,                                                                         \
    kHttpEnableEndpointLatencyFilter,                                             \
    "http-server.enable-endpoint-latency-filter",                                 \
    false)                                                                        \
  V(BOOL, kRegisterTestFunctions, "register-test-functions", false)               \
  /*                                                                              \
    The options to configure the max quantized memory allocation size to store    \
    the received http response data.                                              \
  */                                                                              \
  V(NUM,                                                                          \
    kHttpMaxAllocateBytes,                                                        \
    "http-server.max-response-allocate-bytes",                                    \
    65536)                                                                        \
  V(STR, kQueryMaxMemoryPerNode, "query.max-memory-per-node", "4GB")              \
  /*                                                                              \
    This system property is added for not crashing the cluster when memory        \
    leak is detected. The check should be disabled in production cluster.         \
  */                                                                              \
  V(BOOL, kEnableMemoryLeakCheck, "enable-memory-leak-check", true)               \
  /*                                                                              \
    Terminates the process and generates a core file on an allocation failure     \
  */                                                                              \
  V(BOOL,                                                                         \
    kCoreOnAllocationFailureEnabled,                                              \
    "core-on-allocation-failure-enabled",                                         \
    false)                                                                        \
  /*                                                                              \
    Do not include runtime stats in the returned task info if the task is         \
    in running state.                                                             \
  */                                                                              \
  V(BOOL,                                                                         \
    kSkipRuntimeStatsInRunningTaskInfo,                                           \
    "skip-runtime-stats-in-running-task-info",                                    \
    true)                                                                         \
  V(BOOL, kLogZombieTaskInfo, "log-zombie-task-info", false)                      \
  V(NUM, kLogNumZombieTasks, "log-num-zombie-tasks", 20)                          \
  /*                                                                              \
    Time (ms) since the task execution ended, when task is considered old for     \
    cleanup.                                                                      \
  */                                                                              \
  V(NUM, kOldTaskCleanUpMs, "old-task-cleanup-ms", 60'000)                        \
  /*                                                                              \
    Enable periodic old task clean up. Typically enabled for presto (default)     \
    and disabled for presto-on-spark.                                             \
  */                                                                              \
  V(BOOL, kEnableOldTaskCleanUp, "enable-old-task-cleanup", true)                 \
  V(NUM,                                                                          \
    kAnnouncementMaxFrequencyMs,                                                  \
    "announcement-max-frequency-ms",                                              \
    30'000) /* 30 seconds */                                                      \
  /*                                                                              \
    Time (ms) after which we periodically send heartbeats to discovery            \
    endpoint.                                                                     \
  */                                                                              \
  V(NUM, kHeartbeatFrequencyMs, "heartbeat-frequency-ms", 0)                      \
  V(STR, kExchangeMaxErrorDuration, "exchange.max-error-duration", "3m")          \
  /*                                                                              \
    Enable to make immediate buffer memory transfer in the handling IO threads    \
    as soon as exchange gets its response back. Otherwise the memory transfer     \
    will happen later in driver thread pool.                                      \
  */                                                                              \
  V(BOOL,                                                                         \
    kExchangeImmediateBufferTransfer,                                             \
    "exchange.immediate-buffer-transfer",                                         \
    true)                                                                         \
  /*                                                                              \
    Specifies the timeout duration from exchange client's http connect            \
    success to response reception.                                                \
  */                                                                              \
  V(STR,                                                                          \
    kExchangeRequestTimeout,                                                      \
    "exchange.http-client.request-timeout",                                       \
    "10s")                                                                        \
  /*                                                                              \
    Specifies the timeout duration from exchange client's http connect            \
    initiation to connect success. Set to 0 to have no timeout.                   \
  */                                                                              \
  V(STR,                                                                          \
    kExchangeConnectTimeout,                                                      \
    "exchange.http-client.connect-timeout",                                       \
    "20s")                                                                        \
  /* Whether connection pool should be enabled for exchange HTTP client. */       \
  V(BOOL,                                                                         \
    kExchangeEnableConnectionPool,                                                \
    "exchange.http-client.enable-connection-pool",                                \
    false)                                                                        \
  /*                                                                              \
    Floating point number used in calculating how many threads we would use       \
    for Exchange HTTP client IO executor: hw_concurrency x multiplier.            \
    1.0 is default.                                                               \
  */                                                                              \
  V(NUM,                                                                          \
    kExchangeHttpClientNumIoThreadsHwMultiplier,                                  \
    "exchange.http-client.num-io-threads-hw-multiplier",                          \
    1.0)                                                                          \
  /* The maximum timeslice for a task on thread if there are threads queued.*/    \
  V(NUM, kTaskRunTimeSliceMicros, "task-run-timeslice-micros", 50'000)            \
  V(BOOL, kIncludeNodeInSpillPath, "include-node-in-spill-path", false)           \
  /* Remote function server configs. */                                           \
  /* Port used by the remote function thrift server. */                           \
  V(NONE,                                                                         \
    kRemoteFunctionServerThriftPort,                                              \
    "remote-function-server.thrift.port",                                         \
    _)                                                                            \
  /*                                                                              \
    Address (ip or hostname) used by the remote function thrift server            \
    (fallback to localhost if not specified).                                     \
  */                                                                              \
  V(NONE,                                                                         \
    kRemoteFunctionServerThriftAddress,                                           \
    "remote-function-server.thrift.address",                                      \
    _)                                                                            \
  /*                                                                              \
    UDS (unix domain socket) path used by the remote function thrift server.      \
  */                                                                              \
  V(NONE,                                                                         \
    kRemoteFunctionServerThriftUdsPath,                                           \
    "remote-function-server.thrift.uds-path",                                     \
    _)                                                                            \
  /*                                                                              \
    Path where json files containing signatures for remote functions can be       \
    found.                                                                        \
  */                                                                              \
  V(NONE,                                                                         \
    kRemoteFunctionServerSignatureFilesDirectoryPath,                             \
    "remote-function-server.signature.files.directory.path",                      \
    _)                                                                            \
  /*                                                                              \
    Optional catalog name to be added as a prefix to the function names           \
    registered. The pattern registered is `catalog.schema.function_name`.         \
  */                                                                              \
  V(STR,                                                                          \
    kRemoteFunctionServerCatalogName,                                             \
    "remote-function-server.catalog-name",                                        \
    "")                                                                           \
  /*                                                                              \
    Optional string containing the serialization/deserialization format to be     \
    used when communicating with the remote server. Supported types are           \
    "spark_unsafe_row" or "presto_page" ("presto_page" by default).               \
  */                                                                              \
  V(STR,                                                                          \
    kRemoteFunctionServerSerde,                                                   \
    "remote-function-server.serde",                                               \
    "presto_page")                                                                \
  /* Options to configure the internal (in-cluster) JWT authentication.*/         \
  V(BOOL,                                                                         \
    kInternalCommunicationJwtEnabled,                                             \
    "internal-communication.jwt.enabled",                                         \
    false)                                                                        \
  V(STR,                                                                          \
    kInternalCommunicationSharedSecret,                                           \
    "internal-communication.shared-secret",                                       \
    "")                                                                           \
  V(NUM,                                                                          \
    kInternalCommunicationJwtExpirationSeconds,                                   \
    "internal-communication.jwt.expiration-seconds",                              \
    300)                                                                          \
  /*                                                                              \
    Below are the Presto properties from config.properties that get converted     \
    to their velox counterparts in BaseVeloxQueryConfig and used solely from      \
    BaseVeloxQueryConfig.                                                         \
  */                                                                              \
  /* Uses legacy version of array_agg which ignores nulls. */                     \
  V(BOOL, kUseLegacyArrayAgg, "deprecated.legacy-array-agg", false)               \
  /* Used solely from BaseVeloxQueryConfig */                                     \
  V(STR, kSinkMaxBufferSize, "sink.max-buffer-size", "32MB")                      \
  V(STR,                                                                          \
    kDriverMaxPagePartitioningBufferSize,                                         \
    "driver.max-page-partitioning-buffer-size",                                   \
    "32MB")

class ConfigBase {
 public:
#define CONFIG_DEF(_type_, _name_, _prop_, _value_) \
  static constexpr std::string_view _name_{_prop_};
  SYS_CONF_BASE_PROPS(CONFIG_DEF)
#undef CONFIG_DEF

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
  void initialize(std::unique_ptr<velox::Config>&& config) {
    config_ = std::move(config);
  }

  /// Registers an extra property in the config.
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
    auto propertyValue = config_->get(propertyName);
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
    auto val = config_->get(propertyName);
    if (!val.hasValue()) {
      const auto it = registeredProps_.find(propertyName);
      if (it != registeredProps_.end()) {
        val = it->second;
      }
    }
    if (val.hasValue()) {
      return folly::to<T>(val.value());
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
    auto val = config_->get(propertyName);
    if (!val.hasValue()) {
      const auto it = registeredProps_.find(propertyName);
      if (it != registeredProps_.end()) {
        return it->second;
      }
    }
    return val;
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
    return config_->valuesCopy();
  }

  virtual ~ConfigBase() = default;

 protected:
  ConfigBase();

  // Check if all properties are registered.
  void checkRegisteredProperties(
      const std::unordered_map<std::string, std::string>& values);

  std::unique_ptr<velox::Config> config_;
  std::string filePath_;
  // Map of registered properties with their default values.
  std::unordered_map<std::string, folly::Optional<std::string>>
      registeredProps_;
};

/// Provides access to system properties defined in config.properties file.
class SystemConfig : public ConfigBase {
 public:
#define CONFIG_DEF(_type_, _name_, _prop_, _value_) \
  static constexpr std::string_view _name_{_prop_};
  SYS_CONF_PROPS(CONFIG_DEF)
#undef CONFIG_DEF

  SystemConfig();

  virtual ~SystemConfig() = default;

  static SystemConfig* instance();

  int httpServerHttpPort() const;

  bool httpServerReusePort() const;

  bool httpServerBindToNodeInternalAddressOnlyEnabled() const;

  bool httpServerHttpsEnabled() const;

  int httpServerHttpsPort() const;

  // A list of ciphers (comma separated) that are supported by
  // server and client. Note Java and folly::SSLContext use different names to
  // refer to the same cipher. For e.g. TLS_RSA_WITH_AES_256_GCM_SHA384 in Java
  // and AES256-GCM-SHA384 in folly::SSLContext. More details can be found here:
  // https://www.openssl.org/docs/manmaster/man1/openssl-ciphers.html. The
  // ciphers enable worker to worker, worker to coordinator and
  // coordinator to worker communication. At least one cipher needs to be
  // shared for the above 3 communication to work.
  std::string httpsSupportedCiphers() const;

  // Note: Java packages cert and key in combined JKS file. But CPP requires
  // them separately. The HTTPS provides integrity and not
  // security(authentication/authorization). But the HTTPS will protect against
  // data corruption by bad router and man in middle attacks.
  folly::Optional<std::string> httpsCertPath() const;

  folly::Optional<std::string> httpsKeyPath() const;

  // Http client expects the cert and key file to be packed into a single file
  // (most commonly .pem format) The file should not be password protected. If
  // required, break this down to 3 configs one for cert,key and password later.
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

  int32_t concurrentLifespansPerTask() const;

  double httpServerNumIoThreadsHwMultiplier() const;

  double httpServerNumCpuThreadsHwMultiplier() const;

  double exchangeHttpClientNumIoThreadsHwMultiplier() const;

  double connectorNumIoThreadsHwMultiplier() const;

  double driverNumCpuThreadsHwMultiplier() const;

  size_t driverStuckOperatorThresholdMs() const;

  double spillerNumCpuThreadsHwMultiplier() const;

  std::string spillerFileCreateConfig() const;

  folly::Optional<std::string> spillerSpillPath() const;

  int32_t shutdownOnsetSec() const;

  uint32_t systemMemoryGb() const;

  bool systemMemPushbackEnabled() const;

  uint32_t systemMemLimitGb() const;

  uint32_t systemMemShrinkGb() const;

  bool systemMemPushBackAbortEnabled() const;

  bool mallocMemHeapDumpEnabled() const;

  uint32_t mallocHeapDumpThresholdGb() const;

  uint32_t mallocMemMinHeapDumpInterval() const;

  uint32_t mallocMemMaxHeapDumpFiles() const;

  bool asyncDataCacheEnabled() const;

  uint64_t asyncCacheSsdGb() const;

  uint64_t asyncCacheSsdCheckpointGb() const;

  uint64_t localShuffleMaxPartitionBytes() const;

  std::string asyncCacheSsdPath() const;

  bool asyncCacheSsdDisableFileCow() const;

  std::string shuffleName() const;

  bool enableSerializedPageChecksum() const;

  bool enableVeloxTaskLogging() const;

  bool enableVeloxExprSetLogging() const;

  bool useMmapAllocator() const;

  std::string memoryArbitratorKind() const;

  int32_t queryMemoryGb() const;

  uint64_t memoryPoolInitCapacity() const;

  uint64_t memoryPoolTransferCapacity() const;

  uint64_t memoryReclaimWaitMs() const;

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

  bool enableRuntimeMetricsCollection() const;
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

  NodeConfig();

  virtual ~NodeConfig() = default;

  static NodeConfig* instance();

  std::string nodeEnvironment() const;

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
