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
package com.facebook.presto.spark;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestPrestoSparkConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(PrestoSparkConfig.class)
                .setSparkPartitionCountAutoTuneEnabled(true)
                .setInitialSparkPartitionCount(16)
                .setMinSparkInputPartitionCountForAutoTune(100)
                .setMaxSparkInputPartitionCountForAutoTune(1000)
                .setMaxSplitsDataSizePerSparkPartition(new DataSize(2, GIGABYTE))
                .setShuffleOutputTargetAverageRowSize(new DataSize(1, KILOBYTE))
                .setStorageBasedBroadcastJoinEnabled(false)
                .setStorageBasedBroadcastJoinStorage("local")
                .setStorageBasedBroadcastJoinWriteBufferSize(new DataSize(24, MEGABYTE))
                .setSparkBroadcastJoinMaxMemoryOverride(null)
                .setSmileSerializationEnabled(true)
                .setSplitAssignmentBatchSize(1_000_000)
                .setMemoryRevokingThreshold(0)
                .setMemoryRevokingTarget(0)
                .setRetryOnOutOfMemoryBroadcastJoinEnabled(false)
                .setRetryOnOutOfMemoryWithIncreasedMemorySettingsEnabled(false)
                .setOutOfMemoryRetryPrestoSessionProperties("")
                .setOutOfMemoryRetrySparkConfigs("")
                .setAverageInputDataSizePerExecutor(new DataSize(10, GIGABYTE))
                .setMaxExecutorCount(600)
                .setMinExecutorCount(200)
                .setAverageInputDataSizePerPartition(new DataSize(2, GIGABYTE))
                .setMaxHashPartitionCount(4096)
                .setMinHashPartitionCount(1024)
                .setSparkResourceAllocationStrategyEnabled(false)
                .setRetryOnOutOfMemoryWithHigherHashPartitionCountEnabled(false)
                .setHashPartitionCountScalingFactorOnOutOfMemory(2.0)
                .setAdaptiveQueryExecutionEnabled(false)
                .setAdaptiveJoinSideSwitchingEnabled(false)
                .setExecutorAllocationStrategyEnabled(false)
                .setHashPartitionCountAllocationStrategyEnabled(false)
                .setNativeExecutionBroadcastBasePath(null)
                .setNativeTriggerCoredumpWhenUnresponsiveEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("spark.partition-count-auto-tune-enabled", "false")
                .put("spark.initial-partition-count", "128")
                .put("spark.min-spark-input-partition-count-for-auto-tune", "200")
                .put("spark.max-spark-input-partition-count-for-auto-tune", "2000")
                .put("spark.max-splits-data-size-per-partition", "4GB")
                .put("spark.shuffle-output-target-average-row-size", "10kB")
                .put("spark.storage-based-broadcast-join-enabled", "true")
                .put("spark.storage-based-broadcast-join-storage", "tempfs")
                .put("spark.storage-based-broadcast-join-write-buffer-size", "4MB")
                .put("spark.broadcast-join-max-memory-override", "1GB")
                .put("spark.smile-serialization-enabled", "false")
                .put("spark.split-assignment-batch-size", "420")
                .put("spark.memory-revoking-threshold", "0.5")
                .put("spark.memory-revoking-target", "0.5")
                .put("spark.retry-on-out-of-memory-broadcast-join-enabled", "true")
                .put("spark.retry-on-out-of-memory-with-increased-memory-settings-enabled", "true")
                .put("spark.retry-presto-session-properties", "query_max_memory_per_node=1MB,query_max_total_memory_per_node=1MB")
                .put("spark.retry-spark-configs", "spark.executor.memory=1g,spark.task.cpus=5")
                .put("spark.average-input-datasize-per-executor", "5GB")
                .put("spark.max-executor-count", "29")
                .put("spark.min-executor-count", "2")
                .put("spark.average-input-datasize-per-partition", "1GB")
                .put("spark.max-hash-partition-count", "333")
                .put("spark.min-hash-partition-count", "30")
                .put("spark.resource-allocation-strategy-enabled", "true")
                .put("spark.retry-on-out-of-memory-higher-hash-partition-count-enabled", "true")
                .put("spark.hash-partition-count-scaling-factor-on-out-of-memory", "5.6")
                .put("spark.adaptive-query-execution-enabled", "true")
                .put("optimizer.adaptive-join-side-switching-enabled", "true")
                .put("spark.executor-allocation-strategy-enabled", "true")
                .put("spark.hash-partition-count-allocation-strategy-enabled", "true")
                .put("native-execution-broadcast-base-path", "/tmp/broadcast_path")
                .put("native-trigger-coredump-when-unresponsive-enabled", "true")
                .build();
        PrestoSparkConfig expected = new PrestoSparkConfig()
                .setSparkPartitionCountAutoTuneEnabled(false)
                .setInitialSparkPartitionCount(128)
                .setMinSparkInputPartitionCountForAutoTune(200)
                .setMaxSparkInputPartitionCountForAutoTune(2000)
                .setMaxSplitsDataSizePerSparkPartition(new DataSize(4, GIGABYTE))
                .setShuffleOutputTargetAverageRowSize(new DataSize(10, KILOBYTE))
                .setStorageBasedBroadcastJoinEnabled(true)
                .setStorageBasedBroadcastJoinStorage("tempfs")
                .setStorageBasedBroadcastJoinWriteBufferSize(new DataSize(4, MEGABYTE))
                .setSparkBroadcastJoinMaxMemoryOverride(new DataSize(1, GIGABYTE))
                .setSmileSerializationEnabled(false)
                .setSplitAssignmentBatchSize(420)
                .setMemoryRevokingThreshold(0.5)
                .setMemoryRevokingTarget(0.5)
                .setRetryOnOutOfMemoryBroadcastJoinEnabled(true)
                .setRetryOnOutOfMemoryWithIncreasedMemorySettingsEnabled(true)
                .setOutOfMemoryRetryPrestoSessionProperties("query_max_memory_per_node=1MB,query_max_total_memory_per_node=1MB")
                .setOutOfMemoryRetrySparkConfigs("spark.executor.memory=1g,spark.task.cpus=5")
                .setAverageInputDataSizePerExecutor(new DataSize(5, GIGABYTE))
                .setMaxExecutorCount(29)
                .setMinExecutorCount(2)
                .setAverageInputDataSizePerPartition(new DataSize(1, GIGABYTE))
                .setMaxHashPartitionCount(333)
                .setMinHashPartitionCount(30)
                .setSparkResourceAllocationStrategyEnabled(true)
                .setRetryOnOutOfMemoryWithHigherHashPartitionCountEnabled(true)
                .setHashPartitionCountScalingFactorOnOutOfMemory(5.6)
                .setAdaptiveQueryExecutionEnabled(true)
                .setAdaptiveJoinSideSwitchingEnabled(true)
                .setHashPartitionCountAllocationStrategyEnabled(true)
                .setExecutorAllocationStrategyEnabled(true)
                .setNativeExecutionBroadcastBasePath("/tmp/broadcast_path")
                .setNativeTriggerCoredumpWhenUnresponsiveEnabled(true);
        assertFullMapping(properties, expected);
    }
}
