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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Map;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class PrestoSparkConfig
{
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings().withKeyValueSeparator('=');

    private boolean sparkPartitionCountAutoTuneEnabled = true;
    private int minSparkInputPartitionCountForAutoTune = 100;
    private int maxSparkInputPartitionCountForAutoTune = 1000;
    private int initialSparkPartitionCount = 16;
    private DataSize maxSplitsDataSizePerSparkPartition = new DataSize(2, GIGABYTE);
    private DataSize shuffleOutputTargetAverageRowSize = new DataSize(1, KILOBYTE);
    private boolean storageBasedBroadcastJoinEnabled;
    private DataSize storageBasedBroadcastJoinWriteBufferSize = new DataSize(24, MEGABYTE);
    private String storageBasedBroadcastJoinStorage = "local";
    private DataSize sparkBroadcastJoinMaxMemoryOverride;
    private boolean smileSerializationEnabled = true;
    private int splitAssignmentBatchSize = 1_000_000;
    private double memoryRevokingThreshold;
    private double memoryRevokingTarget;
    private boolean retryOnOutOfMemoryBroadcastJoinEnabled;
    private boolean retryOnOutOfMemoryWithIncreasedMemorySettingsEnabled;
    private Map<String, String> outOfMemoryRetryPrestoSessionProperties = ImmutableMap.of();
    private Map<String, String> outOfMemoryRetrySparkConfigs = ImmutableMap.of();
    private DataSize averageInputDataSizePerExecutor = new DataSize(10, GIGABYTE);
    private int maxExecutorCount = 600;
    private int minExecutorCount = 200;
    private DataSize averageInputDataSizePerPartition = new DataSize(2, GIGABYTE);
    private int maxHashPartitionCount = 4096;
    private int minHashPartitionCount = 1024;
    private boolean isResourceAllocationStrategyEnabled;
    private boolean nativeEngineExecutionEnabled = false;

    public boolean isSparkPartitionCountAutoTuneEnabled()
    {
        return sparkPartitionCountAutoTuneEnabled;
    }

    @Config("spark.partition-count-auto-tune-enabled")
    @ConfigDescription("Automatic tuning of spark partition count based on max splits data size per partition")
    public PrestoSparkConfig setSparkPartitionCountAutoTuneEnabled(boolean sparkPartitionCountAutoTuneEnabled)
    {
        this.sparkPartitionCountAutoTuneEnabled = sparkPartitionCountAutoTuneEnabled;
        return this;
    }

    @Config("spark.min-spark-input-partition-count-for-auto-tune")
    @ConfigDescription("Minimal Spark input partition count when Spark partition auto tune is enabled")
    public PrestoSparkConfig setMinSparkInputPartitionCountForAutoTune(int minSparkInputPartitionCountForAutoTune)
    {
        this.minSparkInputPartitionCountForAutoTune = minSparkInputPartitionCountForAutoTune;
        return this;
    }

    public int getMinSparkInputPartitionCountForAutoTune()
    {
        return minSparkInputPartitionCountForAutoTune;
    }

    @Config("spark.max-spark-input-partition-count-for-auto-tune")
    @ConfigDescription("Max Spark input partition count when Spark partition auto tune is enabled")
    public PrestoSparkConfig setMaxSparkInputPartitionCountForAutoTune(int maxSparkInputPartitionCountForAutoTune)
    {
        this.maxSparkInputPartitionCountForAutoTune = maxSparkInputPartitionCountForAutoTune;
        return this;
    }

    public int getMaxSparkInputPartitionCountForAutoTune()
    {
        return maxSparkInputPartitionCountForAutoTune;
    }

    public int getInitialSparkPartitionCount()
    {
        return initialSparkPartitionCount;
    }

    @Config("spark.initial-partition-count")
    @ConfigDescription("Initial partition count for Spark RDD when reading table")
    public PrestoSparkConfig setInitialSparkPartitionCount(int initialPartitionCount)
    {
        this.initialSparkPartitionCount = initialPartitionCount;
        return this;
    }

    public DataSize getMaxSplitsDataSizePerSparkPartition()
    {
        return maxSplitsDataSizePerSparkPartition;
    }

    @Config("spark.max-splits-data-size-per-partition")
    @ConfigDescription("Maximal size in bytes for splits assigned to one partition")
    public PrestoSparkConfig setMaxSplitsDataSizePerSparkPartition(DataSize maxSplitsDataSizePerSparkPartition)
    {
        this.maxSplitsDataSizePerSparkPartition = maxSplitsDataSizePerSparkPartition;
        return this;
    }

    @NotNull
    public DataSize getShuffleOutputTargetAverageRowSize()
    {
        return shuffleOutputTargetAverageRowSize;
    }

    @Config("spark.shuffle-output-target-average-row-size")
    @ConfigDescription("Target average size for row entries produced by Presto on Spark for shuffle")
    public PrestoSparkConfig setShuffleOutputTargetAverageRowSize(DataSize shuffleOutputTargetAverageRowSize)
    {
        this.shuffleOutputTargetAverageRowSize = shuffleOutputTargetAverageRowSize;
        return this;
    }

    public boolean isStorageBasedBroadcastJoinEnabled()
    {
        return storageBasedBroadcastJoinEnabled;
    }

    @Config("spark.storage-based-broadcast-join-enabled")
    @ConfigDescription("Distribute broadcast hashtable to workers using storage")
    public PrestoSparkConfig setStorageBasedBroadcastJoinEnabled(boolean storageBasedBroadcastJoinEnabled)
    {
        this.storageBasedBroadcastJoinEnabled = storageBasedBroadcastJoinEnabled;
        return this;
    }

    public DataSize getStorageBasedBroadcastJoinWriteBufferSize()
    {
        return storageBasedBroadcastJoinWriteBufferSize;
    }

    @Config("spark.storage-based-broadcast-join-write-buffer-size")
    @ConfigDescription("Maximum size in bytes to buffer before flushing pages to disk")
    public PrestoSparkConfig setStorageBasedBroadcastJoinWriteBufferSize(DataSize storageBasedBroadcastJoinWriteBufferSize)
    {
        this.storageBasedBroadcastJoinWriteBufferSize = storageBasedBroadcastJoinWriteBufferSize;
        return this;
    }

    public String getStorageBasedBroadcastJoinStorage()
    {
        return storageBasedBroadcastJoinStorage;
    }

    @Config("spark.storage-based-broadcast-join-storage")
    @ConfigDescription("TempStorage to use for dumping broadcast table")
    public PrestoSparkConfig setStorageBasedBroadcastJoinStorage(String storageBasedBroadcastJoinStorage)
    {
        this.storageBasedBroadcastJoinStorage = storageBasedBroadcastJoinStorage;
        return this;
    }

    public DataSize getSparkBroadcastJoinMaxMemoryOverride()
    {
        return sparkBroadcastJoinMaxMemoryOverride;
    }

    @Config("spark.broadcast-join-max-memory-override")
    public PrestoSparkConfig setSparkBroadcastJoinMaxMemoryOverride(DataSize sparkBroadcastJoinMaxMemoryOverride)
    {
        this.sparkBroadcastJoinMaxMemoryOverride = sparkBroadcastJoinMaxMemoryOverride;
        return this;
    }

    public boolean isSmileSerializationEnabled()
    {
        return smileSerializationEnabled;
    }

    @Config("spark.smile-serialization-enabled")
    public PrestoSparkConfig setSmileSerializationEnabled(boolean smileSerializationEnabled)
    {
        this.smileSerializationEnabled = smileSerializationEnabled;
        return this;
    }

    public int getSplitAssignmentBatchSize()
    {
        return splitAssignmentBatchSize;
    }

    @Config("spark.split-assignment-batch-size")
    public PrestoSparkConfig setSplitAssignmentBatchSize(int splitAssignmentBatchSize)
    {
        this.splitAssignmentBatchSize = splitAssignmentBatchSize;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryRevokingThreshold()
    {
        return memoryRevokingThreshold;
    }

    @Config("spark.memory-revoking-threshold")
    @ConfigDescription("Revoke memory when memory pool is filled over threshold")
    public PrestoSparkConfig setMemoryRevokingThreshold(double memoryRevokingThreshold)
    {
        this.memoryRevokingThreshold = memoryRevokingThreshold;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryRevokingTarget()
    {
        return memoryRevokingTarget;
    }

    @Config("spark.memory-revoking-target")
    @ConfigDescription("When revoking memory, try to revoke so much that memory pool is filled below target at the end")
    public PrestoSparkConfig setMemoryRevokingTarget(double memoryRevokingTarget)
    {
        this.memoryRevokingTarget = memoryRevokingTarget;
        return this;
    }

    public boolean isRetryOnOutOfMemoryBroadcastJoinEnabled()
    {
        return retryOnOutOfMemoryBroadcastJoinEnabled;
    }

    @Config("spark.retry-on-out-of-memory-broadcast-join-enabled")
    @ConfigDescription("Disable broadcast join on broadcast OOM and re-submit the query again within the same spark session")
    public PrestoSparkConfig setRetryOnOutOfMemoryBroadcastJoinEnabled(boolean retryOnOutOfMemoryBroadcastJoinEnabled)
    {
        this.retryOnOutOfMemoryBroadcastJoinEnabled = retryOnOutOfMemoryBroadcastJoinEnabled;
        return this;
    }

    public boolean isRetryOnOutOfMemoryWithIncreasedMemorySettingsEnabled()
    {
        return retryOnOutOfMemoryWithIncreasedMemorySettingsEnabled;
    }

    @Config("spark.retry-on-out-of-memory-with-increased-memory-settings-enabled")
    @ConfigDescription("Retry OOMs with increased memory settings and re-submit the query again within the same spark session")
    public PrestoSparkConfig setRetryOnOutOfMemoryWithIncreasedMemorySettingsEnabled(boolean retryOnOutOfMemoryWithIncreasedMemorySettingsEnabled)
    {
        this.retryOnOutOfMemoryWithIncreasedMemorySettingsEnabled = retryOnOutOfMemoryWithIncreasedMemorySettingsEnabled;
        return this;
    }

    public Map<String, String> getOutOfMemoryRetryPrestoSessionProperties()
    {
        return outOfMemoryRetryPrestoSessionProperties;
    }

    @Config("spark.retry-presto-session-properties")
    @ConfigDescription("Presto session properties to use on OOM query retry")
    public PrestoSparkConfig setOutOfMemoryRetryPrestoSessionProperties(String outOfMemoryRetryPrestoSessionProperties)
    {
        this.outOfMemoryRetryPrestoSessionProperties = MAP_SPLITTER.split(nullToEmpty(outOfMemoryRetryPrestoSessionProperties));
        return this;
    }

    public Map<String, String> getOutOfMemoryRetrySparkConfigs()
    {
        return outOfMemoryRetrySparkConfigs;
    }

    @Config("spark.retry-spark-configs")
    @ConfigDescription("Spark Configs to use on OOM query retry")
    public PrestoSparkConfig setOutOfMemoryRetrySparkConfigs(String outOfMemoryRetrySparkConfigs)
    {
        this.outOfMemoryRetrySparkConfigs = MAP_SPLITTER.split(nullToEmpty(outOfMemoryRetrySparkConfigs));
        return this;
    }

    public DataSize getAverageInputDataSizePerExecutor()
    {
        return averageInputDataSizePerExecutor;
    }

    @Config("spark.average-input-datasize-per-executor")
    @ConfigDescription("Provides average input data size used per executor")
    public PrestoSparkConfig setAverageInputDataSizePerExecutor(DataSize averageInputDataSizePerExecutor)
    {
        this.averageInputDataSizePerExecutor = averageInputDataSizePerExecutor;
        return this;
    }

    @Min(1)
    public int getMaxExecutorCount()
    {
        return maxExecutorCount;
    }

    @Config("spark.max-executor-count")
    @ConfigDescription("Provides the maximum count of the executors the query will allocate")
    public PrestoSparkConfig setMaxExecutorCount(int maxExecutorCount)
    {
        this.maxExecutorCount = maxExecutorCount;
        return this;
    }

    @Min(1)
    public int getMinExecutorCount()
    {
        return minExecutorCount;
    }

    @Config("spark.min-executor-count")
    @ConfigDescription("Provides the minimum count of the executors the query will allocate")
    public PrestoSparkConfig setMinExecutorCount(int minExecutorCount)
    {
        this.minExecutorCount = minExecutorCount;
        return this;
    }

    public DataSize getAverageInputDataSizePerPartition()
    {
        return averageInputDataSizePerPartition;
    }

    @Config("spark.average-input-datasize-per-partition")
    @ConfigDescription("Provides average input data size per partition")
    public PrestoSparkConfig setAverageInputDataSizePerPartition(DataSize averageInputDataSizePerPartition)
    {
        this.averageInputDataSizePerPartition = averageInputDataSizePerPartition;
        return this;
    }

    @Min(1)
    public int getMaxHashPartitionCount()
    {
        return maxHashPartitionCount;
    }

    @Config("spark.max-hash-partition-count")
    @ConfigDescription("Provides the maximum number of the hash partition count the query can allocate")
    public PrestoSparkConfig setMaxHashPartitionCount(int maxHashPartitionCount)
    {
        this.maxHashPartitionCount = maxHashPartitionCount;
        return this;
    }

    @Min(1)
    public int getMinHashPartitionCount()
    {
        return minHashPartitionCount;
    }

    @Config("spark.min-hash-partition-count")
    @ConfigDescription("Provides the minimum number of the hash partition count the query can allocate")
    public PrestoSparkConfig setMinHashPartitionCount(int minHashPartitionCount)
    {
        this.minHashPartitionCount = minHashPartitionCount;
        return this;
    }

    public boolean isSparkResourceAllocationStrategyEnabled()
    {
        return isResourceAllocationStrategyEnabled;
    }

    @Config("spark.resource-allocation-strategy-enabled")
    @ConfigDescription("Determines whether the resource allocation strategy for executor and partition count is enabled")
    public PrestoSparkConfig setSparkResourceAllocationStrategyEnabled(boolean isResourceAllocationStrategyEnabled)
    {
        this.isResourceAllocationStrategyEnabled = isResourceAllocationStrategyEnabled;
        return this;
    }

    @Config("spark.native-engine-execution-enabled")
    @ConfigDescription("Enable execution on native engine.")
    public PrestoSparkConfig setNativeEngineExecutionEnabled(boolean nativeEngineExecutionEnabled)
    {
        this.nativeEngineExecutionEnabled = nativeEngineExecutionEnabled;
        return this;
    }

    public boolean isNativeEngineExecutionEnabled()
    {
        return this.nativeEngineExecutionEnabled;
    }
}
