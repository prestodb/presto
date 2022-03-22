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
import io.airlift.units.DataSize;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class PrestoSparkConfig
{
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
}
