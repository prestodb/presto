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

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class PrestoSparkConfig
{
    private boolean sparkPartitionCountAutoTuneEnabled = true;
    private int initialSparkPartitionCount = 16;
    private DataSize maxSplitsDataSizePerSparkPartition = new DataSize(2, GIGABYTE);

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
}
