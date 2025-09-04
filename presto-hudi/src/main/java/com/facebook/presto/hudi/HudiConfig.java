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

package com.facebook.presto.hudi;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HudiConfig
{
    private boolean metadataTableEnabled;
    private boolean sizeBasedSplitWeightsEnabled = true;
    private DataSize standardSplitWeightSize = new DataSize(128, MEGABYTE);
    private double minimumAssignedSplitWeight = 0.05;
    private int maxOutstandingSplits = 1000;
    private int splitLoaderParallelism = 4;
    private int splitGeneratorParallelism = 4;

    private boolean isColumnStatsIndexEnabled = true;
    private boolean isRecordLevelIndexEnabled = true;
    private boolean isSecondaryIndexEnabled = true;
    private boolean isPartitionStatsIndexEnabled = true;
    private Duration columnStatsWaitTimeout = new Duration(1, SECONDS);
    private Duration recordIndexWaitTimeout = new Duration(2, SECONDS);
    private Duration secondaryIndexWaitTimeout = new Duration(2, SECONDS);
    private boolean metadataPartitionListingEnabled = true;
    private boolean resolveColumnNameCasingEnabled;

    public boolean isMetadataTableEnabled()
    {
        return metadataTableEnabled;
    }

    @Config("hudi.metadata-table-enabled")
    public HudiConfig setMetadataTableEnabled(boolean metadataTableEnabled)
    {
        this.metadataTableEnabled = metadataTableEnabled;
        return this;
    }

    public boolean isSizeBasedSplitWeightsEnabled()
    {
        return sizeBasedSplitWeightsEnabled;
    }

    @Config("hudi.size-based-split-weights-enabled")
    @ConfigDescription("Unlike uniform splitting, size-based splitting ensures that each batch of splits has enough data to process. " +
            "By default, it is enabled to improve performance.")
    public HudiConfig setSizeBasedSplitWeightsEnabled(boolean sizeBasedSplitWeightsEnabled)
    {
        this.sizeBasedSplitWeightsEnabled = sizeBasedSplitWeightsEnabled;
        return this;
    }

    @NotNull
    public DataSize getStandardSplitWeightSize()
    {
        return standardSplitWeightSize;
    }

    @Config("hudi.standard-split-weight-size")
    @ConfigDescription("The split size corresponding to the standard weight (1.0) "
            + "when size based split weights are enabled.")
    public HudiConfig setStandardSplitWeightSize(DataSize standardSplitWeightSize)
    {
        this.standardSplitWeightSize = standardSplitWeightSize;
        return this;
    }

    @DecimalMax("1")
    @DecimalMin(value = "0", inclusive = false)
    public double getMinimumAssignedSplitWeight()
    {
        return minimumAssignedSplitWeight;
    }

    @Config("hudi.minimum-assigned-split-weight")
    @ConfigDescription("Minimum weight that a split can be assigned when size based split weights are enabled.")
    public HudiConfig setMinimumAssignedSplitWeight(double minimumAssignedSplitWeight)
    {
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
        return this;
    }

    @Min(1)
    public int getMaxOutstandingSplits()
    {
        return maxOutstandingSplits;
    }

    @Config("hudi.max-outstanding-splits")
    @ConfigDescription("Maximum outstanding splits per batch for query.")
    public HudiConfig setMaxOutstandingSplits(int maxOutstandingSplits)
    {
        this.maxOutstandingSplits = maxOutstandingSplits;
        return this;
    }

    @Min(1)
    public int getSplitGeneratorParallelism()
    {
        return splitGeneratorParallelism;
    }

    @Config("hudi.split-generator-parallelism")
    @ConfigDescription("Number of threads used to generate splits from partitions.")
    public HudiConfig setSplitGeneratorParallelism(int splitGeneratorParallelism)
    {
        this.splitGeneratorParallelism = splitGeneratorParallelism;
        return this;
    }

    @Min(1)
    public int getSplitLoaderParallelism()
    {
        return splitLoaderParallelism;
    }

    @Config("hudi.split-loader-parallelism")
    @ConfigDescription("Number of threads used to run background split loader. "
            + "A single background split loader is needed per query.")
    public HudiConfig setSplitLoaderParallelism(int splitLoaderParallelism)
    {
        this.splitLoaderParallelism = splitLoaderParallelism;
        return this;
    }

    @Config("hudi.index.column-stats-index-enabled")
    @ConfigDescription("Internal configuration to control whether column stats index is enabled for debugging/testing.")
    public HudiConfig setColumnStatsIndexEnabled(boolean isColumnStatsIndexEnabled)
    {
        this.isColumnStatsIndexEnabled = isColumnStatsIndexEnabled;
        return this;
    }

    public boolean isColumnStatsIndexEnabled()
    {
        return isColumnStatsIndexEnabled;
    }

    @Config("hudi.index.record-level-index-enabled")
    @ConfigDescription("Internal configuration to control whether record level index is enabled for debugging/testing.")
    public HudiConfig setRecordLevelIndexEnabled(boolean isRecordLevelIndexEnabled)
    {
        this.isRecordLevelIndexEnabled = isRecordLevelIndexEnabled;
        return this;
    }

    public boolean isRecordLevelIndexEnabled()
    {
        return isRecordLevelIndexEnabled;
    }

    @Config("hudi.index.secondary-index-enabled")
    @ConfigDescription("Internal configuration to control whether secondary index is enabled for debugging/testing.")
    public HudiConfig setSecondaryIndexEnabled(boolean isSecondaryIndexEnabled)
    {
        this.isSecondaryIndexEnabled = isSecondaryIndexEnabled;
        return this;
    }

    public boolean isSecondaryIndexEnabled()
    {
        return isSecondaryIndexEnabled;
    }

    @Config("hudi.index.partition-stats-index-enabled")
    @ConfigDescription("Internal configuration to control whether partition stats index is enabled for debugging/testing.")
    public HudiConfig setPartitionStatsIndexEnabled(boolean isPartitionStatsIndexEnabled)
    {
        this.isPartitionStatsIndexEnabled = isPartitionStatsIndexEnabled;
        return this;
    }

    public boolean isPartitionStatsIndexEnabled()
    {
        return isPartitionStatsIndexEnabled;
    }

    @Config("hudi.index.record-index.wait-timeout")
    @ConfigDescription("Maximum timeout to wait for loading record index, e.g. 1000ms, 20s")
    public HudiConfig setRecordIndexWaitTimeout(Duration recordIndexWaitTimeout)
    {
        this.recordIndexWaitTimeout = recordIndexWaitTimeout;
        return this;
    }

    @Config("hudi.index.column-stats.wait-timeout")
    @ConfigDescription("Maximum timeout to wait for loading column stats, e.g. 1000ms, 20s")
    public HudiConfig setColumnStatsWaitTimeout(Duration columnStatusWaitTimeout)
    {
        this.columnStatsWaitTimeout = columnStatusWaitTimeout;
        return this;
    }

    @NotNull
    public Duration getColumnStatsWaitTimeout()
    {
        return columnStatsWaitTimeout;
    }

    @NotNull
    public Duration getRecordIndexWaitTimeout()
    {
        return recordIndexWaitTimeout;
    }

    @Config("hudi.index.secondary-index.wait-timeout")
    @ConfigDescription("Maximum timeout to wait for loading secondary index, e.g. 1000ms, 20s")
    public HudiConfig setSecondaryIndexWaitTimeout(Duration secondaryIndexWaitTimeout)
    {
        this.secondaryIndexWaitTimeout = secondaryIndexWaitTimeout;
        return this;
    }

    @NotNull
    public Duration getSecondaryIndexWaitTimeout()
    {
        return secondaryIndexWaitTimeout;
    }

    public boolean isMetadataPartitionListingEnabled()
    {
        return metadataPartitionListingEnabled;
    }

    @Config("hudi.metadata.partition-listing.enabled")
    @ConfigDescription("Enables listing table partitions through the metadata table.")
    public HudiConfig setMetadataPartitionListingEnabled(boolean metadataPartitionListingEnabled)
    {
        this.metadataPartitionListingEnabled = metadataPartitionListingEnabled;
        return this;
    }


    public boolean isResolveColumnNameCasingEnabled()
    {
        return resolveColumnNameCasingEnabled;
    }

    @Config("hudi.table.resolve-column-name-casing.enabled")
    @ConfigDescription("Reconcile column names between the catalog schema and the Hudi table to handle case differences")
    public HudiConfig setResolveColumnNameCasingEnabled(boolean resolveColumnNameCasingEnabled)
    {
        this.resolveColumnNameCasingEnabled = resolveColumnNameCasingEnabled;
        return this;
    }
}
