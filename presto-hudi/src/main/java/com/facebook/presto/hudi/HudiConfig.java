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
import io.airlift.units.DataSize;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class HudiConfig
{
    private boolean metadataTableEnabled;
    private boolean sizeBasedSplitWeightsEnabled = true;
    private DataSize standardSplitWeightSize = new DataSize(128, MEGABYTE);
    private double minimumAssignedSplitWeight = 0.05;
    private int maxOutstandingSplits = 1000;
    private int splitLoaderParallelism = 4;
    private int splitGeneratorParallelism = 4;

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
}
