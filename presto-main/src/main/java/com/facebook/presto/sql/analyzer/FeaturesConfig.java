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
package com.facebook.presto.sql.analyzer;

import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.NotNull;

public class FeaturesConfig
{
    public static final String FILE_BASED_RESOURCE_GROUP_MANAGER = "file";
    private boolean experimentalSyntaxEnabled;
    private boolean distributedIndexJoinsEnabled;
    private boolean distributedJoinsEnabled = true;
    private boolean redistributeWrites = true;
    private boolean optimizeMetadataQueries;
    private boolean optimizeHashGeneration = true;
    private boolean optimizeSingleDistinct = true;
    private boolean pushTableWriteThroughUnion = true;
    private boolean intermediateAggregationsEnabled;

    private boolean columnarProcessing;
    private boolean columnarProcessingDictionary;
    private boolean dictionaryAggregation;

    private String resourceGroupManager = FILE_BASED_RESOURCE_GROUP_MANAGER;

    @NotNull
    public String getResourceGroupManager()
    {
        return resourceGroupManager;
    }

    @Config("resource-group-manager")
    public FeaturesConfig setResourceGroupManager(String resourceGroupManager)
    {
        this.resourceGroupManager = resourceGroupManager;
        return this;
    }

    @LegacyConfig("analyzer.experimental-syntax-enabled")
    @Config("experimental-syntax-enabled")
    public FeaturesConfig setExperimentalSyntaxEnabled(boolean enabled)
    {
        experimentalSyntaxEnabled = enabled;
        return this;
    }

    public boolean isExperimentalSyntaxEnabled()
    {
        return experimentalSyntaxEnabled;
    }

    @Config("distributed-index-joins-enabled")
    public FeaturesConfig setDistributedIndexJoinsEnabled(boolean distributedIndexJoinsEnabled)
    {
        this.distributedIndexJoinsEnabled = distributedIndexJoinsEnabled;
        return this;
    }

    public boolean isDistributedIndexJoinsEnabled()
    {
        return distributedIndexJoinsEnabled;
    }

    @Config("distributed-joins-enabled")
    public FeaturesConfig setDistributedJoinsEnabled(boolean distributedJoinsEnabled)
    {
        this.distributedJoinsEnabled = distributedJoinsEnabled;
        return this;
    }

    public boolean isRedistributeWrites()
    {
        return redistributeWrites;
    }

    @Config("redistribute-writes")
    public FeaturesConfig setRedistributeWrites(boolean redistributeWrites)
    {
        this.redistributeWrites = redistributeWrites;
        return this;
    }

    public boolean isDistributedJoinsEnabled()
    {
        return distributedJoinsEnabled;
    }

    public boolean isOptimizeMetadataQueries()
    {
        return optimizeMetadataQueries;
    }

    @Config("optimizer.optimize-metadata-queries")
    public FeaturesConfig setOptimizeMetadataQueries(boolean optimizeMetadataQueries)
    {
        this.optimizeMetadataQueries = optimizeMetadataQueries;
        return this;
    }

    public boolean isOptimizeHashGeneration()
    {
        return optimizeHashGeneration;
    }

    @Config("optimizer.optimize-hash-generation")
    public FeaturesConfig setOptimizeHashGeneration(boolean optimizeHashGeneration)
    {
        this.optimizeHashGeneration = optimizeHashGeneration;
        return this;
    }

    public boolean isOptimizeSingleDistinct()
    {
        return optimizeSingleDistinct;
    }

    @Config("optimizer.optimize-single-distinct")
    public FeaturesConfig setOptimizeSingleDistinct(boolean optimizeSingleDistinct)
    {
        this.optimizeSingleDistinct = optimizeSingleDistinct;
        return this;
    }

    public boolean isPushTableWriteThroughUnion()
    {
        return pushTableWriteThroughUnion;
    }

    @Config("optimizer.push-table-write-through-union")
    public FeaturesConfig setPushTableWriteThroughUnion(boolean pushTableWriteThroughUnion)
    {
        this.pushTableWriteThroughUnion = pushTableWriteThroughUnion;
        return this;
    }

    public boolean isIntermediateAggregationsEnabled()
    {
        return intermediateAggregationsEnabled;
    }

    @Config("optimizer.use-intermediate-aggregations")
    public FeaturesConfig setIntermediateAggregationsEnabled(boolean intermediateAggregationsEnabled)
    {
        this.intermediateAggregationsEnabled = intermediateAggregationsEnabled;
        return this;
    }

    public boolean isColumnarProcessing()
    {
        return columnarProcessing;
    }

    @Config("optimizer.columnar-processing")
    public FeaturesConfig setColumnarProcessing(boolean columnarProcessing)
    {
        this.columnarProcessing = columnarProcessing;
        return this;
    }

    public boolean isColumnarProcessingDictionary()
    {
        return columnarProcessingDictionary;
    }

    @Config("optimizer.columnar-processing-dictionary")
    public FeaturesConfig setColumnarProcessingDictionary(boolean columnarProcessingDictionary)
    {
        this.columnarProcessingDictionary = columnarProcessingDictionary;
        return this;
    }

    public boolean isDictionaryAggregation()
    {
        return dictionaryAggregation;
    }

    @Config("optimizer.dictionary-aggregation")
    public FeaturesConfig setDictionaryAggregation(boolean dictionaryAggregation)
    {
        this.dictionaryAggregation = dictionaryAggregation;
        return this;
    }
}
