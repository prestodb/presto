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

public class FeaturesConfig
{
    private boolean experimentalSyntaxEnabled;
    private boolean distributedIndexJoinsEnabled;
    private boolean distributedJoinsEnabled;
    private boolean redistributeWrites = true;
    private boolean optimizeMetadataQueries;
    private boolean optimizeHashGeneration;
    private boolean optimizeSingleDistinct = true;

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
}
