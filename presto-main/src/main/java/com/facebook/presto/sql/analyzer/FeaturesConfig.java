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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({
        "resource-group-manager",
        "experimental-syntax-enabled",
        "analyzer.experimental-syntax-enabled"
})
public class FeaturesConfig
{
    public static class ProcessingOptimization
    {
        public static final String DISABLED = "disabled";
        public static final String COLUMNAR = "columnar";
        public static final String COLUMNAR_DICTIONARY = "columnar_dictionary";

        public static final List<String> AVAILABLE_OPTIONS = ImmutableList.of(DISABLED, COLUMNAR, COLUMNAR_DICTIONARY);
    }

    private boolean distributedIndexJoinsEnabled;
    private boolean distributedJoinsEnabled = true;
    private boolean colocatedJoinsEnabled;
    private boolean reorderJoins;
    private boolean redistributeWrites = true;
    private boolean optimizeMetadataQueries;
    private boolean optimizeHashGeneration = true;
    private boolean optimizeSingleDistinct = true;
    private boolean optimizerReorderWindows = false;
    private boolean pushTableWriteThroughUnion = true;
    private boolean exchangeCompressionEnabled = false;
    private boolean legacyArrayAgg;
    private boolean legacyOrderBy;
    private boolean legacyMapSubscript;
    private boolean optimizeMixedDistinctAggregations;

    private String processingOptimization = ProcessingOptimization.DISABLED;
    private boolean dictionaryAggregation;
    private boolean resourceGroups;

    private int re2JDfaStatesLimit = Integer.MAX_VALUE;
    private int re2JDfaRetries = 5;
    private RegexLibrary regexLibrary = JONI;
    private boolean spillEnabled;
    private DataSize operatorMemoryLimitBeforeSpill = new DataSize(4, DataSize.Unit.MEGABYTE);
    private List<Path> spillerSpillPaths = ImmutableList.of();
    private int spillerThreads = 4;
    private double spillMaxUsedSpaceThreshold = 0.9;
    private boolean iterativeOptimizerEnabled = true;

    private Duration iterativeOptimizerTimeout = new Duration(3, MINUTES); // by default let optimizer wait a long time in case it retrieves some data from ConnectorMetadata

    public boolean isResourceGroupsEnabled()
    {
        return resourceGroups;
    }

    @Config("experimental.resource-groups-enabled")
    public FeaturesConfig setResourceGroupsEnabled(boolean enabled)
    {
        resourceGroups = enabled;
        return this;
    }

    public boolean isDistributedIndexJoinsEnabled()
    {
        return distributedIndexJoinsEnabled;
    }

    @Config("distributed-index-joins-enabled")
    public FeaturesConfig setDistributedIndexJoinsEnabled(boolean distributedIndexJoinsEnabled)
    {
        this.distributedIndexJoinsEnabled = distributedIndexJoinsEnabled;
        return this;
    }

    public boolean isDistributedJoinsEnabled()
    {
        return distributedJoinsEnabled;
    }

    @Config("deprecated.legacy-array-agg")
    public FeaturesConfig setLegacyArrayAgg(boolean legacyArrayAgg)
    {
        this.legacyArrayAgg = legacyArrayAgg;
        return this;
    }

    public boolean isLegacyArrayAgg()
    {
        return legacyArrayAgg;
    }

    @Config("deprecated.legacy-order-by")
    public FeaturesConfig setLegacyOrderBy(boolean value)
    {
        this.legacyOrderBy = value;
        return this;
    }

    public boolean isLegacyOrderBy()
    {
        return legacyOrderBy;
    }

    @Config("deprecated.legacy-map-subscript")
    public FeaturesConfig setLegacyMapSubscript(boolean value)
    {
        this.legacyMapSubscript = value;
        return this;
    }

    public boolean isLegacyMapSubscript()
    {
        return legacyMapSubscript;
    }

    @Config("distributed-joins-enabled")
    public FeaturesConfig setDistributedJoinsEnabled(boolean distributedJoinsEnabled)
    {
        this.distributedJoinsEnabled = distributedJoinsEnabled;
        return this;
    }

    public boolean isColocatedJoinsEnabled()
    {
        return colocatedJoinsEnabled;
    }

    @Config("colocated-joins-enabled")
    @ConfigDescription("Experimental: Use a colocated join when possible")
    public FeaturesConfig setColocatedJoinsEnabled(boolean colocatedJoinsEnabled)
    {
        this.colocatedJoinsEnabled = colocatedJoinsEnabled;
        return this;
    }

    public boolean isJoinReorderingEnabled()
    {
        return reorderJoins;
    }

    @Config("reorder-joins")
    @ConfigDescription("Experimental: Reorder joins to optimize plan")
    public FeaturesConfig setJoinReorderingEnabled(boolean reorderJoins)
    {
        this.reorderJoins = reorderJoins;
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

    public String getProcessingOptimization()
    {
        return processingOptimization;
    }

    @Config("optimizer.processing-optimization")
    public FeaturesConfig setProcessingOptimization(String processingOptimization)
    {
        if (!ProcessingOptimization.AVAILABLE_OPTIONS.contains(processingOptimization)) {
            throw new IllegalStateException(String.format("Value %s is not valid for processingOptimization.", processingOptimization));
        }
        this.processingOptimization = processingOptimization;
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

    @Min(2)
    public int getRe2JDfaStatesLimit()
    {
        return re2JDfaStatesLimit;
    }

    @Config("re2j.dfa-states-limit")
    public FeaturesConfig setRe2JDfaStatesLimit(int re2JDfaStatesLimit)
    {
        this.re2JDfaStatesLimit = re2JDfaStatesLimit;
        return this;
    }

    @Min(0)
    public int getRe2JDfaRetries()
    {
        return re2JDfaRetries;
    }

    @Config("re2j.dfa-retries")
    public FeaturesConfig setRe2JDfaRetries(int re2JDfaRetries)
    {
        this.re2JDfaRetries = re2JDfaRetries;
        return this;
    }

    public RegexLibrary getRegexLibrary()
    {
        return regexLibrary;
    }

    @Config("regex-library")
    public FeaturesConfig setRegexLibrary(RegexLibrary regexLibrary)
    {
        this.regexLibrary = regexLibrary;
        return this;
    }

    public boolean isSpillEnabled()
    {
        return spillEnabled;
    }

    @Config("experimental.spill-enabled")
    public FeaturesConfig setSpillEnabled(boolean spillEnabled)
    {
        this.spillEnabled = spillEnabled;
        return this;
    }

    public boolean isIterativeOptimizerEnabled()
    {
        return iterativeOptimizerEnabled;
    }

    @Config("experimental.iterative-optimizer-enabled")
    public FeaturesConfig setIterativeOptimizerEnabled(boolean value)
    {
        this.iterativeOptimizerEnabled = value;
        return this;
    }

    public Duration getIterativeOptimizerTimeout()
    {
        return iterativeOptimizerTimeout;
    }

    @Config("experimental.iterative-optimizer-timeout")
    public FeaturesConfig setIterativeOptimizerTimeout(Duration timeout)
    {
        this.iterativeOptimizerTimeout = timeout;
        return this;
    }

    public DataSize getOperatorMemoryLimitBeforeSpill()
    {
        return operatorMemoryLimitBeforeSpill;
    }

    @Config("experimental.operator-memory-limit-before-spill")
    public FeaturesConfig setOperatorMemoryLimitBeforeSpill(DataSize operatorMemoryLimitBeforeSpill)
    {
        this.operatorMemoryLimitBeforeSpill = operatorMemoryLimitBeforeSpill;
        return this;
    }

    public List<Path> getSpillerSpillPaths()
    {
        return spillerSpillPaths;
    }

    @Config("experimental.spiller-spill-path")
    public FeaturesConfig setSpillerSpillPaths(String spillPaths)
    {
        List<String> spillPathsSplit = ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(spillPaths));
        this.spillerSpillPaths = spillPathsSplit.stream().map(path -> Paths.get(path)).collect(toImmutableList());
        return this;
    }

    public int getSpillerThreads()
    {
        return spillerThreads;
    }

    @Config("experimental.spiller-threads")
    public FeaturesConfig setSpillerThreads(int spillerThreads)
    {
        this.spillerThreads = spillerThreads;
        return this;
    }

    public double getSpillMaxUsedSpaceThreshold()
    {
        return spillMaxUsedSpaceThreshold;
    }

    @Config("experimental.spiller-max-used-space-threshold")
    public FeaturesConfig setSpillMaxUsedSpaceThreshold(double spillMaxUsedSpaceThreshold)
    {
        this.spillMaxUsedSpaceThreshold = spillMaxUsedSpaceThreshold;
        return this;
    }

    public boolean isOptimizeMixedDistinctAggregations()
    {
        return optimizeMixedDistinctAggregations;
    }

    @Config("optimizer.optimize-mixed-distinct-aggregations")
    public FeaturesConfig setOptimizeMixedDistinctAggregations(boolean value)
    {
        this.optimizeMixedDistinctAggregations = value;
        return this;
    }

    public boolean isExchangeCompressionEnabled()
    {
        return exchangeCompressionEnabled;
    }

    @Config("exchange.compression-enabled")
    public FeaturesConfig setExchangeCompressionEnabled(boolean exchangeCompressionEnabled)
    {
        this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        return this;
    }
}
