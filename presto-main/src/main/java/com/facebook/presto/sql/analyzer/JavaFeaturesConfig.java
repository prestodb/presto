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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class JavaFeaturesConfig
{
    private boolean tableWriterMergeOperatorEnabled = true;
    private boolean optimizeHashGeneration = true;
    private boolean topNSpillEnabled = true;
    private boolean aggregationSpillEnabled = true;
    private boolean distinctAggregationSpillEnabled = true;
    private boolean dedupBasedDistinctAggregationSpillEnabled;
    private boolean distinctAggregationLargeBlockSpillEnabled;
    private boolean orderByAggregationSpillEnabled = true;
    private boolean orderBySpillEnabled = true;
    private boolean windowSpillEnabled = true;
    private DataSize distinctAggregationLargeBlockSizeThreshold = new DataSize(50, MEGABYTE);
    private DataSize topNOperatorUnspillMemoryLimit = new DataSize(4, MEGABYTE);
    private boolean parseDecimalLiteralsAsDouble;
    private boolean offsetClauseEnabled;
    private boolean inlineSqlFunctions = true;
    private DataSize aggregationOperatorUnspillMemoryLimit = new DataSize(4, MEGABYTE);

    public boolean isOrderBySpillEnabled()
    {
        return orderBySpillEnabled;
    }

    @Config("experimental.order-by-spill-enabled")
    @ConfigDescription("Enable Order-by Operator Spilling if spill is enabled")
    public JavaFeaturesConfig setOrderBySpillEnabled(boolean orderBySpillEnabled)
    {
        this.orderBySpillEnabled = orderBySpillEnabled;
        return this;
    }

    public boolean isWindowSpillEnabled()
    {
        return windowSpillEnabled;
    }

    @Config("experimental.window-spill-enabled")
    @ConfigDescription("Enable Window Operator Spilling if spill is enabled")
    public JavaFeaturesConfig setWindowSpillEnabled(boolean windowSpillEnabled)
    {
        this.windowSpillEnabled = windowSpillEnabled;
        return this;
    }

    public boolean isOrderByAggregationSpillEnabled()
    {
        return orderByAggregationSpillEnabled;
    }

    @Config("experimental.order-by-aggregation-spill-enabled")
    @ConfigDescription("Spill order-by aggregations if aggregation spill is enabled")
    public JavaFeaturesConfig setOrderByAggregationSpillEnabled(boolean orderByAggregationSpillEnabled)
    {
        this.orderByAggregationSpillEnabled = orderByAggregationSpillEnabled;
        return this;
    }

    public DataSize getDistinctAggregationLargeBlockSizeThreshold()
    {
        return distinctAggregationLargeBlockSizeThreshold;
    }

    @Config("experimental.distinct-aggregation-large-block-size-threshold")
    @ConfigDescription("Block size threshold beyond which it will be spilled into a separate spill file")
    public JavaFeaturesConfig setDistinctAggregationLargeBlockSizeThreshold(DataSize distinctAggregationLargeBlockSizeThreshold)
    {
        this.distinctAggregationLargeBlockSizeThreshold = distinctAggregationLargeBlockSizeThreshold;
        return this;
    }

    public boolean isDistinctAggregationLargeBlockSpillEnabled()
    {
        return distinctAggregationLargeBlockSpillEnabled;
    }

    @Config("experimental.distinct-aggregation-large-block-spill-enabled")
    @ConfigDescription("Spill large block to a separate spill file")
    public JavaFeaturesConfig setDistinctAggregationLargeBlockSpillEnabled(boolean distinctAggregationLargeBlockSpillEnabled)
    {
        this.distinctAggregationLargeBlockSpillEnabled = distinctAggregationLargeBlockSpillEnabled;
        return this;
    }

    public boolean isDedupBasedDistinctAggregationSpillEnabled()
    {
        return dedupBasedDistinctAggregationSpillEnabled;
    }

    @Config("experimental.dedup-based-distinct-aggregation-spill-enabled")
    @ConfigDescription("Dedup input data for Distinct Aggregates before spilling")
    public JavaFeaturesConfig setDedupBasedDistinctAggregationSpillEnabled(boolean dedupBasedDistinctAggregationSpillEnabled)
    {
        this.dedupBasedDistinctAggregationSpillEnabled = dedupBasedDistinctAggregationSpillEnabled;
        return this;
    }

    public boolean isAggregationSpillEnabled()
    {
        return aggregationSpillEnabled;
    }

    @Config("experimental.aggregation-spill-enabled")
    @ConfigDescription("Spill aggregations if spill is enabled")
    public JavaFeaturesConfig setAggregationSpillEnabled(boolean aggregationSpillEnabled)
    {
        this.aggregationSpillEnabled = aggregationSpillEnabled;
        return this;
    }

    public boolean isTableWriterMergeOperatorEnabled()
    {
        return tableWriterMergeOperatorEnabled;
    }

    @Config("experimental.table-writer-merge-operator-enabled")
    public JavaFeaturesConfig setTableWriterMergeOperatorEnabled(boolean tableWriterMergeOperatorEnabled)
    {
        this.tableWriterMergeOperatorEnabled = tableWriterMergeOperatorEnabled;
        return this;
    }

    public boolean isOptimizeHashGeneration()
    {
        return optimizeHashGeneration;
    }

    @Config("optimizer.optimize-hash-generation")
    public JavaFeaturesConfig setOptimizeHashGeneration(boolean optimizeHashGeneration)
    {
        this.optimizeHashGeneration = optimizeHashGeneration;
        return this;
    }

    @Config("experimental.topn-spill-enabled")
    @ConfigDescription("Spill TopN if spill is enabled")
    public JavaFeaturesConfig setTopNSpillEnabled(boolean topNSpillEnabled)
    {
        this.topNSpillEnabled = topNSpillEnabled;
        return this;
    }

    public boolean isTopNSpillEnabled()
    {
        return topNSpillEnabled;
    }

    public boolean isDistinctAggregationSpillEnabled()
    {
        return distinctAggregationSpillEnabled;
    }

    @Config("experimental.distinct-aggregation-spill-enabled")
    @ConfigDescription("Spill distinct aggregations if aggregation spill is enabled")
    public JavaFeaturesConfig setDistinctAggregationSpillEnabled(boolean distinctAggregationSpillEnabled)
    {
        this.distinctAggregationSpillEnabled = distinctAggregationSpillEnabled;
        return this;
    }

    @Config("experimental.aggregation-operator-unspill-memory-limit")
    public JavaFeaturesConfig setAggregationOperatorUnspillMemoryLimit(DataSize aggregationOperatorUnspillMemoryLimit)
    {
        this.aggregationOperatorUnspillMemoryLimit = aggregationOperatorUnspillMemoryLimit;
        return this;
    }

    public DataSize getAggregationOperatorUnspillMemoryLimit()
    {
        return aggregationOperatorUnspillMemoryLimit;
    }

    public DataSize getTopNOperatorUnspillMemoryLimit()
    {
        return topNOperatorUnspillMemoryLimit;
    }

    @Config("experimental.topn-operator-unspill-memory-limit")
    public JavaFeaturesConfig setTopNOperatorUnspillMemoryLimit(DataSize aggregationOperatorUnspillMemoryLimit)
    {
        this.topNOperatorUnspillMemoryLimit = aggregationOperatorUnspillMemoryLimit;
        return this;
    }

    public boolean isParseDecimalLiteralsAsDouble()
    {
        return parseDecimalLiteralsAsDouble;
    }

    @Config("parse-decimal-literals-as-double")
    public JavaFeaturesConfig setParseDecimalLiteralsAsDouble(boolean parseDecimalLiteralsAsDouble)
    {
        this.parseDecimalLiteralsAsDouble = parseDecimalLiteralsAsDouble;
        return this;
    }

    public boolean isOffsetClauseEnabled()
    {
        return offsetClauseEnabled;
    }

    @Config("offset-clause-enabled")
    @ConfigDescription("Enable support for OFFSET clause")
    public JavaFeaturesConfig setOffsetClauseEnabled(boolean offsetClauseEnabled)
    {
        this.offsetClauseEnabled = offsetClauseEnabled;
        return this;
    }

    public boolean isInlineSqlFunctions()
    {
        return inlineSqlFunctions;
    }

    @Config("inline-sql-functions")
    public JavaFeaturesConfig setInlineSqlFunctions(boolean inlineSqlFunctions)
    {
        this.inlineSqlFunctions = inlineSqlFunctions;
        return this;
    }
}
