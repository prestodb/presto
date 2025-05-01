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
package com.facebook.presto.operator.aggregation.partial;

import com.facebook.airlift.units.DataSize;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class PartialAggregationController
{
    /**
     * Process enough pages to fill up the partial aggregation buffer, before considering disabling partial aggregation.
     * With 16 MB as default partial agg buffer, this means we process at least 24 MB of input data before considering to disable partial agg.
     * We use bytes instead of rows as the floor to disable partial aggregation due to issues with file skew when rows are small. We want to make sure
     * the partial aggregation buffer is fully utilized before making the decision on disabling partial aggregation.
     */
    private static final double DISABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_RATIO = 1.5;
    /**
     * Re-enable partial aggregation periodically, in case later data can be partially aggregated more effectively.
     */
    private static final double ENABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_RATIO = DISABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_RATIO * 200;

    private final DataSize maxPartialAggregationMemorySize;
    private final double uniqueRowsRatioThreshold;

    private volatile boolean partialAggregationDisabled;
    private long totalBytesProcessed;
    private long totalRowsProcessed;
    private long totalUniqueRowsProduced;

    public PartialAggregationController(DataSize maxPartialAggregationMemorySize, double uniqueRowsRatioThreshold)
    {
        this.maxPartialAggregationMemorySize = requireNonNull(maxPartialAggregationMemorySize, "maxPartialMemory is null");
        this.uniqueRowsRatioThreshold = uniqueRowsRatioThreshold;
    }

    public boolean isPartialAggregationDisabled()
    {
        return partialAggregationDisabled;
    }

    public synchronized void onFlush(long bytesProcessed, long rowsProcessed, OptionalLong uniqueRowsProduced)
    {
        if (!partialAggregationDisabled && !uniqueRowsProduced.isPresent()) {
            // when partial aggregation has been re-enabled, ignore stats from disabled flushes
            return;
        }

        totalBytesProcessed += bytesProcessed;
        totalRowsProcessed += rowsProcessed;
        uniqueRowsProduced.ifPresent(value -> totalUniqueRowsProduced += value);

        if (!partialAggregationDisabled && shouldDisablePartialAggregation()) {
            partialAggregationDisabled = true;
        }

        if (partialAggregationDisabled && totalBytesProcessed >= maxPartialAggregationMemorySize.toBytes() * ENABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_RATIO) {
            totalBytesProcessed = 0;
            totalRowsProcessed = 0;
            totalUniqueRowsProduced = 0;
            partialAggregationDisabled = false;
        }
    }

    private boolean shouldDisablePartialAggregation()
    {
        return totalBytesProcessed >= maxPartialAggregationMemorySize.toBytes() * DISABLE_AGGREGATION_BUFFER_SIZE_TO_INPUT_BYTES_RATIO
                && ((double) totalUniqueRowsProduced / totalRowsProcessed) > uniqueRowsRatioThreshold;
    }

    public PartialAggregationController duplicate()
    {
        return new PartialAggregationController(maxPartialAggregationMemorySize, uniqueRowsRatioThreshold);
    }
}
