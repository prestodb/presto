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
package com.facebook.presto.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Double.NaN;

public class PartialAggregationStatsEstimate
{
    private static final PartialAggregationStatsEstimate UNKNOWN = new PartialAggregationStatsEstimate(NaN, NaN, NaN, NaN);

    private final double inputBytes;
    private final double outputBytes;

    private final double inputRowCount;
    private final double outputRowCount;

    @JsonCreator
    public PartialAggregationStatsEstimate(@JsonProperty("inputBytes") double inputBytes, @JsonProperty("outputBytes") double outputBytes,
            @JsonProperty("inputRowCount") double inputRowCount, @JsonProperty("outputRowCount") double outputRowCount)
    {
        this.inputBytes = inputBytes;
        this.outputBytes = outputBytes;
        this.inputRowCount = inputRowCount;
        this.outputRowCount = outputRowCount;
    }

    public static PartialAggregationStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    @JsonProperty
    public double getInputBytes()
    {
        return inputBytes;
    }

    @JsonProperty
    public double getOutputBytes()
    {
        return outputBytes;
    }

    @JsonProperty
    public double getInputRowCount()
    {
        return inputRowCount;
    }

    @JsonProperty
    public double getOutputRowCount()
    {
        return outputRowCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("inputBytes", inputBytes)
                .add("outputBytes", outputBytes)
                .add("inputRowCount", inputRowCount)
                .add("outputRowCount", outputRowCount)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartialAggregationStatsEstimate that = (PartialAggregationStatsEstimate) o;
        return Double.compare(inputBytes, that.inputBytes) == 0 &&
                Double.compare(outputBytes, that.outputBytes) == 0 &&
                Double.compare(inputRowCount, that.inputRowCount) == 0 &&
                Double.compare(outputRowCount, that.outputRowCount) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(inputBytes, outputBytes, inputRowCount, outputRowCount);
    }

    public static boolean isUnknown(PartialAggregationStatsEstimate partialAggregationStatsEstimate)
    {
        return partialAggregationStatsEstimate == PartialAggregationStatsEstimate.unknown();
    }
}
