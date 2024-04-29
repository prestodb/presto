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
package com.facebook.presto.spi.statistics;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class PartialAggregationStatistics
{
    private static final PartialAggregationStatistics EMPTY = new PartialAggregationStatistics(Estimate.unknown(), Estimate.unknown(), Estimate.unknown(), Estimate.unknown());
    // Number of input bytes
    private final Estimate partialAggregationInputBytes;
    // Number of output bytes
    private final Estimate partialAggregationOutputBytes;

    private final Estimate partialAggregationInputRows;

    private final Estimate partialAggregationOutputRows;

    @JsonCreator
    @ThriftConstructor
    public PartialAggregationStatistics(
            @JsonProperty("partialAggregationInputBytes") Estimate partialAggregationInputBytes,
            @JsonProperty("partialAggregationOutputBytes") Estimate partialAggregationOutputBytes,
            @JsonProperty("partialAggregationInputRows") Estimate partialAggregationInputRows,
            @JsonProperty("partialAggregationOutputRows") Estimate partialAggregationOutputRows)
    {
        this.partialAggregationInputBytes = requireNonNull(partialAggregationInputBytes, "partialAggregationInputBytes is null");
        this.partialAggregationOutputBytes = requireNonNull(partialAggregationOutputBytes, "partialAggregationOutputBytes is null");
        this.partialAggregationInputRows = requireNonNull(partialAggregationInputRows, "partialAggregationInputRows is null");
        this.partialAggregationOutputRows = requireNonNull(partialAggregationOutputRows, "partialAggregationOutputRows is null");
    }

    public static PartialAggregationStatistics empty()
    {
        return EMPTY;
    }

    public boolean isEmpty()
    {
        return this.equals(empty());
    }

    @JsonProperty
    @ThriftField(1)
    public Estimate getPartialAggregationInputBytes()
    {
        return partialAggregationInputBytes;
    }

    @JsonProperty
    @ThriftField(2)
    public Estimate getPartialAggregationOutputBytes()
    {
        return partialAggregationOutputBytes;
    }

    @JsonProperty
    @ThriftField(3)
    public Estimate getPartialAggregationInputRows()
    {
        return partialAggregationInputRows;
    }

    @JsonProperty
    @ThriftField(4)
    public Estimate getPartialAggregationOutputRows()
    {
        return partialAggregationOutputRows;
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
        PartialAggregationStatistics that = (PartialAggregationStatistics) o;
        return Objects.equals(partialAggregationInputBytes, that.partialAggregationInputBytes) &&
                Objects.equals(partialAggregationOutputBytes, that.partialAggregationOutputBytes) &&
                Objects.equals(partialAggregationInputRows, that.partialAggregationInputRows) &&
                Objects.equals(partialAggregationOutputRows, that.partialAggregationOutputRows);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partialAggregationInputBytes, partialAggregationOutputBytes, partialAggregationInputRows, partialAggregationOutputRows);
    }

    @Override
    public String toString()
    {
        return "PartialAggregationStatistics{" +
                "partialAggregationInputBytes=" + partialAggregationInputBytes +
                ", partialAggregationOutputBytes=" + partialAggregationOutputBytes +
                "partialAggregationInputRows=" + partialAggregationInputRows +
                ", partialAggregationOutputRows=" + partialAggregationOutputRows +
                '}';
    }
}
