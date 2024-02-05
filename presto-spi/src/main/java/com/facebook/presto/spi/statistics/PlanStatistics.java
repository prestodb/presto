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

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class PlanStatistics
{
    private static final PlanStatistics EMPTY = new PlanStatistics(Estimate.unknown(), Estimate.unknown(), 0, JoinNodeStatistics.empty(), TableWriterNodeStatistics.empty(), PartialAggregationStatistics.empty());

    private final Estimate rowCount;
    private final Estimate outputSize;
    // A number ranging between 0 and 1, reflecting our confidence in the statistics
    private final double confidence;
    // Join node specific statistics
    private final JoinNodeStatistics joinNodeStatistics;
    // TableWriter node specific statistics
    private final TableWriterNodeStatistics tableWriterNodeStatistics;
    // Aggregation node specific statistics
    private final PartialAggregationStatistics partialAggregationStatistics;

    public static PlanStatistics empty()
    {
        return EMPTY;
    }

    @JsonCreator
    @ThriftConstructor
    public PlanStatistics(@JsonProperty("rowCount") Estimate rowCount,
            @JsonProperty("outputSize") Estimate outputSize,
            @JsonProperty("confidence") double confidence,
            @JsonProperty("joinNodeStatistics") JoinNodeStatistics joinNodeStatistics,
            @JsonProperty("tableWriterNodeStatistics") TableWriterNodeStatistics tableWriterNodeStatistics,
            @JsonProperty("partialAggregationStatistics") PartialAggregationStatistics partialAggregationStatistics)
    {
        this.rowCount = requireNonNull(rowCount, "rowCount is null");
        this.outputSize = requireNonNull(outputSize, "outputSize is null");
        checkArgument(confidence >= 0 && confidence <= 1, "confidence should be between 0 and 1");
        this.confidence = confidence;
        this.joinNodeStatistics = requireNonNull(joinNodeStatistics == null ? JoinNodeStatistics.empty() : joinNodeStatistics, "joinNodeStatistics is null");
        this.tableWriterNodeStatistics = requireNonNull(tableWriterNodeStatistics == null ? TableWriterNodeStatistics.empty() : tableWriterNodeStatistics, "tableWriterNodeStatistics is null");
        this.partialAggregationStatistics = requireNonNull(partialAggregationStatistics == null ? PartialAggregationStatistics.empty() : partialAggregationStatistics, "partialAggregationStatistics is null");
    }

    @JsonProperty
    @ThriftField(1)
    public Estimate getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    @ThriftField(2)
    public Estimate getOutputSize()
    {
        return outputSize;
    }

    @JsonProperty
    @ThriftField(3)
    public double getConfidence()
    {
        return confidence;
    }

    @JsonProperty
    @ThriftField(value = 6, requiredness = OPTIONAL)
    public JoinNodeStatistics getJoinNodeStatistics()
    {
        return joinNodeStatistics;
    }

    @JsonProperty
    @ThriftField(value = 7, requiredness = OPTIONAL)
    public TableWriterNodeStatistics getTableWriterNodeStatistics()
    {
        return tableWriterNodeStatistics;
    }

    @JsonProperty
    @ThriftField(value = 8, requiredness = OPTIONAL)
    public PartialAggregationStatistics getPartialAggregationStatistics()
    {
        return partialAggregationStatistics;
    }

    // Next ThriftField value 9

    public PlanStatistics update(PlanStatistics planStatistics)
    {
        return new PlanStatistics(planStatistics.getRowCount(),
                planStatistics.getOutputSize(),
                planStatistics.getConfidence(),
                planStatistics.getJoinNodeStatistics().isEmpty() ? getJoinNodeStatistics() : planStatistics.getJoinNodeStatistics(),
                planStatistics.getTableWriterNodeStatistics().isEmpty() ? getTableWriterNodeStatistics() : planStatistics.getTableWriterNodeStatistics(),
                planStatistics.getPartialAggregationStatistics().isEmpty() ? getPartialAggregationStatistics() : planStatistics.getPartialAggregationStatistics());
    }

    public PlanStatistics updateAggregationStatistics(PartialAggregationStatistics partialAggregationStatistics)
    {
        return new PlanStatistics(getRowCount(),
                getOutputSize(),
                getConfidence(),
                getJoinNodeStatistics(),
                getTableWriterNodeStatistics(),
                partialAggregationStatistics);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
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
        PlanStatistics that = (PlanStatistics) o;
        return Double.compare(that.confidence, confidence) == 0 && Objects.equals(rowCount, that.rowCount) && Objects.equals(outputSize, that.outputSize)
                && Objects.equals(joinNodeStatistics, that.joinNodeStatistics) && Objects.equals(tableWriterNodeStatistics, that.tableWriterNodeStatistics)
                && Objects.equals(partialAggregationStatistics, that.partialAggregationStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowCount, outputSize, confidence, joinNodeStatistics, tableWriterNodeStatistics, partialAggregationStatistics);
    }

    @Override
    public String toString()
    {
        return "PlanStatistics{" +
                "rowCount=" + rowCount +
                ", outputSize=" + outputSize +
                ", confidence=" + confidence +
                ", joinNodeStatistics=" + joinNodeStatistics +
                ", tableWriterNodeStatistics=" + tableWriterNodeStatistics +
                ", partialAggregationStatistics=" + partialAggregationStatistics +
                '}';
    }
}
