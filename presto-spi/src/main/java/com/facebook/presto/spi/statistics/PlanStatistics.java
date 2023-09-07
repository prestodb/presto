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
    private static final PlanStatistics EMPTY = new PlanStatistics(Estimate.unknown(), Estimate.unknown(), 0, JoinNodeStatistics.empty());

    private final Estimate rowCount;
    private final Estimate outputSize;
    // A number ranging between 0 and 1, reflecting our confidence in the statistics
    private final double confidence;
    // Join node specific statistics
    private final JoinNodeStatistics joinNodeStatistics;

    public static PlanStatistics empty()
    {
        return EMPTY;
    }

    @JsonCreator
    @ThriftConstructor
    public PlanStatistics(@JsonProperty("rowCount") Estimate rowCount,
            @JsonProperty("outputSize") Estimate outputSize,
            @JsonProperty("confidence") double confidence,
            @JsonProperty("nullJoinBuildKeyCount") Estimate nullJoinBuildKeyCount,
            @JsonProperty("joinBuildKeyCount") Estimate joinBuildKeyCount,
            @JsonProperty("joinNodeStatistics") JoinNodeStatistics joinNodeStatistics)
    {
        this(rowCount, outputSize, confidence, joinNodeStatistics);
    }

    public PlanStatistics(Estimate rowCount,
            Estimate outputSize,
            double confidence,
            JoinNodeStatistics joinNodeStatistics)
    {
        this.rowCount = requireNonNull(rowCount, "rowCount is null");
        this.outputSize = requireNonNull(outputSize, "outputSize is null");
        checkArgument(confidence >= 0 && confidence <= 1, "confidence should be between 0 and 1");
        this.confidence = confidence;
        this.joinNodeStatistics = requireNonNull(joinNodeStatistics == null ? JoinNodeStatistics.empty() : joinNodeStatistics, "joinNodeStatistics is null");
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
    @ThriftField(4)
    public Estimate getNullJoinBuildKeyCount()
    {
        return Estimate.unknown();
    }

    @JsonProperty
    @ThriftField(5)
    public Estimate getJoinBuildKeyCount()
    {
        return Estimate.unknown();
    }

    @JsonProperty
    @ThriftField(value = 6, requiredness = OPTIONAL)
    public JoinNodeStatistics getJoinNodeStatistics()
    {
        return joinNodeStatistics;
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
                && Objects.equals(joinNodeStatistics, that.joinNodeStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowCount, outputSize, confidence, joinNodeStatistics);
    }

    @Override
    public String toString()
    {
        return "PlanStatistics{" +
                "rowCount=" + rowCount +
                ", outputSize=" + outputSize +
                ", confidence=" + confidence +
                ", joinNodeStatistics=" + joinNodeStatistics +
                '}';
    }
}
