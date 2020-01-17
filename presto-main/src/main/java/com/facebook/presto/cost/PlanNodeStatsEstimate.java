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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VariableWidthType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.util.MoreMath.firstNonNaN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class PlanNodeStatsEstimate
{
    private static final double DEFAULT_DATA_SIZE_PER_COLUMN = 50;
    private static final PlanNodeStatsEstimate UNKNOWN = new PlanNodeStatsEstimate(NaN, ImmutableMap.of());

    private final double outputRowCount;
    private final PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics;

    public static PlanNodeStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    @JsonCreator
    public PlanNodeStatsEstimate(
            @JsonProperty("outputRowCount") double outputRowCount,
            @JsonProperty("variableStatistics") Map<VariableReferenceExpression, VariableStatsEstimate> variableStatistics)
    {
        this(outputRowCount, HashTreePMap.from(requireNonNull(variableStatistics, "variableStatistics is null")));
    }

    private PlanNodeStatsEstimate(double outputRowCount, PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics)
    {
        checkArgument(isNaN(outputRowCount) || outputRowCount >= 0, "outputRowCount cannot be negative");
        this.outputRowCount = outputRowCount;
        this.variableStatistics = variableStatistics;
    }

    /**
     * Returns estimated number of rows.
     * Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getOutputRowCount()
    {
        return outputRowCount;
    }

    /**
     * Returns estimated data size.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputSizeInBytes(Collection<VariableReferenceExpression> outputVariables)
    {
        requireNonNull(outputVariables, "outputSymbols is null");

        return outputVariables.stream()
                .mapToDouble(variable -> getOutputSizeForVariable(getVariableStatistics(variable), variable.getType()))
                .sum();
    }

    private double getOutputSizeForVariable(VariableStatsEstimate variableStatistics, Type type)
    {
        checkArgument(type != null, "type is null");

        double averageRowSize = variableStatistics.getAverageRowSize();
        double nullsFraction = firstNonNaN(variableStatistics.getNullsFraction(), 0d);
        double numberOfNonNullRows = outputRowCount * (1.0 - nullsFraction);

        if (isNaN(averageRowSize)) {
            if (type instanceof FixedWidthType) {
                averageRowSize = ((FixedWidthType) type).getFixedSize();
            }
            else {
                averageRowSize = DEFAULT_DATA_SIZE_PER_COLUMN;
            }
        }

        double outputSize = numberOfNonNullRows * averageRowSize;

        // account for "is null" boolean array
        outputSize += outputRowCount * Byte.BYTES;

        // account for offsets array for variable width types
        if (type instanceof VariableWidthType) {
            outputSize += outputRowCount * Integer.BYTES;
        }

        return outputSize;
    }

    public PlanNodeStatsEstimate mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputRowCount(mappingFunction.apply(outputRowCount)).build();
    }

    public PlanNodeStatsEstimate mapVariableColumnStatistics(VariableReferenceExpression variable, Function<VariableStatsEstimate, VariableStatsEstimate> mappingFunction)
    {
        return buildFrom(this)
                .addVariableStatistics(variable, mappingFunction.apply(getVariableStatistics(variable)))
                .build();
    }

    public VariableStatsEstimate getVariableStatistics(VariableReferenceExpression variable)
    {
        return variableStatistics.getOrDefault(variable, VariableStatsEstimate.unknown());
    }

    @JsonProperty
    public Map<VariableReferenceExpression, VariableStatsEstimate> getVariableStatistics()
    {
        return variableStatistics;
    }

    public Set<VariableReferenceExpression> getVariablesWithKnownStatistics()
    {
        return variableStatistics.keySet();
    }

    public boolean isOutputRowCountUnknown()
    {
        return isNaN(outputRowCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("outputRowCount", outputRowCount)
                .add("variableStatistics", variableStatistics)
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
        PlanNodeStatsEstimate that = (PlanNodeStatsEstimate) o;
        return Double.compare(outputRowCount, that.outputRowCount) == 0 &&
                Objects.equals(variableStatistics, that.variableStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCount, variableStatistics);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return new Builder(other.getOutputRowCount(), other.variableStatistics);
    }

    public static final class Builder
    {
        private double outputRowCount;
        private PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics;

        public Builder()
        {
            this(NaN, HashTreePMap.empty());
        }

        private Builder(double outputRowCount, PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics)
        {
            this.outputRowCount = outputRowCount;
            this.variableStatistics = variableStatistics;
        }

        public Builder setOutputRowCount(double outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder addVariableStatistics(VariableReferenceExpression variable, VariableStatsEstimate statistics)
        {
            variableStatistics = variableStatistics.plus(variable, statistics);
            return this;
        }

        public Builder addVariableStatistics(Map<VariableReferenceExpression, VariableStatsEstimate> variableStatistics)
        {
            this.variableStatistics = this.variableStatistics.plusAll(variableStatistics);
            return this;
        }

        public Builder removeVariableStatistics(VariableReferenceExpression variable)
        {
            variableStatistics = variableStatistics.minus(variable);
            return this;
        }

        public PlanNodeStatsEstimate build()
        {
            return new PlanNodeStatsEstimate(outputRowCount, variableStatistics);
        }
    }
}
