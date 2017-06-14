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

import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

public class PlanNodeStatsEstimate
{
    public static final PlanNodeStatsEstimate UNKNOWN_STATS = builder().build();
    public static final double DEFAULT_ROW_SIZE = 42;

    private final double outputRowCount;
    private final double outputSizeInBytes;
    private final Map<Symbol, SymbolStatsEstimate> symbolStatistics;

    private PlanNodeStatsEstimate(double outputRowCount, double outputSizeInBytes, Map<Symbol, SymbolStatsEstimate> symbolStatistics)
    {
        checkArgument(isNaN(outputRowCount) || outputRowCount >= 0, "outputRowCount cannot be negative");
        checkArgument(isNaN(outputSizeInBytes) || outputSizeInBytes >= 0, "outputSizeInBytes cannot be negative");
        this.outputRowCount = outputRowCount;
        this.outputSizeInBytes = outputSizeInBytes;
        this.symbolStatistics = ImmutableMap.copyOf(symbolStatistics);
    }

    /**
     * Returns estimated number of rows.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputRowCount()
    {
        return outputRowCount;
    }

    /**
     * Returns estimated data size.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputSizeInBytes()
    {
        return outputSizeInBytes;
    }

    public PlanNodeStatsEstimate mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputRowCount(mappingFunction.apply(outputRowCount)).build();
    }

    public PlanNodeStatsEstimate mapOutputSizeInBytes(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputSizeInBytes(mappingFunction.apply(outputRowCount)).build();
    }

    public PlanNodeStatsEstimate mapSymbolColumnStatistics(Symbol symbol, Function<SymbolStatsEstimate, SymbolStatsEstimate> mappingFunction)
    {
        return buildFrom(this)
                .setSymbolStatistics(symbolStatistics.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> {
                                    if (e.getKey().equals(symbol)) {
                                        return mappingFunction.apply(e.getValue());
                                    }
                                    return e.getValue();
                                })))
                .build();
    }

    public SymbolStatsEstimate getSymbolStatistics(Symbol symbol)
    {
        return symbolStatistics.getOrDefault(symbol, SymbolStatsEstimate.UNKNOWN_STATS);
    }

    public Set<Symbol> getSymbolsWithKnownStatistics()
    {
        return symbolStatistics.keySet();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("outputRowCount", outputRowCount)
                .add("outputSizeInBytes", outputSizeInBytes)
                .add("symbolStatistics", symbolStatistics)
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
        return Double.compare(that.outputRowCount, outputRowCount) == 0 &&
                Double.compare(that.outputSizeInBytes, outputSizeInBytes) == 0 &&
                Objects.equals(symbolStatistics, that.symbolStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCount, outputSizeInBytes, symbolStatistics);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return builder().setOutputRowCount(other.getOutputRowCount())
                .setOutputSizeInBytes(other.getOutputSizeInBytes())
                .setSymbolStatistics(other.symbolStatistics);
    }

    public static final class Builder
    {
        private double outputRowCount = NaN;
        private double outputSizeInBytes = NaN;
        private Map<Symbol, SymbolStatsEstimate> symbolStatistics = new HashMap<>();

        public Builder setOutputRowCount(double outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder setOutputSizeInBytes(double outputSizeInBytes)
        {
            this.outputSizeInBytes = outputSizeInBytes;
            return this;
        }

        public Builder setSymbolStatistics(Map<Symbol, SymbolStatsEstimate> symbolStatistics)
        {
            this.symbolStatistics = new HashMap<>(symbolStatistics);
            return this;
        }

        public Builder addSymbolStatistics(Symbol symbol, SymbolStatsEstimate statistics)
        {
            this.symbolStatistics.put(symbol, statistics);
            return this;
        }

        public PlanNodeStatsEstimate build()
        {
            if (isNaN(outputSizeInBytes) && !isNaN(outputRowCount)) {
                outputSizeInBytes = DEFAULT_ROW_SIZE * outputRowCount;
            }
            return new PlanNodeStatsEstimate(outputRowCount, outputSizeInBytes, symbolStatistics);
        }
    }
}
