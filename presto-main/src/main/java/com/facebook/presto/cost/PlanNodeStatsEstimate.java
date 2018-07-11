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

import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VariableWidthType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
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
    public static final PlanNodeStatsEstimate UNKNOWN_STATS = builder().build();

    private final double outputRowCount;
    private final PMap<Symbol, SymbolStatsEstimate> symbolStatistics;

    private PlanNodeStatsEstimate(double outputRowCount, PMap<Symbol, SymbolStatsEstimate> symbolStatistics)
    {
        checkArgument(isNaN(outputRowCount) || outputRowCount >= 0, "outputRowCount cannot be negative");
        this.outputRowCount = outputRowCount;
        this.symbolStatistics = symbolStatistics;
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
    public double getOutputSizeInBytes(Collection<Symbol> outputSymbols, TypeProvider types)
    {
        requireNonNull(outputSymbols, "outputSymbols is null");

        return outputSymbols.stream()
                .mapToDouble(symbol -> getOutputSizeForSymbol(getSymbolStatistics(symbol), types.get(symbol)))
                .sum();
    }

    private double getOutputSizeForSymbol(SymbolStatsEstimate symbolStatistics, Type type)
    {
        checkArgument(type != null, "type is null");

        double averageRowSize = symbolStatistics.getAverageRowSize();
        double nullsFraction = firstNonNaN(symbolStatistics.getNullsFraction(), 0d);
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

    public PlanNodeStatsEstimate mapSymbolColumnStatistics(Symbol symbol, Function<SymbolStatsEstimate, SymbolStatsEstimate> mappingFunction)
    {
        return buildFrom(this)
                .addSymbolStatistics(symbol, mappingFunction.apply(getSymbolStatistics(symbol)))
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
        return Double.compare(outputRowCount, that.outputRowCount) == 0 &&
                Objects.equals(symbolStatistics, that.symbolStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCount, symbolStatistics);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return new Builder(other.getOutputRowCount(), other.symbolStatistics);
    }

    public static final class Builder
    {
        private double outputRowCount;
        private PMap<Symbol, SymbolStatsEstimate> symbolStatistics;

        public Builder()
        {
            this(NaN, HashTreePMap.empty());
        }

        private Builder(double outputRowCount, PMap<Symbol, SymbolStatsEstimate> symbolStatistics)
        {
            this.outputRowCount = outputRowCount;
            this.symbolStatistics = symbolStatistics;
        }

        public Builder setOutputRowCount(double outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder addSymbolStatistics(Symbol symbol, SymbolStatsEstimate statistics)
        {
            symbolStatistics = symbolStatistics.plus(symbol, statistics);
            return this;
        }

        public Builder addSymbolStatistics(Map<Symbol, SymbolStatsEstimate> symbolStatistics)
        {
            this.symbolStatistics = this.symbolStatistics.plusAll(symbolStatistics);
            return this;
        }

        public Builder removeSymbolStatistics(Symbol symbol)
        {
            symbolStatistics = symbolStatistics.minus(symbol);
            return this;
        }

        public PlanNodeStatsEstimate build()
        {
            return new PlanNodeStatsEstimate(outputRowCount, symbolStatistics);
        }
    }
}
