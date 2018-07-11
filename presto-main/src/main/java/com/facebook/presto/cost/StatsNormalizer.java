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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.cost.SymbolStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.cost.SymbolStatsEstimate.ZERO_STATS;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Math.floor;
import static java.lang.Math.pow;
import static java.util.Objects.requireNonNull;

/**
 * Makes stats consistent
 */
public class StatsNormalizer
{
    public PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats, TypeProvider types)
    {
        return normalize(stats, Optional.empty(), types);
    }

    public PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats, Collection<Symbol> outputSymbols, TypeProvider types)
    {
        return normalize(stats, Optional.of(outputSymbols), types);
    }

    private PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats, Optional<Collection<Symbol>> outputSymbols, TypeProvider types)
    {
        PlanNodeStatsEstimate.Builder normalized = PlanNodeStatsEstimate.buildFrom(stats);

        Predicate<Symbol> symbolFilter = outputSymbols
                .map(ImmutableSet::copyOf)
                .map(set -> (Predicate<Symbol>) set::contains)
                .orElse(symbol -> true);

        for (Symbol symbol : stats.getSymbolsWithKnownStatistics()) {
            if (!symbolFilter.test(symbol)) {
                normalized.removeSymbolStatistics(symbol);
                continue;
            }

            SymbolStatsEstimate symbolStats = stats.getSymbolStatistics(symbol);
            SymbolStatsEstimate normalizedSymbolStats = normalizeSymbolStats(symbol, symbolStats, stats, types);
            if (UNKNOWN_STATS.equals(normalizedSymbolStats)) {
                normalized.removeSymbolStatistics(symbol);
                continue;
            }
            if (!Objects.equals(normalizedSymbolStats, symbolStats)) {
                normalized.addSymbolStatistics(symbol, normalizedSymbolStats);
            }
        }

        return normalized.build();
    }

    /**
     * Calculates consistent stats for a symbol.
     */
    private SymbolStatsEstimate normalizeSymbolStats(Symbol symbol, SymbolStatsEstimate symbolStats, PlanNodeStatsEstimate stats, TypeProvider types)
    {
        if (UNKNOWN_STATS.equals(symbolStats)) {
            return UNKNOWN_STATS;
        }

        double outputRowCount = stats.getOutputRowCount();
        double distinctValuesCount = symbolStats.getDistinctValuesCount();
        double nullsFraction = symbolStats.getNullsFraction();

        if (!isNaN(distinctValuesCount)) {
            Type type = requireNonNull(types.get(symbol), () -> "No stats for symbol " + symbol);
            double maxDistinctValuesByLowHigh = maxDistinctValuesByLowHigh(symbolStats, type);
            if (distinctValuesCount > maxDistinctValuesByLowHigh) {
                distinctValuesCount = maxDistinctValuesByLowHigh;
            }

            if (distinctValuesCount > outputRowCount) {
                distinctValuesCount = outputRowCount;
            }

            double nonNullValues = outputRowCount * (1 - nullsFraction);
            if (distinctValuesCount > nonNullValues) {
                double difference = distinctValuesCount - nonNullValues;
                distinctValuesCount -= difference / 2;
                nonNullValues += difference / 2;
                nullsFraction = 1 - nonNullValues / outputRowCount;
            }
        }

        if (distinctValuesCount == 0.0) {
            return ZERO_STATS;
        }

        return SymbolStatsEstimate.buildFrom(symbolStats)
                .setDistinctValuesCount(distinctValuesCount)
                .setNullsFraction(nullsFraction)
                .build();
    }

    private double maxDistinctValuesByLowHigh(SymbolStatsEstimate symbolStats, Type type)
    {
        if (symbolStats.statisticRange().length() == 0.0) {
            return 1;
        }

        if (!isDiscrete(type)) {
            return NaN;
        }

        double length = symbolStats.getHighValue() - symbolStats.getLowValue();
        if (isNaN(length)) {
            return NaN;
        }

        if (type instanceof DecimalType) {
            length *= pow(10, ((DecimalType) type).getScale());
        }
        return floor(length + 1);
    }

    private static boolean isDiscrete(Type type)
    {
        return type.equals(IntegerType.INTEGER) ||
                type.equals(BigintType.BIGINT) ||
                type.equals(SmallintType.SMALLINT) ||
                type.equals(TinyintType.TINYINT) ||
                type.equals(BooleanType.BOOLEAN) ||
                type.equals(DateType.DATE) ||
                type instanceof DecimalType;
    }
}
