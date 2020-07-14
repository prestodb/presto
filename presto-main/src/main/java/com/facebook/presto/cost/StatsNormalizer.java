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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Math.floor;
import static java.lang.Math.pow;

/**
 * Makes stats consistent
 */
public class StatsNormalizer
{
    public PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats)
    {
        return normalize(stats, Optional.empty());
    }

    public PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats, Collection<VariableReferenceExpression> outputVariables)
    {
        return normalize(stats, Optional.of(outputVariables));
    }

    private PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats, Optional<Collection<VariableReferenceExpression>> outputVariables)
    {
        if (stats.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder normalized = PlanNodeStatsEstimate.buildFrom(stats)
                .setTotalSize(stats.getOutputSizeInBytes());

        Predicate<VariableReferenceExpression> variableFilter = outputVariables
                .map(ImmutableSet::copyOf)
                .map(set -> (Predicate<VariableReferenceExpression>) set::contains)
                .orElse(variable -> true);

        for (VariableReferenceExpression variable : stats.getVariablesWithKnownStatistics()) {
            if (!variableFilter.test(variable)) {
                normalized.removeVariableStatistics(variable);
                continue;
            }

            VariableStatsEstimate variableStats = stats.getVariableStatistics(variable);
            VariableStatsEstimate normalizedSymbolStats = stats.getOutputRowCount() == 0 ? VariableStatsEstimate.zero() : normalizeVariableStats(variable, variableStats, stats);
            if (normalizedSymbolStats.isUnknown()) {
                normalized.removeVariableStatistics(variable);
                continue;
            }
            if (!Objects.equals(normalizedSymbolStats, variableStats)) {
                normalized.addVariableStatistics(variable, normalizedSymbolStats);
            }
        }

        return normalized.build();
    }

    /**
     * Calculates consistent stats for a symbol.
     */
    private VariableStatsEstimate normalizeVariableStats(VariableReferenceExpression variable, VariableStatsEstimate variableStats, PlanNodeStatsEstimate stats)
    {
        if (variableStats.isUnknown()) {
            return VariableStatsEstimate.unknown();
        }

        double outputRowCount = stats.getOutputRowCount();
        checkArgument(outputRowCount > 0, "outputRowCount must be greater than zero: %s", outputRowCount);
        double distinctValuesCount = variableStats.getDistinctValuesCount();
        double nullsFraction = variableStats.getNullsFraction();

        if (!isNaN(distinctValuesCount)) {
            Type type = variable.getType();
            double maxDistinctValuesByLowHigh = maxDistinctValuesByLowHigh(variableStats, type);
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
            return VariableStatsEstimate.zero();
        }

        return VariableStatsEstimate.buildFrom(variableStats)
                .setDistinctValuesCount(distinctValuesCount)
                .setNullsFraction(nullsFraction)
                .build();
    }

    private double maxDistinctValuesByLowHigh(VariableStatsEstimate variableStats, Type type)
    {
        if (variableStats.statisticRange().length() == 0.0) {
            return 1;
        }

        if (!isDiscrete(type)) {
            return NaN;
        }

        double length = variableStats.getHighValue() - variableStats.getLowValue();
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
