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

import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.ScalarStatsHeader;
import com.facebook.presto.spi.function.StatsPropagationBehavior;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.function.StatsPropagationBehavior.SUM_ARGUMENTS;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.UNKNOWN;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.USE_SOURCE_STATS;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static com.facebook.presto.util.MoreMath.minExcludingNaNs;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public final class ScalarStatsAnnotationProcessor
{
    private ScalarStatsAnnotationProcessor()
    {
    }

    public static VariableStatsEstimate process(
            double outputRowCount,
            CallExpression callExpression,
            List<VariableStatsEstimate> sourceStats,
            ScalarStatsHeader scalarStatsHeader)
    {
        double nullFraction = scalarStatsHeader.getNullFraction();
        double distinctValuesCount = NaN;
        double averageRowSize = NaN;
        double maxValue = scalarStatsHeader.getMax();
        double minValue = scalarStatsHeader.getMin();
        for (Map.Entry<Integer, ScalarPropagateSourceStats> paramIndexToStatsMap : scalarStatsHeader.getArgumentStats().entrySet()) {
            ScalarPropagateSourceStats scalarPropagateSourceStats = paramIndexToStatsMap.getValue();
            boolean propagateAllStats = scalarPropagateSourceStats.propagateAllStats();
            nullFraction = min(firstFiniteValue(nullFraction, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getNullsFraction).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.nullFraction()))), 1.0);
            distinctValuesCount = firstFiniteValue(distinctValuesCount, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getDistinctValuesCount).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.distinctValuesCount())));
            StatsPropagationBehavior averageRowSizeStatsBehaviour = applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.avgRowSize());
            averageRowSize = minExcludingNaNs(firstFiniteValue(averageRowSize, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getAverageRowSize).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    averageRowSizeStatsBehaviour)), returnNaNIfTypeWidthUnknown(getReturnTypeWidth(callExpression, averageRowSizeStatsBehaviour)));
            maxValue = firstFiniteValue(maxValue, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getHighValue).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.maxValue())));
            minValue = firstFiniteValue(minValue, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getLowValue).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.minValue())));
        }
        if (isNaN(maxValue) || isNaN(minValue)) {
            minValue = NaN;
            maxValue = NaN;
        }
        return VariableStatsEstimate.builder()
                .setLowValue(minValue)
                .setHighValue(maxValue)
                .setNullsFraction(nullFraction)
                .setAverageRowSize(firstFiniteValue(scalarStatsHeader.getAvgRowSize(), averageRowSize, returnNaNIfTypeWidthUnknown(getReturnTypeWidth(callExpression, UNKNOWN))))
                .setDistinctValuesCount(processDistinctValuesCount(outputRowCount, scalarStatsHeader.getDistinctValuesCount(), distinctValuesCount)).build();
    }

    private static double processDistinctValuesCount(double outputRowCount, double distinctValuesCountFromConstant, double distinctValuesCount)
    {
        double distinctValuesCountFinal = firstFiniteValue(distinctValuesCountFromConstant, distinctValuesCount);
        if (distinctValuesCountFinal > outputRowCount) {
            distinctValuesCountFinal = NaN;
        }
        return distinctValuesCountFinal;
    }

    private static double processSingleArgumentStatistic(
            double outputRowCount,
            double nullFraction,
            CallExpression callExpression,
            List<Double> sourceStats,
            int sourceStatsArgumentIndex,
            StatsPropagationBehavior operation)
    {
        // sourceStatsArgumentIndex is index of the argument on which
        // ScalarPropagateSourceStats annotation was applied.
        double statValue = NaN;
        if (operation.isMultiArgumentStat()) {
            for (int i = 0; i < sourceStats.size(); i++) {
                if (i == 0 && operation.isSourceStatsDependentStats() && isFinite(sourceStats.get(i))) {
                    statValue = sourceStats.get(i);
                }
                else {
                    switch (operation) {
                        case MAX_TYPE_WIDTH_VARCHAR:
                            statValue = returnNaNIfTypeWidthUnknown(getTypeWidthVarchar(callExpression.getArguments().get(i).getType()));
                            break;
                        case USE_MIN_ARGUMENT:
                            statValue = min(statValue, sourceStats.get(i));
                            break;
                        case USE_MAX_ARGUMENT:
                            statValue = max(statValue, sourceStats.get(i));
                            break;
                        case SUM_ARGUMENTS:
                            statValue = statValue + sourceStats.get(i);
                            break;
                    }
                }
            }
        }
        else {
            switch (operation) {
                case USE_SOURCE_STATS:
                    statValue = sourceStats.get(sourceStatsArgumentIndex);
                    break;
                case ROW_COUNT:
                    statValue = outputRowCount;
                    break;
                case NON_NULL_ROW_COUNT:
                    statValue = outputRowCount * (1 - firstFiniteValue(nullFraction, 0.0));
                    break;
                case USE_TYPE_WIDTH_VARCHAR:
                    statValue = returnNaNIfTypeWidthUnknown(getTypeWidthVarchar(callExpression.getArguments().get(sourceStatsArgumentIndex).getType()));
                    break;
            }
        }
        return statValue;
    }

    private static int getTypeWidthVarchar(Type argumentType)
    {
        if (argumentType instanceof VarcharType) {
            if (!((VarcharType) argumentType).isUnbounded()) {
                return ((VarcharType) argumentType).getLengthSafe();
            }
        }
        return -VarcharType.MAX_LENGTH;
    }

    private static double returnNaNIfTypeWidthUnknown(int typeWidthValue)
    {
        if (typeWidthValue <= 0) {
            return NaN;
        }
        return typeWidthValue;
    }

    private static int getReturnTypeWidth(CallExpression callExpression, StatsPropagationBehavior operation)
    {
        if (callExpression.getType() instanceof FixedWidthType) {
            return ((FixedWidthType) callExpression.getType()).getFixedSize();
        }
        if (callExpression.getType() instanceof VarcharType) {
            VarcharType returnType = (VarcharType) callExpression.getType();
            if (!returnType.isUnbounded()) {
                return returnType.getLengthSafe();
            }
            if (operation == SUM_ARGUMENTS) {
                // since return type is an unbounded varchar and operation is SUM_ARGUMENTS,
                // calculating the type width by doing a SUM of each argument's varchar type bounds - if available.
                int sum = 0;
                for (RowExpression r : callExpression.getArguments()) {
                    int typeWidth;
                    if (r instanceof CallExpression) { // argument is another function call
                        typeWidth = getReturnTypeWidth((CallExpression) r, UNKNOWN);
                    }
                    else {
                        typeWidth = getTypeWidthVarchar(r.getType());
                    }
                    if (typeWidth < 0) {
                        return -VarcharType.MAX_LENGTH;
                    }
                    sum += typeWidth;
                }
                return sum;
            }
        }
        return -VarcharType.MAX_LENGTH;
    }

    // Return first 'finite' value from values, else return values[0]
    private static double firstFiniteValue(double... values)
    {
        checkArgument(values.length > 1);
        for (double v : values) {
            if (isFinite(v)) {
                return v;
            }
        }
        return values[0];
    }

    private static StatsPropagationBehavior applyPropagateAllStats(
            boolean propagateAllStats, StatsPropagationBehavior operation)
    {
        if (operation == UNKNOWN && propagateAllStats) {
            return USE_SOURCE_STATS;
        }
        return operation;
    }
}
