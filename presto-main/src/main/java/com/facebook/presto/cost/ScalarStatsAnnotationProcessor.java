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
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.scalar.ScalarFunctionStatsHeader;
import com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior;
import com.facebook.presto.spi.function.scalar.ScalarPropagateSourceStats;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.SUM_ARGUMENTS;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.UNKNOWN;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.USE_SOURCE_STATS;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static com.facebook.presto.util.MoreMath.minExcludingNaNs;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class ScalarStatsAnnotationProcessor
{
    private ScalarStatsAnnotationProcessor()
    {
    }

    /*
     * Canonical (normalized i.e. erased type size bounds) form of signature instance.
     */
    public static Signature eraseTypeBounds(Signature signature)
    {
        return new Signature(signature.getName(), signature.getKind(), new TypeSignature(signature.getReturnType().getBase(), emptyList()),
                signature.getArgumentTypes()
                        .stream()
                        .map(argumentTypeSignature -> new TypeSignature(argumentTypeSignature.getBase(), emptyList())).collect(toList()));
    }

    public static VariableStatsEstimate computeStatistics(
            double outputRowCount,
            CallExpression callExpression,
            List<VariableStatsEstimate> sourceStats,
            ScalarFunctionStatsHeader scalarFunctionStatsHeader)
    {
        double nullFraction = scalarFunctionStatsHeader.getNullFraction();
        double distinctValuesCount = NaN;
        double averageRowSize = NaN;
        double maxValue = scalarFunctionStatsHeader.getMax();
        double minValue = scalarFunctionStatsHeader.getMin();
        for (Map.Entry<Integer, ScalarPropagateSourceStats> paramIndexToStatsMap : scalarFunctionStatsHeader.getArgumentSourceStatsResolver().entrySet()) {
            ScalarPropagateSourceStats scalarPropagateSourceStats = paramIndexToStatsMap.getValue();
            boolean propagateAllStats = scalarPropagateSourceStats.propagateAllStats();
            nullFraction = min(firstFiniteValue(nullFraction, computePropagatedStatsValue(
                    sourceStats.stream().map(VariableStatsEstimate::getNullsFraction).collect(toImmutableList()),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.nullFraction()),
                    outputRowCount, nullFraction, callExpression,
                    paramIndexToStatsMap.getKey())), 1.0);
            distinctValuesCount = firstFiniteValue(distinctValuesCount, computePropagatedStatsValue(
                    sourceStats.stream().map(VariableStatsEstimate::getDistinctValuesCount).collect(toImmutableList()),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.distinctValuesCount()),
                    outputRowCount, nullFraction, callExpression,
                    paramIndexToStatsMap.getKey()));
            ScalarFunctionStatsPropagationBehavior averageRowSizeStatsBehaviour = applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.avgRowSize());
            averageRowSize = minExcludingNaNs(firstFiniteValue(averageRowSize, computePropagatedStatsValue(
                    sourceStats.stream().map(VariableStatsEstimate::getAverageRowSize).collect(toImmutableList()),
                    averageRowSizeStatsBehaviour,
                    outputRowCount, nullFraction, callExpression,
                    paramIndexToStatsMap.getKey())), returnNaNIfTypeWidthUnknown(getReturnTypeWidth(callExpression, averageRowSizeStatsBehaviour)));
            maxValue = firstFiniteValue(maxValue, computePropagatedStatsValue(
                    sourceStats.stream().map(VariableStatsEstimate::getHighValue).collect(toImmutableList()),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.maxValue()),
                    outputRowCount, nullFraction, callExpression,
                    paramIndexToStatsMap.getKey()));
            minValue = firstFiniteValue(minValue, computePropagatedStatsValue(
                    sourceStats.stream().map(VariableStatsEstimate::getLowValue).collect(toImmutableList()),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.minValue()),
                    outputRowCount, nullFraction, callExpression,
                    paramIndexToStatsMap.getKey()));
        }
        if (isNaN(maxValue) || isNaN(minValue)) {
            minValue = NaN;
            maxValue = NaN;
        }
        return VariableStatsEstimate.builder()
                .setLowValue(minValue)
                .setHighValue(maxValue)
                .setNullsFraction(nullFraction)
                .setAverageRowSize(firstFiniteValue(scalarFunctionStatsHeader.getAvgRowSize(), averageRowSize, returnNaNIfTypeWidthUnknown(getReturnTypeWidth(callExpression, UNKNOWN))))
                .setDistinctValuesCount(processDistinctValuesCount(outputRowCount, scalarFunctionStatsHeader.getDistinctValuesCount(), distinctValuesCount)).build();
    }

    private static double processDistinctValuesCount(double outputRowCount, double distinctValuesCountFromConstant, double distinctValuesCount)
    {
        double distinctValuesCountFinal = firstFiniteValue(distinctValuesCountFromConstant, distinctValuesCount);
        if (distinctValuesCountFinal > outputRowCount) {
            distinctValuesCountFinal = NaN;
        }
        return distinctValuesCountFinal;
    }

    private static double computePropagatedStatsValue(
            List<Double> sourceStats,
            ScalarFunctionStatsPropagationBehavior operation,
            double sourceOutputRowCount,
            double sourceNullFraction,
            CallExpression callExpression,
            int sourceStatsArgumentIndex)
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
                    statValue = sourceOutputRowCount;
                    break;
                case NON_NULL_ROW_COUNT:
                    statValue = sourceOutputRowCount * (1 - firstFiniteValue(sourceNullFraction, 0.0));
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

    private static int getReturnTypeWidth(CallExpression callExpression, ScalarFunctionStatsPropagationBehavior operation)
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

    private static ScalarFunctionStatsPropagationBehavior applyPropagateAllStats(
            boolean propagateAllStats, ScalarFunctionStatsPropagationBehavior operation)
    {
        if (operation == UNKNOWN && propagateAllStats) {
            return USE_SOURCE_STATS;
        }
        return operation;
    }
}
