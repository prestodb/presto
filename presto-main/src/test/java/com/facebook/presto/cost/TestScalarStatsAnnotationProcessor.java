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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.BuiltInFunctionHandle;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.scalar.ScalarFunctionConstantStats;
import com.facebook.presto.spi.function.scalar.ScalarFunctionStatsHeader;
import com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior;
import com.facebook.presto.spi.function.scalar.ScalarPropagateSourceStats;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.MAX_TYPE_WIDTH_VARCHAR;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.NON_NULL_ROW_COUNT;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.ROW_COUNT;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.SUM_ARGUMENTS;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.UNKNOWN;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.USE_MAX_ARGUMENT;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.USE_MIN_ARGUMENT;
import static com.facebook.presto.spi.function.scalar.ScalarFunctionStatsPropagationBehavior.USE_SOURCE_STATS;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;

public class TestScalarStatsAnnotationProcessor
{
    private static final VariableStatsEstimate STATS_ESTIMATE_FINITE = VariableStatsEstimate.builder()
            .setLowValue(1.0)
            .setHighValue(120.0)
            .setNullsFraction(0.1)
            .setAverageRowSize(15.0)
            .setDistinctValuesCount(23.0)
            .build();
    private static final ScalarFunctionConstantStats CONSTANT_STATS_UNKNOWN = createScalarFunctionConstantStatsInstance(NEGATIVE_INFINITY, POSITIVE_INFINITY, NaN, NaN, NaN);
    private static final VariableStatsEstimate STATS_ESTIMATE_UNKNOWN = VariableStatsEstimate.unknown();
    private static final List<VariableStatsEstimate> STATS_ESTIMATE_LIST = ImmutableList.of(STATS_ESTIMATE_FINITE, STATS_ESTIMATE_FINITE);
    private static final List<VariableStatsEstimate> STATS_ESTIMATE_LIST_WITH_UNKNOWN = ImmutableList.of(STATS_ESTIMATE_FINITE, STATS_ESTIMATE_UNKNOWN);
    private static final TypeSignature VARCHAR_TYPE_10 = createVarcharType(10).getTypeSignature();
    private static final List<RowExpression> TWO_ARGUMENTS = ImmutableList.of(
            new VariableReferenceExpression(Optional.empty(), "x", createVarcharType(10)),
            new VariableReferenceExpression(Optional.empty(), "y", createVarcharType(10)));

    @Test
    public void testComputeStatisticsConstantStatsTakePrecedence()
    {
        Signature signature = new Signature(QualifiedObjectName.valueOf("presto.default.test"), SCALAR, VARCHAR_TYPE_10);
        CallExpression callExpression =
                new CallExpression("test", new BuiltInFunctionHandle(signature), createVarcharType(10),
                        ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "y", VARCHAR)));
        ScalarFunctionStatsHeader scalarFunctionStatsHeader = new ScalarFunctionStatsHeader(
                ImmutableMap.of(1, createScalarPropagateSourceStatsInstance(true, UNKNOWN, ROW_COUNT, UNKNOWN, UNKNOWN, UNKNOWN)),
                createScalarFunctionConstantStatsInstance(1, 10, 0.1, 2.3, 25));
        VariableStatsEstimate actualStats = ScalarStatsAnnotationProcessor.computeStatistics(1000, callExpression, STATS_ESTIMATE_LIST, scalarFunctionStatsHeader);
        VariableStatsEstimate expectedStats = VariableStatsEstimate.builder()
                .setLowValue(1)
                .setHighValue(10)
                .setNullsFraction(0.1)
                .setAverageRowSize(2.3)
                .setDistinctValuesCount(25)
                .build();
        assertEquals(actualStats, expectedStats);
    }

    @Test
    public void testComputeStatisticsNaNSourceStats()
    {
        Signature signature = new Signature(QualifiedObjectName.valueOf("presto.default.test"), SCALAR, VARCHAR_TYPE_10,
                VARCHAR_TYPE_10, VARCHAR_TYPE_10);
        CallExpression callExpression =
                new CallExpression("test", new BuiltInFunctionHandle(signature), createVarcharType(10), TWO_ARGUMENTS);
        ScalarFunctionStatsHeader scalarFunctionStatsHeader = new ScalarFunctionStatsHeader(
                ImmutableMap.of(1,
                        createScalarPropagateSourceStatsInstance(true, USE_SOURCE_STATS, USE_MAX_ARGUMENT, SUM_ARGUMENTS, SUM_ARGUMENTS, NON_NULL_ROW_COUNT)),
                CONSTANT_STATS_UNKNOWN);
        VariableStatsEstimate actualStats =
                ScalarStatsAnnotationProcessor.computeStatistics(1000, callExpression, STATS_ESTIMATE_LIST_WITH_UNKNOWN, scalarFunctionStatsHeader);
        VariableStatsEstimate expectedStats = VariableStatsEstimate
                .buildFrom(VariableStatsEstimate.unknown())
                .setDistinctValuesCount(1000)
                .setAverageRowSize(10.0).build();
        assertEquals(actualStats, expectedStats);
    }

    @Test
    public void testComputeStatisticsTypeWidthBoundaryConditions()
    {
        VariableStatsEstimate statsEstimateLarge =
                VariableStatsEstimate.builder()
                        .setNullsFraction(0.0)
                        .setAverageRowSize(8.0)
                        .setDistinctValuesCount(Double.MAX_VALUE - 1)
                        .build();
        Signature signature = new Signature(QualifiedObjectName.valueOf("presto.default.test"), SCALAR, VARCHAR_TYPE_10,
                createVarcharType(VarcharType.MAX_LENGTH).getTypeSignature(), createVarcharType(VarcharType.MAX_LENGTH).getTypeSignature());

        List<RowExpression> largeVarcharArguments = ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "x", createVarcharType(VarcharType.MAX_LENGTH)),
                new VariableReferenceExpression(Optional.empty(), "y", createVarcharType(VarcharType.MAX_LENGTH)));
        CallExpression callExpression = new CallExpression("test", new BuiltInFunctionHandle(signature), createUnboundedVarcharType(), largeVarcharArguments);
        ScalarFunctionStatsHeader scalarFunctionStatsHeader = new ScalarFunctionStatsHeader(
                ImmutableMap.of(1, createScalarPropagateSourceStatsInstance(false, USE_SOURCE_STATS, SUM_ARGUMENTS, SUM_ARGUMENTS, SUM_ARGUMENTS, MAX_TYPE_WIDTH_VARCHAR)),
                CONSTANT_STATS_UNKNOWN);
        VariableStatsEstimate actualStats =
                ScalarStatsAnnotationProcessor.computeStatistics(Double.MAX_VALUE - 1, callExpression, ImmutableList.of(statsEstimateLarge, statsEstimateLarge),
                        scalarFunctionStatsHeader);
        VariableStatsEstimate expectedStats = VariableStatsEstimate
                .builder()
                .setNullsFraction(0.0)
                .setDistinctValuesCount(VarcharType.MAX_LENGTH)
                .setAverageRowSize(16.0).build();
        assertEquals(actualStats, expectedStats);
    }

    @Test
    public void testComputeStatisticsTypeWidthBoundaryConditions2()
    {
        VariableStatsEstimate statsEstimateLarge =
                VariableStatsEstimate.builder()
                        .setLowValue(Double.MIN_VALUE)
                        .setHighValue(Double.MAX_VALUE)
                        .setNullsFraction(0.0)
                        .setAverageRowSize(8.0)
                        .setDistinctValuesCount(Double.MAX_VALUE - 1)
                        .build();
        Signature signature = new Signature(QualifiedObjectName.valueOf("presto.default.test"), SCALAR, VARCHAR_TYPE_10,
                DoubleType.DOUBLE.getTypeSignature(), DoubleType.DOUBLE.getTypeSignature());

        List<RowExpression> doubleArguments = ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "x", DoubleType.DOUBLE),
                new VariableReferenceExpression(Optional.empty(), "y", DoubleType.DOUBLE));
        CallExpression callExpression = new CallExpression("test", new BuiltInFunctionHandle(signature), createUnboundedVarcharType(), doubleArguments);
        ScalarFunctionStatsHeader scalarFunctionStatsHeader = new ScalarFunctionStatsHeader(
                ImmutableMap.of(1, createScalarPropagateSourceStatsInstance(false,
                        USE_MIN_ARGUMENT, SUM_ARGUMENTS, SUM_ARGUMENTS, SUM_ARGUMENTS,
                        USE_MAX_ARGUMENT)), CONSTANT_STATS_UNKNOWN);
        VariableStatsEstimate actualStats =
                ScalarStatsAnnotationProcessor.computeStatistics(Double.MAX_VALUE - 1, callExpression, ImmutableList.of(statsEstimateLarge, statsEstimateLarge),
                        scalarFunctionStatsHeader);
        VariableStatsEstimate expectedStats = VariableStatsEstimate
                .builder()
                .setLowValue(Double.MIN_VALUE)
                .setHighValue(POSITIVE_INFINITY)
                .setNullsFraction(0.0)
                .setAverageRowSize(16.0)
                .setDistinctValuesCount(Double.MAX_VALUE - 1).build();
        assertEquals(actualStats, expectedStats);
    }

    @Test
    public void testComputeStatisticsConstantStats()
    {
        Signature signature = new Signature(QualifiedObjectName.valueOf("presto.default.test"), SCALAR, VARCHAR_TYPE_10);
        CallExpression callExpression =
                new CallExpression("test", new BuiltInFunctionHandle(signature), createVarcharType(10), ImmutableList.of());
        ScalarFunctionStatsHeader scalarFunctionStatsHeader = new ScalarFunctionStatsHeader(
                ImmutableMap.of(), createScalarFunctionConstantStatsInstance(0, 1, 0.1, 8, 900));
        VariableStatsEstimate actualStats = ScalarStatsAnnotationProcessor.computeStatistics(1000, callExpression, STATS_ESTIMATE_LIST, scalarFunctionStatsHeader);
        VariableStatsEstimate expectedStats = VariableStatsEstimate.builder()
                .setLowValue(0)
                .setHighValue(1)
                .setNullsFraction(0.1)
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(900)
                .build();
        assertEquals(actualStats, expectedStats);
    }

    @Test
    public void testComputeStatisticsConstantNDVWithNullFractionFromArgumentStats()
    {
        Signature signature = new Signature(QualifiedObjectName.valueOf("presto.default.test"), SCALAR, VARCHAR_TYPE_10, VARCHAR_TYPE_10, VARCHAR_TYPE_10);
        CallExpression callExpression = new CallExpression("test", new BuiltInFunctionHandle(signature), createVarcharType(10), TWO_ARGUMENTS);
        ScalarFunctionStatsHeader scalarFunctionStatsHeader = new ScalarFunctionStatsHeader(
                ImmutableMap.of(0, createScalarPropagateSourceStatsInstance(false, UNKNOWN, UNKNOWN, SUM_ARGUMENTS, USE_SOURCE_STATS, UNKNOWN)),
                createScalarFunctionConstantStatsInstance(0, 1, NaN, NaN, 900));
        VariableStatsEstimate actualStats = ScalarStatsAnnotationProcessor.computeStatistics(1000, callExpression, STATS_ESTIMATE_LIST, scalarFunctionStatsHeader);
        VariableStatsEstimate expectedStats = VariableStatsEstimate.builder()
                .setLowValue(0)
                .setHighValue(1)
                .setNullsFraction(0.1)
                .setAverageRowSize(10)
                .setDistinctValuesCount(900)
                .build();
        assertEquals(actualStats, expectedStats);
    }

    private static ScalarFunctionConstantStats createScalarFunctionConstantStatsInstance(
            double min, double max, double nullFraction, double avgRowSize,
            double distinctValuesCount)
    {
        return new ScalarFunctionConstantStats()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return ScalarFunctionConstantStats.class;
            }

            @Override
            public double minValue()
            {
                return min;
            }

            @Override
            public double maxValue()
            {
                return max;
            }

            @Override
            public double distinctValuesCount()
            {
                return distinctValuesCount;
            }

            @Override
            public double nullFraction()
            {
                return nullFraction;
            }

            @Override
            public double avgRowSize()
            {
                return avgRowSize;
            }
        };
    }

    private ScalarPropagateSourceStats createScalarPropagateSourceStatsInstance(
            Boolean propagateAllStats,
            ScalarFunctionStatsPropagationBehavior minValue,
            ScalarFunctionStatsPropagationBehavior maxValue,
            ScalarFunctionStatsPropagationBehavior avgRowSize,
            ScalarFunctionStatsPropagationBehavior nullFraction,
            ScalarFunctionStatsPropagationBehavior distinctValuesCount)
    {
        return new ScalarPropagateSourceStats()
        {
            @Override
            public Class<? extends Annotation> annotationType()
            {
                return ScalarPropagateSourceStats.class;
            }

            @Override
            public boolean propagateAllStats()
            {
                return propagateAllStats;
            }

            @Override
            public ScalarFunctionStatsPropagationBehavior minValue()
            {
                return minValue;
            }

            @Override
            public ScalarFunctionStatsPropagationBehavior maxValue()
            {
                return maxValue;
            }

            @Override
            public ScalarFunctionStatsPropagationBehavior distinctValuesCount()
            {
                return distinctValuesCount;
            }

            @Override
            public ScalarFunctionStatsPropagationBehavior avgRowSize()
            {
                return avgRowSize;
            }

            @Override
            public ScalarFunctionStatsPropagationBehavior nullFraction()
            {
                return nullFraction;
            }
        };
    }
}
