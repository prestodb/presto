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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.time.LocalDate;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static java.lang.Double.NaN;
import static java.util.Collections.emptyList;

public class TestStatsNormalizer
{
    private final FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
    private final ConnectorSession session = new TestingConnectorSession(emptyList());

    private final StatsNormalizer normalizer = new StatsNormalizer();

    @Test
    public void testNoCapping()
    {
        VariableReferenceExpression a = new VariableReferenceExpression("a", BIGINT);
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(30)
                .setTotalSize(120)
                .addVariableStatistics(a, VariableStatsEstimate.builder().setDistinctValuesCount(20).build())
                .build();

        assertNormalized(estimate)
                .totalSize(120)
                .variableStats(a, variableAssert -> variableAssert.distinctValuesCount(20));
    }

    @Test
    public void testDropNonOutputSymbols()
    {
        VariableReferenceExpression a = new VariableReferenceExpression("a", BIGINT);
        VariableReferenceExpression b = new VariableReferenceExpression("b", BIGINT);
        VariableReferenceExpression c = new VariableReferenceExpression("c", BIGINT);
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(40)
                .setTotalSize(160)
                .addVariableStatistics(a, VariableStatsEstimate.builder().setDistinctValuesCount(20).build())
                .addVariableStatistics(b, VariableStatsEstimate.builder().setDistinctValuesCount(30).build())
                .addVariableStatistics(c, VariableStatsEstimate.unknown())
                .build();

        PlanNodeStatsAssertion.assertThat(normalizer.normalize(estimate, ImmutableList.of(b, c)))
                .totalSize(160)
                .variablesWithKnownStats(b)
                .variableStats(b, variableAssert -> variableAssert.distinctValuesCount(30));
    }

    @Test
    public void tesCapDistinctValuesByOutputRowCount()
    {
        VariableReferenceExpression a = new VariableReferenceExpression("a", BIGINT);
        VariableReferenceExpression b = new VariableReferenceExpression("b", BIGINT);
        VariableReferenceExpression c = new VariableReferenceExpression("c", BIGINT);
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .addVariableStatistics(a, VariableStatsEstimate.builder().setNullsFraction(0).setDistinctValuesCount(20).build())
                .addVariableStatistics(b, VariableStatsEstimate.builder().setNullsFraction(0.4).setDistinctValuesCount(20).build())
                .addVariableStatistics(c, VariableStatsEstimate.unknown())
                .setOutputRowCount(10)
                .setTotalSize(40)
                .build();

        assertNormalized(estimate)
                .totalSize(40)
                .variableStats(a, variableAssert -> variableAssert.distinctValuesCount(10))
                .variableStats(b, variableAssert -> variableAssert.distinctValuesCount(8))
                .variableStats(c, VariableStatsAssertion::distinctValuesCountUnknown);
    }

    @Test
    public void testCapDistinctValuesByToDomainRangeLength()
    {
        testCapDistinctValuesByToDomainRangeLength(INTEGER, 15, 1, 5, 5);
        testCapDistinctValuesByToDomainRangeLength(INTEGER, 2_0000_000_000., 1, 1_000_000_000, 1_000_000_000);
        testCapDistinctValuesByToDomainRangeLength(INTEGER, 3, 1, 5, 3);
        testCapDistinctValuesByToDomainRangeLength(INTEGER, NaN, 1, 5, NaN);

        testCapDistinctValuesByToDomainRangeLength(BIGINT, 15, 1, 5, 5);
        testCapDistinctValuesByToDomainRangeLength(SMALLINT, 15, 1, 5, 5);
        testCapDistinctValuesByToDomainRangeLength(TINYINT, 15, 1, 5, 5);

        testCapDistinctValuesByToDomainRangeLength(createDecimalType(10, 2), 11, 1, 1, 1);
        testCapDistinctValuesByToDomainRangeLength(createDecimalType(10, 2), 13, 101, 103, 3);
        testCapDistinctValuesByToDomainRangeLength(createDecimalType(10, 2), 10, 100, 200, 10);

        testCapDistinctValuesByToDomainRangeLength(DOUBLE, 42, 10.1, 10.2, 42);
        testCapDistinctValuesByToDomainRangeLength(DOUBLE, 42, 10.1, 10.1, 1);

        testCapDistinctValuesByToDomainRangeLength(BOOLEAN, 11, true, true, 1);
        testCapDistinctValuesByToDomainRangeLength(BOOLEAN, 12, false, true, 2);

        testCapDistinctValuesByToDomainRangeLength(
                DATE,
                12,
                LocalDate.of(2017, 8, 31).toEpochDay(),
                LocalDate.of(2017, 9, 2).toEpochDay(),
                3);
    }

    private void testCapDistinctValuesByToDomainRangeLength(Type type, double ndv, Object low, Object high, double expectedNormalizedNdv)
    {
        VariableReferenceExpression variable = new VariableReferenceExpression("x", type);
        VariableStatsEstimate symbolStats = VariableStatsEstimate.builder()
                .setNullsFraction(0)
                .setDistinctValuesCount(ndv)
                .setLowValue(asStatsValue(low, type))
                .setHighValue(asStatsValue(high, type))
                .build();
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10000000000L)
                .setTotalSize(40000000000L)
                .addVariableStatistics(variable, symbolStats).build();

        assertNormalized(estimate)
                .totalSize(40000000000L)
                .variableStats(variable, variableAssert -> variableAssert.distinctValuesCount(expectedNormalizedNdv));
    }

    private PlanNodeStatsAssertion assertNormalized(PlanNodeStatsEstimate estimate)
    {
        PlanNodeStatsEstimate normalized = normalizer.normalize(estimate, estimate.getVariablesWithKnownStatistics());
        return PlanNodeStatsAssertion.assertThat(normalized);
    }

    private double asStatsValue(Object value, Type type)
    {
        return toStatsRepresentation(functionAndTypeManager, session, type, value).orElse(NaN);
    }
}
