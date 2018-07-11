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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.time.LocalDate;

import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.cost.SymbolStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.Double.NaN;
import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;

public class TestStatsNormalizer
{
    private final TypeManager typeManager = new TypeRegistry();
    private final FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
    private final ConnectorSession session = new TestingConnectorSession(emptyList());

    private final StatsNormalizer normalizer = new StatsNormalizer();

    @Test
    public void testNoCapping()
    {
        Symbol a = new Symbol("a");
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(a, SymbolStatsEstimate.builder().setDistinctValuesCount(20).build())
                .build();

        assertNormalized(estimate)
                .symbolStats(a, symbolAssert -> symbolAssert.distinctValuesCount(20));
    }

    @Test
    public void testDropNonOutputSymbols()
    {
        Symbol a = new Symbol("a");
        Symbol b = new Symbol("b");
        Symbol c = new Symbol("c");
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(a, SymbolStatsEstimate.builder().setDistinctValuesCount(20).build())
                .addSymbolStatistics(b, SymbolStatsEstimate.builder().setDistinctValuesCount(30).build())
                .addSymbolStatistics(c, SymbolStatsEstimate.buildFrom(UNKNOWN_STATS).build())
                .build();

        PlanNodeStatsAssertion.assertThat(normalizer.normalize(estimate, ImmutableList.of(b, c), TypeProvider.copyOf(ImmutableMap.of(b, BIGINT, c, BIGINT))))
                .symbolsWithKnownStats(b)
                .symbolStats(b, symbolAssert -> symbolAssert.distinctValuesCount(30));
    }

    @Test
    public void tesCapDistinctValuesByOutputRowCount()
    {
        Symbol a = new Symbol("a");
        Symbol b = new Symbol("b");
        Symbol c = new Symbol("c");
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(a, SymbolStatsEstimate.builder().setNullsFraction(0).setDistinctValuesCount(20).build())
                .addSymbolStatistics(b, SymbolStatsEstimate.builder().setNullsFraction(0.4).setDistinctValuesCount(20).build())
                .addSymbolStatistics(c, SymbolStatsEstimate.builder().build())
                .setOutputRowCount(10)
                .build();

        assertNormalized(estimate)
                .symbolStats(a, symbolAssert -> symbolAssert.distinctValuesCount(10))
                .symbolStats(b, symbolAssert -> symbolAssert.distinctValuesCount(8))
                .symbolStats(c, SymbolStatsAssertion::distinctValuesCountUnknown);
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
        Symbol symbol = new Symbol("x");
        SymbolStatsEstimate symbolStats = SymbolStatsEstimate.builder()
                .setNullsFraction(0)
                .setDistinctValuesCount(ndv)
                .setLowValue(asStatsValue(low, type))
                .setHighValue(asStatsValue(high, type))
                .build();
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder().addSymbolStatistics(symbol, symbolStats).build();

        assertNormalized(estimate, TypeProvider.copyOf(ImmutableMap.of(symbol, type)))
                .symbolStats(symbol, symbolAssert -> symbolAssert.distinctValuesCount(expectedNormalizedNdv));
    }

    private PlanNodeStatsAssertion assertNormalized(PlanNodeStatsEstimate estimate)
    {
        TypeProvider types = TypeProvider.copyOf(estimate.getSymbolsWithKnownStatistics().stream()
                .collect(toImmutableMap(identity(), symbol -> BIGINT)));
        return assertNormalized(estimate, types);
    }

    private PlanNodeStatsAssertion assertNormalized(PlanNodeStatsEstimate estimate, TypeProvider types)
    {
        PlanNodeStatsEstimate normalized = normalizer.normalize(estimate, estimate.getSymbolsWithKnownStatistics(), types);
        return PlanNodeStatsAssertion.assertThat(normalized);
    }

    private double asStatsValue(Object value, Type type)
    {
        return toStatsRepresentation(functionRegistry, session, type, value).orElse(NaN);
    }
}
