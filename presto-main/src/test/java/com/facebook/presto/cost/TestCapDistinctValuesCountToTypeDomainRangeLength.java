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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.cost.SymbolStatsAssertion.assertThat;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static java.util.Collections.emptyList;

public class TestCapDistinctValuesCountToTypeDomainRangeLength
{
    private final TypeManager typeManager = new TypeRegistry();
    private final FunctionRegistry functionRegistry = new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
    private final ConnectorSession session = new TestingConnectorSession(emptyList());

    @Test
    public void test()
    {
        Symbol bool = new Symbol("bool");
        Symbol bool2 = new Symbol("bool2");
        Symbol tinyint = new Symbol("tinyint");
        Symbol smallint = new Symbol("smallint");
        Symbol integer = new Symbol("integer");
        Symbol integer2 = new Symbol("integer2");
        Symbol integer3 = new Symbol("integer3");
        Symbol bigint = new Symbol("bigint");
        Symbol decimal = new Symbol("decimal");
        Symbol decimal2 = new Symbol("decimal2");
        Symbol decimal3 = new Symbol("decimal3");
        Symbol double1 = new Symbol("double1");
        Symbol double2 = new Symbol("double2");

        DecimalType decimalType = DecimalType.createDecimalType(10, 2);
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(bool, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(true, BOOLEAN))
                        .setHighValue(asStatsValue(true, BOOLEAN))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(bool2, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(false, BOOLEAN))
                        .setHighValue(asStatsValue(true, BOOLEAN))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(tinyint, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(1, TINYINT))
                        .setHighValue(asStatsValue(5, TINYINT))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(smallint, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(1, SMALLINT))
                        .setHighValue(asStatsValue(5, SMALLINT))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(integer, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(1, INTEGER))
                        .setHighValue(asStatsValue(5, INTEGER))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(integer2, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(1, INTEGER))
                        .setHighValue(asStatsValue(5, INTEGER))
                        .setDistinctValuesCount(3)
                        .build())
                .addSymbolStatistics(integer3, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(1, INTEGER))
                        .setHighValue(asStatsValue(5, INTEGER))
                        .build())
                .addSymbolStatistics(bigint, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(1, BIGINT))
                        .setHighValue(asStatsValue(5, BIGINT))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(decimal, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(1, decimalType))
                        .setHighValue(asStatsValue(1, decimalType))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(decimal2, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(101, decimalType))
                        .setHighValue(asStatsValue(103, decimalType))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(decimal3, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(100, decimalType))
                        .setHighValue(asStatsValue(200, decimalType))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(double1, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(10.1, DOUBLE))
                        .setHighValue(asStatsValue(10.2, DOUBLE))
                        .setDistinctValuesCount(10)
                        .build())
                .addSymbolStatistics(double2, SymbolStatsEstimate.builder()
                        .setLowValue(asStatsValue(10.0, DOUBLE))
                        .setHighValue(asStatsValue(10.0, DOUBLE))
                        .setDistinctValuesCount(10)
                        .build())
                .build();

        Map<Symbol, Type> types = ImmutableMap.<Symbol, Type>builder()
                .put(bool, BOOLEAN)
                .put(bool2, BOOLEAN)
                .put(tinyint, TINYINT)
                .put(smallint, SMALLINT)
                .put(integer, INTEGER)
                .put(integer2, INTEGER)
                .put(integer3, INTEGER)
                .put(bigint, BIGINT)
                .put(decimal, decimalType)
                .put(decimal2, decimalType)
                .put(decimal3, decimalType)
                .put(double1, DOUBLE)
                .put(double2, DOUBLE)
                .build();

        ComposableStatsCalculator.Normalizer normalizer = new CapDistinctValuesCountToTypeDomainRangeLength();
        PlanNodeStatsEstimate normalized = normalizer.normalize(null, estimate, types);

        assertThat(normalized.getSymbolStatistics(bool)).distinctValuesCount(1);
        assertThat(normalized.getSymbolStatistics(bool2)).distinctValuesCount(2);
        assertThat(normalized.getSymbolStatistics(tinyint)).distinctValuesCount(5);
        assertThat(normalized.getSymbolStatistics(smallint)).distinctValuesCount(5);
        assertThat(normalized.getSymbolStatistics(smallint)).distinctValuesCount(5);
        assertThat(normalized.getSymbolStatistics(integer)).distinctValuesCount(5);
        assertThat(normalized.getSymbolStatistics(integer2)).distinctValuesCount(3);
        assertThat(normalized.getSymbolStatistics(integer3)).distinctValuesCountUnknown();
        assertThat(normalized.getSymbolStatistics(bigint)).distinctValuesCount(5);
        assertThat(normalized.getSymbolStatistics(decimal)).distinctValuesCount(1);
        assertThat(normalized.getSymbolStatistics(decimal2)).distinctValuesCount(3);
        assertThat(normalized.getSymbolStatistics(decimal3)).distinctValuesCount(10);
        assertThat(normalized.getSymbolStatistics(double1)).distinctValuesCount(10);
        assertThat(normalized.getSymbolStatistics(double2)).distinctValuesCount(1);
    }

    private double asStatsValue(Object value, Type type)
    {
        return new DomainConverter(type, functionRegistry, session).translateToDouble(value).orElse(Double.NaN);
    }
}
