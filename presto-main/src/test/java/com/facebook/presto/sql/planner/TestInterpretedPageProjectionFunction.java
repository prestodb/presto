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
package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.InterpretedPageProjection;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.project.SelectedPositions.positionsList;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestInterpretedPageProjectionFunction
{
    // todo add cases for decimal

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-%s"));

    @Test
    public void testBooleanExpression()
    {
        assertProjection("true", true);
        assertProjection("false", false);
        assertProjection("1 = 1", true);
        assertProjection("1 = 0", false);
        assertProjection("true and false", false);
    }

    @Test
    public void testArithmeticExpression()
    {
        assertProjection("42 + 87", 42 + 87);
        assertProjection("42 + 22.2E0", 42 + 22.2);
        assertProjection("11.1E0 + 22.2E0", 11.1 + 22.2);

        assertProjection("42 - 87", 42 - 87);
        assertProjection("42 - 22.2E0", 42 - 22.2);
        assertProjection("11.1E0 - 22.2E0", 11.1 - 22.2);

        assertProjection("42 * 87", 42 * 87);
        assertProjection("42 * 22.2E0", 42 * 22.2);
        assertProjection("11.1E0 * 22.2E0", 11.1 * 22.2);

        assertProjection("42 / 87", 42 / 87);
        assertProjection("42 / 22.2E0", 42 / 22.2);
        assertProjection("11.1E0 / 22.2E0", 11.1 / 22.2);

        assertProjection("42 % 87", 42 % 87);
        assertProjection("42 % 22.2E0", 42 % 22.2);
        assertProjection("11.1E0 % 22.2E0", 11.1 % 22.2);

        assertProjection("42 + BIGINT '87'", 42 + 87L);
        assertProjection("BIGINT '42' - 22.2E0", 42L - 22.2);
        assertProjection("42 * BIGINT '87'", 42 * 87L);
        assertProjection("BIGINT '11' / 22.2E0", 11L / 22.2);
        assertProjection("11.1E0 % BIGINT '22'", 11.1 % 22L);
    }

    @Test
    public void testArithmeticExpressionWithNulls()
    {
        for (ArithmeticBinaryExpression.Operator operator : ArithmeticBinaryExpression.Operator.values()) {
            assertProjection("CAST(NULL AS INTEGER) " + operator.getValue() + " CAST(NULL AS INTEGER)", null);

            assertProjection("42 " + operator.getValue() + " NULL", null);
            assertProjection("NULL " + operator.getValue() + " 42", null);

            assertProjection("11.1 " + operator.getValue() + " CAST(NULL AS INTEGER)", null);
            assertProjection("CAST(NULL AS INTEGER) " + operator.getValue() + " 11.1", null);
        }
    }

    @Test
    public void testCoalesceExpression()
    {
        assertProjection("COALESCE(42, 87, 100)", 42);
        assertProjection("COALESCE(NULL, 87, 100)", 87);
        assertProjection("COALESCE(42, NULL, 100)", 42);
        assertProjection("COALESCE(42, NULL, BIGINT '100')", 42L);
        assertProjection("COALESCE(NULL, NULL, 100)", 100);
        assertProjection("COALESCE(NULL, NULL, BIGINT '100')", 100L);

        assertProjection("COALESCE(42.2E0, 87.2E0, 100.2E0)", 42.2);
        assertProjection("COALESCE(NULL, 87.2E0, 100.2E0)", 87.2);
        assertProjection("COALESCE(42.2E0, NULL, 100.2E0)", 42.2);
        assertProjection("COALESCE(NULL, NULL, 100.2E0)", 100.2);

        assertProjection("COALESCE('foo', 'bar', 'zah')", "foo");
        assertProjection("COALESCE(NULL, 'bar', 'zah')", "bar");
        assertProjection("COALESCE('foo', NULL, 'zah')", "foo");
        assertProjection("COALESCE(NULL, NULL, 'zah')", "zah");

        assertProjection("COALESCE(NULL, NULL, NULL)", null);
    }

    @Test
    public void testNullIf()
    {
        assertProjection("NULLIF(42, 42)", null);
        assertProjection("NULLIF(42, 42.0E0)", null);
        assertProjection("NULLIF(42.42E0, 42.42E0)", null);
        assertProjection("NULLIF('foo', 'foo')", null);

        assertProjection("NULLIF(42, 87)", 42);
        assertProjection("NULLIF(42, 22.2E0)", 42);
        assertProjection("NULLIF(42, BIGINT '87')", 42);
        assertProjection("NULLIF(BIGINT '42', 22.2E0)", 42L);
        assertProjection("NULLIF(42.42E0, 22.2E0)", 42.42);
        assertProjection("NULLIF('foo', 'bar')", "foo");

        assertProjection("NULLIF(NULL, NULL)", null);

        assertProjection("NULLIF(42, NULL)", 42);
        assertProjection("NULLIF(NULL, 42)", null);

        assertProjection("NULLIF(11.1E0, NULL)", 11.1);
        assertProjection("NULLIF(NULL, 11.1E0)", null);
    }

    @Test
    public void testSymbolReference()
    {
        Symbol symbol = new Symbol("symbol");
        ImmutableMap<Symbol, Integer> symbolToInputMappings = ImmutableMap.of(symbol, 0);
        assertProjection("symbol", true, symbolToInputMappings, TypeProvider.copyOf(ImmutableMap.of(symbol, BOOLEAN)), 0, createBlock(BOOLEAN, true));
        assertProjection("symbol", null, symbolToInputMappings, TypeProvider.copyOf(ImmutableMap.of(symbol, BOOLEAN)), 0, createNullBlock(BOOLEAN));

        assertProjection("symbol", 42L, symbolToInputMappings, TypeProvider.copyOf(ImmutableMap.of(symbol, BIGINT)), 0, createBlock(BIGINT, 42));
        assertProjection("symbol", null, symbolToInputMappings, TypeProvider.copyOf(ImmutableMap.of(symbol, BIGINT)), 0, createNullBlock(BIGINT));

        assertProjection("symbol", 11.1, symbolToInputMappings, TypeProvider.copyOf(ImmutableMap.of(symbol, DOUBLE)), 0, createBlock(DOUBLE, 11.1));
        assertProjection("symbol", null, symbolToInputMappings, TypeProvider.copyOf(ImmutableMap.of(symbol, DOUBLE)), 0, createNullBlock(DOUBLE));

        assertProjection("symbol", "foo", symbolToInputMappings, TypeProvider.copyOf(ImmutableMap.of(symbol, VARCHAR)), 0, createBlock(VARCHAR, "foo"));
        assertProjection("symbol", null, symbolToInputMappings, TypeProvider.copyOf(ImmutableMap.of(symbol, VARCHAR)), 0, createNullBlock(VARCHAR));
    }

    private static void assertProjection(String expression, @Nullable Object expectedValue)
    {
        assertProjection(
                expression,
                expectedValue,
                ImmutableMap.of(),
                TypeProvider.empty(),
                0);
    }

    private static void assertProjection(
            String expression,
            @Nullable Object expectedValue,
            Map<Symbol, Integer> symbolToInputMappings,
            TypeProvider symbolTypes,
            int position,
            Block... blocks)
    {
        assertProjection(expression, new Object[] {expectedValue}, symbolToInputMappings, symbolTypes, new int[] {position}, blocks);
    }

    private static void assertProjection(
            String expression,
            Object[] expectedValues,
            Map<Symbol, Integer> symbolToInputMappings,
            TypeProvider symbolTypes,
            int[] positions,
            Block... blocks)
    {
        InterpretedPageProjection projectionFunction = new InterpretedPageProjection(
                createExpression(expression, METADATA, symbolTypes),
                symbolTypes,
                symbolToInputMappings,
                METADATA,
                SQL_PARSER,
                TEST_SESSION);

        // project with yield
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        Work<Block> work = projectionFunction.project(
                TEST_SESSION.toConnectorSession(),
                yieldSignal,
                new Page(positions.length, blocks),
                positionsList(positions, 0, positions.length));

        Block block;
        // Get nothing for the first position.length compute due to yield
        // Currently we enforce a yield check for every position; free feel to adjust the number if the behavior changes
        for (int i = 0; i < positions.length; i++) {
            yieldSignal.setWithDelay(1, executor);
            yieldSignal.forceYieldForTesting();
            assertFalse(work.process());
            yieldSignal.reset();
        }
        // the next yield is not going to prevent a block to be produced
        yieldSignal.setWithDelay(1, executor);
        yieldSignal.forceYieldForTesting();
        yieldSignal.reset();
        assertTrue(work.process());
        block = work.getResult();

        List<Object> actualValues = BlockAssertions.toValues(projectionFunction.getType(), block);
        assertEquals(actualValues.size(), positions.length);
        assertEquals(expectedValues.length, positions.length);
        for (int i = 0; i < positions.length; i++) {
            assertEquals(actualValues.get(i), expectedValues[i]);
        }

        // project without yield
        work = projectionFunction.project(
                TEST_SESSION.toConnectorSession(),
                new DriverYieldSignal(),
                new Page(positions.length, blocks),
                positionsList(positions, 0, positions.length));
        assertTrue(work.process());
        block = work.getResult();

        actualValues = BlockAssertions.toValues(projectionFunction.getType(), block);
        assertEquals(actualValues.size(), positions.length);
        assertEquals(expectedValues.length, positions.length);
        for (int i = 0; i < positions.length; i++) {
            assertEquals(actualValues.get(i), expectedValues[i]);
        }
    }

    private static Block createBlock(Type type, Object value)
    {
        return createBlock(type, new Object[] {value});
    }

    private static Block createNullBlock(Type type)
    {
        return createBlock(type, new Object[] {null});
    }

    private static Block createBlock(Type type, Object[] values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.length);
        for (Object value : values) {
            writeNativeValue(type, blockBuilder, value);
        }
        return blockBuilder.build();
    }
}
