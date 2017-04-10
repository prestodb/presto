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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestInterpretedProjectionFunction
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

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
        assertProjection("42 + 22.2", 42 + 22.2);
        assertProjection("11.1 + 22.2", 11.1 + 22.2);

        assertProjection("42 - 87", 42 - 87);
        assertProjection("42 - 22.2", 42 - 22.2);
        assertProjection("11.1 - 22.2", 11.1 - 22.2);

        assertProjection("42 * 87", 42 * 87);
        assertProjection("42 * 22.2", 42 * 22.2);
        assertProjection("11.1 * 22.2", 11.1 * 22.2);

        assertProjection("42 / 87", 42 / 87);
        assertProjection("42 / 22.2", 42 / 22.2);
        assertProjection("11.1 / 22.2", 11.1 / 22.2);

        assertProjection("42 % 87", 42 % 87);
        assertProjection("42 % 22.2", 42 % 22.2);
        assertProjection("11.1 % 22.2", 11.1 % 22.2);

        assertProjection("42 + BIGINT '87'", 42 + 87L);
        assertProjection("BIGINT '42' - 22.2", 42L - 22.2);
        assertProjection("42 * BIGINT '87'", 42 * 87L);
        assertProjection("BIGINT '11' / 22.2", 11L / 22.2);
        assertProjection("11.1 % BIGINT '22'", 11.1 % 22L);
    }

    @Test
    public void testArithmeticExpressionWithNulls()
    {
        for (ArithmeticBinaryExpression.Type type : ArithmeticBinaryExpression.Type.values()) {
            assertProjection("CAST(NULL AS INTEGER) " + type.getValue() + " CAST(NULL AS INTEGER)", null);

            assertProjection("42 " + type.getValue() + " NULL", null);
            assertProjection("NULL " + type.getValue() + " 42", null);

            assertProjection("11.1 " + type.getValue() + " CAST(NULL AS INTEGER)", null);
            assertProjection("CAST(NULL AS INTEGER) " + type.getValue() + " 11.1", null);
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

        assertProjection("COALESCE(42.2, 87.2, 100.2)", 42.2);
        assertProjection("COALESCE(NULL, 87.2, 100.2)", 87.2);
        assertProjection("COALESCE(42.2, NULL, 100.2)", 42.2);
        assertProjection("COALESCE(NULL, NULL, 100.2)", 100.2);

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
        assertProjection("NULLIF(42, 42.0)", null);
        assertProjection("NULLIF(42.42, 42.42)", null);
        assertProjection("NULLIF('foo', 'foo')", null);

        assertProjection("NULLIF(42, 87)", 42);
        assertProjection("NULLIF(42, 22.2)", 42);
        assertProjection("NULLIF(42, BIGINT '87')", 42);
        assertProjection("NULLIF(BIGINT '42', 22.2)", 42L);
        assertProjection("NULLIF(42.42, 22.2)", 42.42);
        assertProjection("NULLIF('foo', 'bar')", "foo");

        assertProjection("NULLIF(NULL, NULL)", null);

        assertProjection("NULLIF(42, NULL)", 42);
        assertProjection("NULLIF(NULL, 42)", null);

        assertProjection("NULLIF(11.1, NULL)", 11.1);
        assertProjection("NULLIF(NULL, 11.1)", null);
    }

    @Test
    public void testSymbolReference()
    {
        Symbol symbol = new Symbol("symbol");
        ImmutableMap<Symbol, Integer> symbolToInputMappings = ImmutableMap.of(symbol, 0);
        assertProjection("symbol", true, symbolToInputMappings, ImmutableMap.of(symbol, BOOLEAN), 0, createBlock(BOOLEAN, true));
        assertProjection("symbol", null, symbolToInputMappings, ImmutableMap.of(symbol, BOOLEAN), 0, createBlock(BOOLEAN, null));

        assertProjection("symbol", 42L, symbolToInputMappings, ImmutableMap.of(symbol, BIGINT), 0, createBlock(BIGINT, 42));
        assertProjection("symbol", null, symbolToInputMappings, ImmutableMap.of(symbol, BIGINT), 0, createBlock(BIGINT, null));

        assertProjection("symbol", 11.1, symbolToInputMappings, ImmutableMap.of(symbol, DOUBLE), 0, createBlock(DOUBLE, 11.1));
        assertProjection("symbol", null, symbolToInputMappings, ImmutableMap.of(symbol, DOUBLE), 0, createBlock(DOUBLE, null));

        assertProjection("symbol", "foo", symbolToInputMappings, ImmutableMap.of(symbol, VARCHAR), 0, createBlock(VARCHAR, "foo"));
        assertProjection("symbol", null, symbolToInputMappings, ImmutableMap.of(symbol, VARCHAR), 0, createBlock(VARCHAR, null));
    }

    public static void assertProjection(String expression, @Nullable Object expectedValue)
    {
        assertProjection(
                expression,
                expectedValue,
                ImmutableMap.of(),
                ImmutableMap.of(),
                0);
    }

    private static void assertProjection(
            String expression,
            @Nullable Object expectedValue,
            Map<Symbol, Integer> symbolToInputMappings,
            Map<Symbol, Type> symbolTypes,
            int position,
            Block... blocks)
    {
        InterpretedProjectionFunction projectionFunction = new InterpretedProjectionFunction(
                createExpression(expression, METADATA, symbolTypes),
                symbolTypes,
                symbolToInputMappings,
                METADATA,
                SQL_PARSER,
                TEST_SESSION
        );

        // create output
        Type type = projectionFunction.getType();
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), 1);

        // project
        projectionFunction.project(position, blocks, builder);

        // extract single value
        Object actualValue = BlockAssertions.getOnlyValue(type, builder.build());
        assertEquals(actualValue, expectedValue);
    }

    private static Block createBlock(Type type, Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        writeNativeValue(type, blockBuilder, value);
        return blockBuilder.build();
    }
}
