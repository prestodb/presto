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
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockBuilderStatus;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.type.Type;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.connector.dual.DualMetadata.DUAL_METADATA_MANAGER;
import static com.facebook.presto.operator.scalar.FunctionAssertions.createExpression;
import static com.facebook.presto.type.BigintType.BIGINT;
import static com.facebook.presto.type.BooleanType.BOOLEAN;
import static com.facebook.presto.type.DoubleType.DOUBLE;
import static com.facebook.presto.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestInterpretedProjectionFunction
{
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
        assertProjection("42 + 87", 42L + 87L);
        assertProjection("42 + 22.2", 42L + 22.2);
        assertProjection("11.1 + 22.2", 11.1 + 22.2);

        assertProjection("42 - 87", 42L - 87L);
        assertProjection("42 - 22.2", 42L - 22.2);
        assertProjection("11.1 - 22.2", 11.1 - 22.2);

        assertProjection("42 * 87", 42L * 87L);
        assertProjection("42 * 22.2", 42L * 22.2);
        assertProjection("11.1 * 22.2", 11.1 * 22.2);

        assertProjection("42 / 87", 42L / 87L);
        assertProjection("42 / 22.2", 42L / 22.2);
        assertProjection("11.1 / 22.2", 11.1 / 22.2);

        assertProjection("42 % 87", 42L % 87L);
        assertProjection("42 % 22.2", 42L % 22.2);
        assertProjection("11.1 % 22.2", 11.1 % 22.2);
    }

    @Test
    public void testArithmeticExpressionWithNulls()
    {
        for (ArithmeticExpression.Type type : ArithmeticExpression.Type.values()) {
            assertProjection("NULL " + type.getValue() + " NULL", null);

            assertProjection("42 " + type.getValue() + " NULL", null);
            assertProjection("NULL " + type.getValue() + " 42", null);

            assertProjection("11.1 " + type.getValue() + " NULL", null);
            assertProjection("NULL " + type.getValue() + " 11.1", null);
        }
    }

    @Test
    public void testCoalesceExpression()
    {
        assertProjection("COALESCE(42, 87, 100)", 42L);
        assertProjection("COALESCE(NULL, 87, 100)", 87L);
        assertProjection("COALESCE(42, NULL, 100)", 42L);
        assertProjection("COALESCE(NULL, NULL, 100)", 100L);

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

        assertProjection("NULLIF(42, 87)", 42L);
        assertProjection("NULLIF(42, 22.2)", 42.0);
        assertProjection("NULLIF(42.42, 22.2)", 42.42);
        assertProjection("NULLIF('foo', 'bar')", "foo");

        assertProjection("NULLIF(NULL, NULL)", null);

        assertProjection("NULLIF(42, NULL)", 42L);
        assertProjection("NULLIF(NULL, 42)", null);

        assertProjection("NULLIF(11.1, NULL)", 11.1);
        assertProjection("NULLIF(NULL, 11.1)", null);
    }

    @Test
    public void testSymbolReference()
    {
        assertProjection("symbol", true, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createCursor(BOOLEAN, true));
        assertProjection("symbol", null, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createCursor(BOOLEAN, null));

        assertProjection("symbol", 42L, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createCursor(BIGINT, 42));
        assertProjection("symbol", null, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createCursor(BIGINT, null));

        assertProjection("symbol", 11.1, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createCursor(DOUBLE, 11.1));
        assertProjection("symbol", null, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createCursor(DOUBLE, null));

        assertProjection("symbol", "foo", ImmutableMap.of(new Symbol("symbol"), new Input(0)), createCursor(VARCHAR, "foo"));
        assertProjection("symbol", null, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createCursor(VARCHAR, null));
    }

    public static void assertProjection(String expression, @Nullable Object expectedValue)
    {
        assertProjection(expression, expectedValue, ImmutableMap.<Symbol, Input>of());
    }

    private static void assertProjection(
            String expression,
            @Nullable Object expectedValue,
            Map<Symbol, Input> symbolToInputMappings,
            BlockCursor... channels)
    {
        ImmutableMap.Builder<Symbol, Type> symbolTypes = ImmutableMap.builder();
        for (Entry<Symbol, Input> entry : symbolToInputMappings.entrySet()) {
            symbolTypes.put(entry.getKey(), channels[entry.getValue().getChannel()].getType());
        }

        InterpretedProjectionFunction projectionFunction = new InterpretedProjectionFunction(
                createExpression(expression, DUAL_METADATA_MANAGER, symbolTypes.build()),
                symbolTypes.build(),
                symbolToInputMappings,
                DUAL_METADATA_MANAGER,
                new Session("user", "test", Session.DEFAULT_CATALOG, Session.DEFAULT_SCHEMA, null, null)
        );

        // create output
        BlockBuilder builder = projectionFunction.getType().createBlockBuilder(new BlockBuilderStatus());

        // project
        projectionFunction.project(channels, builder);

        // extract single value
        Object actualValue = BlockAssertions.getOnlyValue(builder.build());
        assertEquals(actualValue, expectedValue);
    }

    private static BlockCursor createCursor(Type type, Object value)
    {
        BlockCursor cursor = type.createBlockBuilder(new BlockBuilderStatus()).appendObject(value).build().cursor();
        assertTrue(cursor.advanceNextPosition());
        return cursor;
    }
}
