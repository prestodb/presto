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
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.util.Map;

import static com.facebook.presto.connector.dual.DualMetadata.DUAL_METADATA_MANAGER;
import static com.facebook.presto.sql.analyzer.Type.BOOLEAN;
import static com.facebook.presto.sql.analyzer.Type.DOUBLE;
import static com.facebook.presto.sql.analyzer.Type.BIGINT;
import static com.facebook.presto.sql.analyzer.Type.VARCHAR;
import static com.facebook.presto.sql.parser.SqlParser.createExpression;
import static com.facebook.presto.tuple.Tuples.NULL_BOOLEAN_TUPLE;
import static com.facebook.presto.tuple.Tuples.NULL_DOUBLE_TUPLE;
import static com.facebook.presto.tuple.Tuples.NULL_LONG_TUPLE;
import static com.facebook.presto.tuple.Tuples.NULL_STRING_TUPLE;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static org.testng.Assert.assertEquals;

public class TestInterpretedProjectionFunction
{
    @Test
    public void testBooleanExpression()
    {
        assertProjection(BOOLEAN, "true", true);
        assertProjection(BOOLEAN, "false", false);
        assertProjection(BOOLEAN, "1 = 1", true);
        assertProjection(BOOLEAN, "1 = 0", false);
        assertProjection(BOOLEAN, "true and false", false);
    }

    @Test
    public void testArithmeticExpression()
    {
        assertProjection(BIGINT, "42 + 87", 42L + 87L);
        assertProjection(DOUBLE, "42 + 22.2", 42L + 22.2);
        assertProjection(DOUBLE, "11.1 + 22.2", 11.1 + 22.2);

        assertProjection(BIGINT, "42 - 87", 42L - 87L);
        assertProjection(DOUBLE, "42 - 22.2", 42L - 22.2);
        assertProjection(DOUBLE, "11.1 - 22.2", 11.1 - 22.2);

        assertProjection(BIGINT, "42 * 87", 42L * 87L);
        assertProjection(DOUBLE, "42 * 22.2", 42L * 22.2);
        assertProjection(DOUBLE, "11.1 * 22.2", 11.1 * 22.2);

        assertProjection(BIGINT, "42 / 87", 42L / 87L);
        assertProjection(DOUBLE, "42 / 22.2", 42L / 22.2);
        assertProjection(DOUBLE, "11.1 / 22.2", 11.1 / 22.2);

        assertProjection(BIGINT, "42 % 87", 42L % 87L);
        assertProjection(DOUBLE, "42 % 22.2", 42L % 22.2);
        assertProjection(DOUBLE, "11.1 % 22.2", 11.1 % 22.2);
    }

    @Test
    public void testArithmeticExpressionWithNulls()
    {
        for (ArithmeticExpression.Type type : ArithmeticExpression.Type.values()) {
            assertProjection(BIGINT, "NULL " + type.getValue() + " NULL", null);

            assertProjection(BIGINT, "42 " + type.getValue() + " NULL", null);
            assertProjection(BIGINT, "NULL " + type.getValue() + " 42", null);

            assertProjection(DOUBLE, "11.1 " + type.getValue() + " NULL", null);
            assertProjection(DOUBLE, "NULL " + type.getValue() + " 11.1", null);
        }
    }

    @Test
    public void testCoalesceExpression()
    {
        assertProjection(BIGINT, "COALESCE(42, 87, 100)", 42L);
        assertProjection(BIGINT, "COALESCE(NULL, 87, 100)", 87L);
        assertProjection(BIGINT, "COALESCE(42, NULL, 100)", 42L);
        assertProjection(BIGINT, "COALESCE(NULL, NULL, 100)", 100L);

        assertProjection(DOUBLE, "COALESCE(42.2, 87.2, 100.2)", 42.2);
        assertProjection(DOUBLE, "COALESCE(NULL, 87.2, 100.2)", 87.2);
        assertProjection(DOUBLE, "COALESCE(42.2, NULL, 100.2)", 42.2);
        assertProjection(DOUBLE, "COALESCE(NULL, NULL, 100.2)", 100.2);

        assertProjection(VARCHAR, "COALESCE('foo', 'bar', 'zah')", "foo");
        assertProjection(VARCHAR, "COALESCE(NULL, 'bar', 'zah')", "bar");
        assertProjection(VARCHAR, "COALESCE('foo', NULL, 'zah')", "foo");
        assertProjection(VARCHAR, "COALESCE(NULL, NULL, 'zah')", "zah");

        assertProjection(VARCHAR, "COALESCE(NULL, NULL, NULL)", null);
    }

    @Test
    public void testNullIf()
    {
        assertProjection(BIGINT, "NULLIF(42, 42)", null);
        assertProjection(BIGINT, "NULLIF(42, 42.0)", null);
        assertProjection(DOUBLE, "NULLIF(42.42, 42.42)", null);
        assertProjection(VARCHAR, "NULLIF('foo', 'foo')", null);

        assertProjection(BIGINT, "NULLIF(42, 87)", 42L);
        assertProjection(BIGINT, "NULLIF(42, 22.2)", 42L);
        assertProjection(DOUBLE, "NULLIF(42.42, 22.2)", 42.42);
        assertProjection(VARCHAR, "NULLIF('foo', 'bar')", "foo");

        assertProjection(BIGINT, "NULLIF(NULL, NULL)", null);

        assertProjection(BIGINT, "NULLIF(42, NULL)", 42L);
        assertProjection(BIGINT, "NULLIF(NULL, 42)", null);

        assertProjection(DOUBLE, "NULLIF(11.1, NULL)", 11.1);
        assertProjection(DOUBLE, "NULLIF(NULL, 11.1)", null);
    }

    @Test
    public void testSymbolReference()
    {
        assertProjection(BOOLEAN, createExpression("symbol"), true, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createTuple(true));
        assertProjection(BOOLEAN, createExpression("symbol"), null, ImmutableMap.of(new Symbol("symbol"), new Input(0)), NULL_BOOLEAN_TUPLE);

        assertProjection(BIGINT, createExpression("symbol"), 42L, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createTuple(42L));
        assertProjection(BIGINT, createExpression("symbol"), null, ImmutableMap.of(new Symbol("symbol"), new Input(0)), NULL_LONG_TUPLE);

        assertProjection(DOUBLE, createExpression("symbol"), 11.1, ImmutableMap.of(new Symbol("symbol"), new Input(0)), createTuple(11.1));
        assertProjection(DOUBLE, createExpression("symbol"), null, ImmutableMap.of(new Symbol("symbol"), new Input(0)), NULL_DOUBLE_TUPLE);

        assertProjection(VARCHAR, createExpression("symbol"), "foo", ImmutableMap.of(new Symbol("symbol"), new Input(0)), createTuple("foo"));
        assertProjection(VARCHAR, createExpression("symbol"), null, ImmutableMap.of(new Symbol("symbol"), new Input(0)), NULL_STRING_TUPLE);
    }

    public static void assertProjection(Type outputType, String expression, @Nullable Object expectedValue)
    {
        assertProjection(outputType, createExpression(expression), expectedValue, ImmutableMap.<Symbol, Input>of());
    }

    public static void assertProjection(Type outputType,
            Expression expression,
            @Nullable Object expectedValue,
            Map<Symbol, Input> symbolToInputMappings,
            TupleReadable... channels)
    {
        Builder<Input, Type> inputTypes = ImmutableMap.builder();
        for (Input input : symbolToInputMappings.values()) {
            TupleInfo.Type type = channels[input.getChannel()].getTupleInfo().getType();
            switch (type) {
                case BOOLEAN:
                    inputTypes.put(input, BOOLEAN);
                    break;
                case FIXED_INT_64:
                    inputTypes.put(input, BIGINT);
                    break;
                case VARIABLE_BINARY:
                    inputTypes.put(input, VARCHAR);
                    break;
                case DOUBLE:
                    inputTypes.put(input, DOUBLE);
                    break;
                default:
                    throw new IllegalStateException("Unsupported type");
            }
        }
        InterpretedProjectionFunction projectionFunction = new InterpretedProjectionFunction(outputType,
                expression,
                symbolToInputMappings,
                DUAL_METADATA_MANAGER,
                new Session("user", "test", Session.DEFAULT_CATALOG, Session.DEFAULT_SCHEMA, null, null)
        );

        // create output
        BlockBuilder builder = new BlockBuilder(new TupleInfo(outputType.getRawType()));

        // project
        projectionFunction.project(channels, builder);

        // extract single value
        Object actualValue = BlockAssertions.getOnlyValue(builder.build());
        assertEquals(actualValue, expectedValue);
    }
}
