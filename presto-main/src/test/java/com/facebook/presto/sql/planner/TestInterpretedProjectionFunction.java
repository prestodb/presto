/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.Type.DOUBLE;
import static com.facebook.presto.sql.analyzer.Type.LONG;
import static com.facebook.presto.sql.analyzer.Type.STRING;
import static com.facebook.presto.sql.parser.SqlParser.createExpression;
import static com.facebook.presto.tuple.Tuples.NULL_DOUBLE_TUPLE;
import static com.facebook.presto.tuple.Tuples.NULL_LONG_TUPLE;
import static com.facebook.presto.tuple.Tuples.NULL_STRING_TUPLE;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static org.testng.Assert.assertEquals;

public class TestInterpretedProjectionFunction
{
    @Test
    public void testArithmeticExpression()
    {
        assertProjection(LONG, "42 + 87", 42L + 87L);
        assertProjection(DOUBLE, "42 + 22.2", 42L + 22.2);
        assertProjection(DOUBLE, "11.1 + 22.2", 11.1 + 22.2);

        assertProjection(LONG, "42 - 87", 42L - 87L);
        assertProjection(DOUBLE, "42 - 22.2", 42L - 22.2);
        assertProjection(DOUBLE, "11.1 - 22.2", 11.1 - 22.2);

        assertProjection(LONG, "42 * 87", 42L * 87L);
        assertProjection(DOUBLE, "42 * 22.2", 42L * 22.2);
        assertProjection(DOUBLE, "11.1 * 22.2", 11.1 * 22.2);

        assertProjection(LONG, "42 / 87", 42L / 87L);
        assertProjection(DOUBLE, "42 / 22.2", 42L / 22.2);
        assertProjection(DOUBLE, "11.1 / 22.2", 11.1 / 22.2);

        assertProjection(LONG, "42 % 87", 42L % 87L);
        assertProjection(DOUBLE, "42 % 22.2", 42L % 22.2);
        assertProjection(DOUBLE, "11.1 % 22.2", 11.1 % 22.2);
    }

    @Test
    public void testArithmeticExpressionWithNulls()
    {
        for (ArithmeticExpression.Type type : ArithmeticExpression.Type.values()) {
            assertProjection(LONG, "NULL " + type.getValue() + " NULL", null);

            assertProjection(LONG, "42 " + type.getValue() + " NULL", null);
            assertProjection(LONG, "NULL " + type.getValue() + " 42", null);

            assertProjection(DOUBLE, "11.1 " + type.getValue() + " NULL", null);
            assertProjection(DOUBLE, "NULL " + type.getValue() + " 11.1", null);
        }
    }

    @Test
    public void testCoalesceExpression()
    {
        assertProjection(LONG, "COALESCE(42, 87, 100)", 42L);
        assertProjection(LONG, "COALESCE(NULL, 87, 100)", 87L);
        assertProjection(LONG, "COALESCE(42, NULL, 100)", 42L);
        assertProjection(LONG, "COALESCE(NULL, NULL, 100)", 100L);

        assertProjection(DOUBLE, "COALESCE(42.2, 87.2, 100.2)", 42.2);
        assertProjection(DOUBLE, "COALESCE(NULL, 87.2, 100.2)", 87.2);
        assertProjection(DOUBLE, "COALESCE(42.2, NULL, 100.2)", 42.2);
        assertProjection(DOUBLE, "COALESCE(NULL, NULL, 100.2)", 100.2);

        assertProjection(STRING, "COALESCE('foo', 'bar', 'zah')", "foo");
        assertProjection(STRING, "COALESCE(NULL, 'bar', 'zah')", "bar");
        assertProjection(STRING, "COALESCE('foo', NULL, 'zah')", "foo");
        assertProjection(STRING, "COALESCE(NULL, NULL, 'zah')", "zah");

        assertProjection(STRING, "COALESCE(NULL, NULL, NULL)", null);
    }

    @Test
    public void testNullIf()
    {
        assertProjection(LONG, "NULLIF(42, 42)", null);
        assertProjection(LONG, "NULLIF(42, 42.0)", null);
        assertProjection(DOUBLE, "NULLIF(42.42, 42.42)", null);
        assertProjection(STRING, "NULLIF('foo', 'foo')", null);

        assertProjection(LONG, "NULLIF(42, 87)", 42L);
        assertProjection(LONG, "NULLIF(42, 22.2)", 42L);
        assertProjection(DOUBLE, "NULLIF(42.42, 22.2)", 42.42);
        assertProjection(STRING, "NULLIF('foo', 'bar')", "foo");

        assertProjection(LONG, "NULLIF(NULL, NULL)", null);

        assertProjection(LONG, "NULLIF(42, NULL)", null);
        assertProjection(LONG, "NULLIF(NULL, 42)", null);

        assertProjection(DOUBLE, "NULLIF(11.1, NULL)", null);
        assertProjection(DOUBLE, "NULLIF(NULL, 11.1)", null);
    }

    @Test
    public void testSymbolReference()
    {
        assertProjection(LONG, createExpression("symbol"), 42L, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), createTuple(42L));
        assertProjection(LONG, createExpression("symbol"), null, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), NULL_LONG_TUPLE);

        assertProjection(DOUBLE, createExpression("symbol"), 11.1, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), createTuple(11.1));
        assertProjection(DOUBLE, createExpression("symbol"), null, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), NULL_DOUBLE_TUPLE);

        assertProjection(STRING, createExpression("symbol"), "foo", ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), createTuple("foo"));
        assertProjection(STRING, createExpression("symbol"), null, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), NULL_STRING_TUPLE);
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
        InterpretedProjectionFunction projectionFunction = new InterpretedProjectionFunction(outputType,
                expression,
                symbolToInputMappings,
                new MetadataManager(),
                new Session(null, Session.DEFAULT_CATALOG, Session.DEFAULT_SCHEMA));

        // create output
        BlockBuilder builder = new BlockBuilder(new TupleInfo(outputType.getRawType()));

        // project
        projectionFunction.project(channels, builder);

        // extract single value
        Object actualValue = Iterables.getOnlyElement(Iterables.concat(BlockAssertions.toValues(builder.build())));
        assertEquals(actualValue, expectedValue);
    }
}
