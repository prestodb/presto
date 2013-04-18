/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.NameToSymbolRewriter;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
        assertProjection(LONG, toExpression("symbol", LONG), 42L, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), ImmutableMap.of(new Symbol("symbol"), LONG), createTuple(42L));
        assertProjection(LONG, toExpression("symbol", LONG), null, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), ImmutableMap.of(new Symbol("symbol"), LONG), NULL_LONG_TUPLE);

        assertProjection(DOUBLE, toExpression("symbol", DOUBLE), 11.1, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), ImmutableMap.of(new Symbol("symbol"), DOUBLE), createTuple(11.1));
        assertProjection(DOUBLE, toExpression("symbol", DOUBLE), null, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), ImmutableMap.of(new Symbol("symbol"), DOUBLE), NULL_DOUBLE_TUPLE);

        assertProjection(STRING, toExpression("symbol", STRING), "foo", ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), ImmutableMap.of(new Symbol("symbol"), STRING), createTuple("foo"));
        assertProjection(STRING, toExpression("symbol", STRING), null, ImmutableMap.of(new Symbol("symbol"), new Input(0, 0)), ImmutableMap.of(new Symbol("symbol"), STRING), NULL_STRING_TUPLE);
    }

    public static void assertProjection(Type outputType, String expression, @Nullable Object expectedValue)
    {
        assertProjection(outputType, toExpression(expression), expectedValue, ImmutableMap.<Symbol, Input>of(), ImmutableMap.<Symbol, Type>of());
    }

    public static void assertProjection(Type outputType, Expression expression, @Nullable Object expectedValue, Map<Symbol, Input> symbolToInputMappings, Map<Symbol, Type> types, TupleReadable... channels)
    {
        InterpretedProjectionFunction projectionFunction = new InterpretedProjectionFunction(outputType,
                expression,
                symbolToInputMappings,
                new TestingMetadata(),
                new Session(null, Session.DEFAULT_CATALOG, Session.DEFAULT_SCHEMA));

        // create output
        BlockBuilder builder = new BlockBuilder(new TupleInfo(outputType.getRawType()));

        // project
        projectionFunction.project(channels, builder);

        // extract single value
        Object actualValue = Iterables.getOnlyElement(Iterables.concat(BlockAssertions.toValues(builder.build())));
        assertEquals(actualValue, expectedValue);
    }

    public static Expression toExpression(String expression)
    {
        return toExpression(expression, DOUBLE);
    }

    public static Expression toExpression(String expression, Type type)
    {
        Expression raw = createExpression(expression);

        Optional<QualifiedName> prefix = Optional.of(QualifiedName.of("T"));
        TupleDescriptor descriptor = new TupleDescriptor(ImmutableList.of(
                new Field(prefix, Optional.of("symbol"), Optional.<ColumnHandle>absent(), new Symbol("symbol"), type)));

        return TreeRewriter.rewriteWith(new NameToSymbolRewriter(descriptor), raw);
    }
}
