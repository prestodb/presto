/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.sql.compiler.NameToSlotRewriter;
import com.facebook.presto.sql.compiler.NamedSlot;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.TupleDescriptor;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.parser.CaseInsensitiveStream;
import com.facebook.presto.sql.parser.StatementBuilder;
import com.facebook.presto.sql.parser.StatementLexer;
import com.facebook.presto.sql.parser.StatementParser;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.BufferedTreeNodeStream;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.Map;

import static com.facebook.presto.sql.compiler.Type.DOUBLE;
import static com.facebook.presto.sql.compiler.Type.LONG;
import static com.facebook.presto.sql.compiler.Type.STRING;
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
    public void testSlotReference()
    {
        Slot longSlot = new Slot(0, LONG);
        assertProjection(LONG, toExpression("slot", LONG), 42L, ImmutableMap.of(longSlot, 0), createTuple(42L));
        assertProjection(LONG, toExpression("slot", LONG), null, ImmutableMap.of(longSlot, 0), NULL_LONG_TUPLE);

        Slot doubleSlot = new Slot(0, DOUBLE);
        assertProjection(DOUBLE, toExpression("slot", DOUBLE), 11.1, ImmutableMap.of(doubleSlot, 0), createTuple(11.1));
        assertProjection(DOUBLE, toExpression("slot", DOUBLE), null, ImmutableMap.of(doubleSlot, 0), NULL_DOUBLE_TUPLE);

        Slot stringSlot = new Slot(0, STRING);
        assertProjection(STRING, toExpression("slot", STRING), "foo", ImmutableMap.of(stringSlot, 0), createTuple("foo"));
        assertProjection(STRING, toExpression("slot", STRING), null, ImmutableMap.of(stringSlot, 0), NULL_STRING_TUPLE);
    }

    public static void assertProjection(Type outputType, String expression, @Nullable Object expectedValue)
    {
        assertProjection(outputType, toExpression(expression), expectedValue, ImmutableMap.<Slot, Integer>of());
    }

    public static void assertProjection(Type outputType, Expression expression, @Nullable Object expectedValue, Map<Slot, Integer> slotToChannelMappings, TupleReadable... channels)
    {
        InterpretedProjectionFunction projectionFunction = new InterpretedProjectionFunction(outputType, expression, slotToChannelMappings);

        // create output
        BlockBuilder builder = new BlockBuilder(0, new TupleInfo(outputType.getRawType()));

        // project
        projectionFunction.project(channels, builder);

        // extract single value
        Object actualValue = Iterables.getOnlyElement(Iterables.concat(BlockAssertions.toValues(builder.build())));
        assertEquals(actualValue, expectedValue);
    }

    public static Expression toExpression(String expression)
    {
        return toExpression(expression, Type.DOUBLE);
    }

    public static Expression toExpression(String expression, Type slotType)
    {
        try {
            StatementLexer lexer = new StatementLexer(new CaseInsensitiveStream(new ANTLRStringStream(expression)));
            StatementParser parser = new StatementParser(new CommonTokenStream(lexer));
            StatementBuilder builder = new StatementBuilder(new BufferedTreeNodeStream(parser.expr().getTree()));

            Expression raw = builder.expr().value;

            Optional<QualifiedName> prefix = Optional.of(QualifiedName.of("T"));
            TupleDescriptor descriptor = new TupleDescriptor(ImmutableList.of(
                    new NamedSlot(prefix, Optional.of("slot"), new Slot(0, slotType))));

            Expression rewritten = TreeRewriter.rewriteWith(new NameToSlotRewriter(descriptor), raw);
            return rewritten;
        }
        catch (RecognitionException e) {
            throw Throwables.propagate(e);
        }
    }
}
