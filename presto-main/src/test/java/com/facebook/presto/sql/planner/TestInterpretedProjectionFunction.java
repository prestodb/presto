/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.SlotReference;
import com.facebook.presto.sql.compiler.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.jetbrains.annotations.Nullable;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.sql.compiler.Type.DOUBLE;
import static com.facebook.presto.sql.compiler.Type.LONG;
import static com.facebook.presto.sql.compiler.Type.STRING;
import static com.facebook.presto.sql.planner.InterpretedTestHelper.NULL_LITERAL;
import static com.facebook.presto.sql.planner.InterpretedTestHelper.doubleLiteral;
import static com.facebook.presto.sql.planner.InterpretedTestHelper.longLiteral;
import static com.facebook.presto.sql.planner.InterpretedTestHelper.stringLiteral;
import static com.facebook.presto.sql.tree.ArithmeticExpression.Type.ADD;
import static com.facebook.presto.sql.tree.ArithmeticExpression.Type.DIVIDE;
import static com.facebook.presto.sql.tree.ArithmeticExpression.Type.MODULUS;
import static com.facebook.presto.sql.tree.ArithmeticExpression.Type.MULTIPLY;
import static com.facebook.presto.sql.tree.ArithmeticExpression.Type.SUBTRACT;
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
        assertProjection(LONG, new ArithmeticExpression(ADD, longLiteral(42L), longLiteral(87L)), 42L + 87L);
        assertProjection(DOUBLE, new ArithmeticExpression(ADD, longLiteral(42L), doubleLiteral(22.2)), 42L + 22.2);
        assertProjection(DOUBLE, new ArithmeticExpression(ADD, doubleLiteral(11.1), doubleLiteral(22.2)), 11.1 + 22.2);

        assertProjection(LONG, new ArithmeticExpression(SUBTRACT, longLiteral(42L), longLiteral(87L)), 42L - 87L);
        assertProjection(DOUBLE, new ArithmeticExpression(SUBTRACT, longLiteral(42L), doubleLiteral(22.2)), 42L - 22.2);
        assertProjection(DOUBLE, new ArithmeticExpression(SUBTRACT, doubleLiteral(11.1), doubleLiteral(22.2)), 11.1 - 22.2);

        assertProjection(LONG, new ArithmeticExpression(MULTIPLY, longLiteral(42L), longLiteral(87L)), 42L * 87L);
        assertProjection(DOUBLE, new ArithmeticExpression(MULTIPLY, longLiteral(42L), doubleLiteral(22.2)), 42L * 22.2);
        assertProjection(DOUBLE, new ArithmeticExpression(MULTIPLY, doubleLiteral(11.1), doubleLiteral(22.2)), 11.1 * 22.2);

        assertProjection(LONG, new ArithmeticExpression(DIVIDE, longLiteral(42L), longLiteral(87L)), 42L / 87L);
        assertProjection(DOUBLE, new ArithmeticExpression(DIVIDE, longLiteral(42L), doubleLiteral(22.2)), 42L / 22.2);
        assertProjection(DOUBLE, new ArithmeticExpression(DIVIDE, doubleLiteral(11.1), doubleLiteral(22.2)), 11.1 / 22.2);

        assertProjection(LONG, new ArithmeticExpression(MODULUS, longLiteral(42L), longLiteral(87L)), 42L % 87L);
        assertProjection(DOUBLE, new ArithmeticExpression(MODULUS, longLiteral(42L), doubleLiteral(22.2)), 42L % 22.2);
        assertProjection(DOUBLE, new ArithmeticExpression(MODULUS, doubleLiteral(11.1), doubleLiteral(22.2)), 11.1 % 22.2);
    }

    @Test
    public void testArithmeticExpressionWithNulls()
    {
        for (ArithmeticExpression.Type type : ArithmeticExpression.Type.values()) {
            assertProjection(LONG, new ArithmeticExpression(type, NULL_LITERAL, NULL_LITERAL), null);

            assertProjection(LONG, new ArithmeticExpression(type, longLiteral(42), NULL_LITERAL), null);
            assertProjection(LONG, new ArithmeticExpression(type, NULL_LITERAL, longLiteral(42)), null);

            assertProjection(LONG, new ArithmeticExpression(type, doubleLiteral(11.1), NULL_LITERAL), null);
            assertProjection(LONG, new ArithmeticExpression(type, NULL_LITERAL, doubleLiteral(11.1)), null);
        }
    }

    @Test
    public void testCoalesceExpression()
    {
        assertProjection(LONG, new CoalesceExpression(longLiteral(42), longLiteral(87), longLiteral(100)), 42L);
        assertProjection(LONG, new CoalesceExpression(NULL_LITERAL, longLiteral(87), longLiteral(100)), 87L);
        assertProjection(LONG, new CoalesceExpression(longLiteral(42), NULL_LITERAL, longLiteral(100)), 42L);
        assertProjection(LONG, new CoalesceExpression(NULL_LITERAL, NULL_LITERAL, longLiteral(100)), 100L);

        assertProjection(DOUBLE, new CoalesceExpression(doubleLiteral(42.2), doubleLiteral(87.2), doubleLiteral(100.2)), 42.2);
        assertProjection(DOUBLE, new CoalesceExpression(NULL_LITERAL, doubleLiteral(87.2), doubleLiteral(100.2)), 87.2);
        assertProjection(DOUBLE, new CoalesceExpression(doubleLiteral(42.2), NULL_LITERAL, doubleLiteral(100.2)), 42.2);
        assertProjection(DOUBLE, new CoalesceExpression(NULL_LITERAL, NULL_LITERAL, doubleLiteral(100.2)), 100.2);

        assertProjection(STRING, new CoalesceExpression(stringLiteral("foo"), stringLiteral("bar"), stringLiteral("zah")), "foo");
        assertProjection(STRING, new CoalesceExpression(NULL_LITERAL, stringLiteral("bar"), stringLiteral("zah")), "bar");
        assertProjection(STRING, new CoalesceExpression(stringLiteral("foo"), NULL_LITERAL, stringLiteral("zah")), "foo");
        assertProjection(STRING, new CoalesceExpression(NULL_LITERAL, NULL_LITERAL, stringLiteral("zah")), "zah");

        assertProjection(STRING, new CoalesceExpression(NULL_LITERAL, NULL_LITERAL, NULL_LITERAL), null);
    }

    @Test
    public void testNullIf()
    {
        assertProjection(LONG, new NullIfExpression(longLiteral(42L), longLiteral(42L)), null);
        assertProjection(DOUBLE, new NullIfExpression(longLiteral(42L), doubleLiteral(42.0)), null);
        assertProjection(DOUBLE, new NullIfExpression(doubleLiteral(42.42), doubleLiteral(42.42)), null);
        assertProjection(STRING, new NullIfExpression(stringLiteral("foo"), stringLiteral("foo")), null);

        assertProjection(LONG, new NullIfExpression(longLiteral(42L), longLiteral(87L)), 42L);
        assertProjection(LONG, new NullIfExpression(longLiteral(42L), doubleLiteral(22.2)), 42L);
        assertProjection(DOUBLE, new NullIfExpression(doubleLiteral(22.2), longLiteral(42L)), 22.2);
        assertProjection(DOUBLE, new NullIfExpression(doubleLiteral(42.42), doubleLiteral(22.2)), 42.42);
        assertProjection(STRING, new NullIfExpression(stringLiteral("foo"), stringLiteral("bar")), "foo");

        assertProjection(LONG, new NullIfExpression(NULL_LITERAL, NULL_LITERAL), null);

        assertProjection(LONG, new NullIfExpression(longLiteral(42L), NULL_LITERAL), null);
        assertProjection(LONG, new NullIfExpression(NULL_LITERAL, longLiteral(42L)), null);

        assertProjection(LONG, new NullIfExpression(doubleLiteral(11.1), NULL_LITERAL), null);
        assertProjection(LONG, new NullIfExpression(NULL_LITERAL, doubleLiteral(11.1)), null);
    }

    @Test
    public void testSlotReference()
    {
        Slot longSlot = new Slot(0, LONG);
        assertProjection(LONG, new SlotReference(longSlot), 42L, ImmutableMap.of(longSlot, 0), createTuple(42L));
        assertProjection(LONG, new SlotReference(longSlot), null, ImmutableMap.of(longSlot, 0), NULL_LONG_TUPLE);

        Slot doubleSlot = new Slot(0, DOUBLE);
        assertProjection(DOUBLE, new SlotReference(doubleSlot), 11.1, ImmutableMap.of(doubleSlot, 0), createTuple(11.1));
        assertProjection(DOUBLE, new SlotReference(doubleSlot), null, ImmutableMap.of(doubleSlot, 0), NULL_DOUBLE_TUPLE);

        Slot stringSlot = new Slot(0, STRING);
        assertProjection(STRING, new SlotReference(stringSlot), "foo", ImmutableMap.of(stringSlot, 0), createTuple("foo"));
        assertProjection(STRING, new SlotReference(stringSlot), null, ImmutableMap.of(stringSlot, 0), NULL_STRING_TUPLE);
    }

    public static void assertProjection(Type outputType, Expression expression, @Nullable Object expectedValue)
    {
        assertProjection(outputType, expression, expectedValue, ImmutableMap.<Slot, Integer>of());
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
}
