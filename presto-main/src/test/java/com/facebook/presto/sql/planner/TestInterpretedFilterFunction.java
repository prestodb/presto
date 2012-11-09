/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.InterpretedTestHelper.NULL_LITERAL;
import static com.facebook.presto.sql.planner.InterpretedTestHelper.booleanLiteral;
import static com.facebook.presto.sql.planner.InterpretedTestHelper.doubleLiteral;
import static com.facebook.presto.sql.planner.InterpretedTestHelper.longLiteral;
import static com.facebook.presto.sql.planner.InterpretedTestHelper.stringLiteral;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_EQUAL;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.AND;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Type.OR;
import static org.testng.Assert.assertEquals;

public class TestInterpretedFilterFunction
{

    @Test
    public void testNullLiteral()
    {
        assertFilter(NULL_LITERAL, false);
    }

    @Test
    public void testBooleanLiteral()
    {
        assertFilter(booleanLiteral(true), true);
        assertFilter(booleanLiteral(false), false);
    }

    @Test
    public void testNotExpression()
    {
        assertFilter(new NotExpression(booleanLiteral(true)), false);
        assertFilter(new NotExpression(booleanLiteral(false)), true);
        assertFilter(new NotExpression(NULL_LITERAL), false);
    }

    @Test
    public void testAndExpression()
    {
        assertFilter(new LogicalBinaryExpression(AND, booleanLiteral(true), booleanLiteral(true)), true);
        assertFilter(new LogicalBinaryExpression(AND, booleanLiteral(true), booleanLiteral(false)), false);
        assertFilter(new LogicalBinaryExpression(AND, booleanLiteral(true), NULL_LITERAL), false);

        assertFilter(new LogicalBinaryExpression(AND, booleanLiteral(false), booleanLiteral(true)), false);
        assertFilter(new LogicalBinaryExpression(AND, booleanLiteral(false), booleanLiteral(false)), false);
        assertFilter(new LogicalBinaryExpression(AND, booleanLiteral(false), NULL_LITERAL), false);

        assertFilter(new LogicalBinaryExpression(AND, NULL_LITERAL, booleanLiteral(true)), false);
        assertFilter(new LogicalBinaryExpression(AND, NULL_LITERAL, booleanLiteral(false)), false);
        assertFilter(new LogicalBinaryExpression(AND, NULL_LITERAL, NULL_LITERAL), false);
    }

    @Test
    public void testORExpression()
    {
        assertFilter(new LogicalBinaryExpression(OR, booleanLiteral(true), booleanLiteral(true)), true);
        assertFilter(new LogicalBinaryExpression(OR, booleanLiteral(true), booleanLiteral(false)), true);
        assertFilter(new LogicalBinaryExpression(OR, booleanLiteral(true), NULL_LITERAL), true);

        assertFilter(new LogicalBinaryExpression(OR, booleanLiteral(false), booleanLiteral(true)), true);
        assertFilter(new LogicalBinaryExpression(OR, booleanLiteral(false), booleanLiteral(false)), false);
        assertFilter(new LogicalBinaryExpression(OR, booleanLiteral(false), NULL_LITERAL), false);

        assertFilter(new LogicalBinaryExpression(OR, NULL_LITERAL, booleanLiteral(true)), true);
        assertFilter(new LogicalBinaryExpression(OR, NULL_LITERAL, booleanLiteral(false)), false);
        assertFilter(new LogicalBinaryExpression(OR, NULL_LITERAL, NULL_LITERAL), false);
    }

    @Test
    public void testIsNullExpression()
    {
        assertFilter(new IsNullPredicate(NULL_LITERAL), true);
        assertFilter(new IsNullPredicate(longLiteral(42L)), false);
    }

    @Test
    public void testIsNotNullExpression()
    {
        assertFilter(new IsNotNullPredicate(longLiteral(42L)), true);
        assertFilter(new IsNotNullPredicate(NULL_LITERAL), false);
    }

    @Test
    public void testComparisonExpression()
    {
        assertFilter(new ComparisonExpression(EQUAL, longLiteral(42L), longLiteral(42L)), true);
        assertFilter(new ComparisonExpression(EQUAL, longLiteral(42L), doubleLiteral(42.0)), true);
        assertFilter(new ComparisonExpression(EQUAL, doubleLiteral(42.42), doubleLiteral(42.42)), true);
        assertFilter(new ComparisonExpression(EQUAL, stringLiteral("foo"), stringLiteral("foo")), true);

        assertFilter(new ComparisonExpression(EQUAL, longLiteral(42L), longLiteral(87L)), false);
        assertFilter(new ComparisonExpression(EQUAL, longLiteral(42L), doubleLiteral(22.2)), false);
        assertFilter(new ComparisonExpression(EQUAL, doubleLiteral(42.42), doubleLiteral(22.2)), false);
        assertFilter(new ComparisonExpression(EQUAL, stringLiteral("foo"), stringLiteral("bar")), false);

        assertFilter(new ComparisonExpression(NOT_EQUAL, longLiteral(42L), longLiteral(87L)), true);
        assertFilter(new ComparisonExpression(NOT_EQUAL, longLiteral(42L), doubleLiteral(22.2)), true);
        assertFilter(new ComparisonExpression(NOT_EQUAL, doubleLiteral(42.42), doubleLiteral(22.2)), true);
        assertFilter(new ComparisonExpression(NOT_EQUAL, stringLiteral("foo"), stringLiteral("bar")), true);

        assertFilter(new ComparisonExpression(NOT_EQUAL, longLiteral(42L), longLiteral(42L)), false);
        assertFilter(new ComparisonExpression(NOT_EQUAL, longLiteral(42L), doubleLiteral(42.0)), false);
        assertFilter(new ComparisonExpression(NOT_EQUAL, doubleLiteral(42.42), doubleLiteral(42.42)), false);
        assertFilter(new ComparisonExpression(NOT_EQUAL, stringLiteral("foo"), stringLiteral("foo")), false);

        assertFilter(new ComparisonExpression(LESS_THAN, longLiteral(42L), longLiteral(88L)), true);
        assertFilter(new ComparisonExpression(LESS_THAN, longLiteral(42L), doubleLiteral(88.8)), true);
        assertFilter(new ComparisonExpression(LESS_THAN, doubleLiteral(42.42), doubleLiteral(88.8)), true);
        assertFilter(new ComparisonExpression(LESS_THAN, stringLiteral("bar"), stringLiteral("foo")), true);

        assertFilter(new ComparisonExpression(LESS_THAN, longLiteral(88L), longLiteral(42L)), false);
        assertFilter(new ComparisonExpression(LESS_THAN, longLiteral(88L), doubleLiteral(42.42)), false);
        assertFilter(new ComparisonExpression(LESS_THAN, doubleLiteral(88.88), doubleLiteral(42.42)), false);
        assertFilter(new ComparisonExpression(LESS_THAN, stringLiteral("foo"), stringLiteral("bar")), false);

        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, longLiteral(42L), longLiteral(88L)), true);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, longLiteral(42L), doubleLiteral(88.8)), true);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, doubleLiteral(42.42), doubleLiteral(88.8)), true);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, stringLiteral("bar"), stringLiteral("foo")), true);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, longLiteral(42L), longLiteral(42L)), true);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, longLiteral(42L), doubleLiteral(42.0)), true);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, doubleLiteral(42.42), doubleLiteral(42.42)), true);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, stringLiteral("foo"), stringLiteral("foo")), true);

        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, longLiteral(88L), longLiteral(42L)), false);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, longLiteral(88L), doubleLiteral(42.42)), false);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, doubleLiteral(88.88), doubleLiteral(42.42)), false);
        assertFilter(new ComparisonExpression(LESS_THAN_OR_EQUAL, stringLiteral("foo"), stringLiteral("bar")), false);

        assertFilter(new ComparisonExpression(GREATER_THAN, longLiteral(88L), longLiteral(42L)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN, doubleLiteral(88.8), doubleLiteral((double) 42L)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN, doubleLiteral(88.8), doubleLiteral(42.42)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN, stringLiteral("foo"), stringLiteral("bar")), true);

        assertFilter(new ComparisonExpression(GREATER_THAN, longLiteral(42L), longLiteral(88L)), false);
        assertFilter(new ComparisonExpression(GREATER_THAN, doubleLiteral(42.42), doubleLiteral((double) 88L)), false);
        assertFilter(new ComparisonExpression(GREATER_THAN, doubleLiteral(42.42), doubleLiteral(88.88)), false);
        assertFilter(new ComparisonExpression(GREATER_THAN, stringLiteral("bar"), stringLiteral("foo")), false);

        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, longLiteral(88L), longLiteral(42L)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, doubleLiteral(88.8), doubleLiteral((double) 42L)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, doubleLiteral(88.8), doubleLiteral(42.42)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, stringLiteral("foo"), stringLiteral("bar")), true);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, longLiteral(42L), longLiteral(42L)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, longLiteral(42L), doubleLiteral(42.0)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, doubleLiteral(42.42), doubleLiteral(42.42)), true);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, stringLiteral("foo"), stringLiteral("foo")), true);

        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, longLiteral(42L), longLiteral(88L)), false);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, doubleLiteral(42.42), doubleLiteral((double) 88L)), false);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, doubleLiteral(42.42), doubleLiteral(88.88)), false);
        assertFilter(new ComparisonExpression(GREATER_THAN_OR_EQUAL, stringLiteral("bar"), stringLiteral("foo")), false);
    }

    @Test
    public void testComparisonExpressionWithNulls()
    {
        for (ComparisonExpression.Type type : ComparisonExpression.Type.values()) {
            assertFilter(new ComparisonExpression(type, NULL_LITERAL, NULL_LITERAL), false);

            assertFilter(new ComparisonExpression(type, longLiteral(42L), NULL_LITERAL), false);
            assertFilter(new ComparisonExpression(type, NULL_LITERAL, longLiteral(42L)), false);

            assertFilter(new ComparisonExpression(type, doubleLiteral(11.1), NULL_LITERAL), false);
            assertFilter(new ComparisonExpression(type, NULL_LITERAL, doubleLiteral(11.1)), false);
        }
    }

    public static void assertFilter(Expression predicate, boolean expectedValue)
    {
        InterpretedFilterFunction filterFunction = new InterpretedFilterFunction(predicate, ImmutableMap.<Slot, Integer>of());
        boolean result = filterFunction.filter();
        assertEquals(result, expectedValue);
    }
}
