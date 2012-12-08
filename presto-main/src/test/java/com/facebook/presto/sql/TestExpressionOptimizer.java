package com.facebook.presto.sql;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import org.antlr.runtime.RecognitionException;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.parser.SqlParser.createExpression;
import static org.testng.Assert.assertEquals;

public class TestExpressionOptimizer
{
    @Test
    public void testAnd()
            throws Exception
    {
        assertOptimizedEquals("true and false", "false");
        assertOptimizedEquals("false and true", "false");
        assertOptimizedEquals("false and false", "false");

        assertOptimizedEquals("true and null", "null");
        assertOptimizedEquals("false and null", "false");
        assertOptimizedEquals("null and true", "null");
        assertOptimizedEquals("null and false", "false");

        assertOptimizedEquals("a='z' and true", "a='z'");
        assertOptimizedEquals("a='z' and false", "false");
        assertOptimizedEquals("true and a='z'", "a='z'");
        assertOptimizedEquals("false and a='z'", "false");
    }

    @Test
    public void testOr()
            throws Exception
    {
        assertOptimizedEquals("true or true", "true");
        assertOptimizedEquals("true or false", "true");
        assertOptimizedEquals("false or true", "true");
        assertOptimizedEquals("false or false", "false");

        assertOptimizedEquals("true or null", "true");
        assertOptimizedEquals("null or true", "true");

        assertOptimizedEquals("false or null", "null");
        assertOptimizedEquals("null or false", "null");

        assertOptimizedEquals("a='z' or true", "true");
        assertOptimizedEquals("a='z' or false", "a='z'");
        assertOptimizedEquals("true or a='z'", "true");
        assertOptimizedEquals("false or a='z'", "a='z'");
    }

    @Test
    public void testComparison()
            throws Exception
    {
        assertOptimizedEquals("null = null", "false");

        assertOptimizedEquals("'a' = 'b'", "false");
        assertOptimizedEquals("'a' = 'a'", "true");
        assertOptimizedEquals("'a' = null", "false");
        assertOptimizedEquals("null = 'a'", "false");
    }

    private void assertOptimizedEquals(String expression, String expected)
            throws RecognitionException
    {
        Expression actual = new ExpressionOptimizer(ImmutableMap.<Symbol, String>of())
                .optimize(createExpression(expression));

        assertEquals(actual, createExpression(expected));
    }

}
