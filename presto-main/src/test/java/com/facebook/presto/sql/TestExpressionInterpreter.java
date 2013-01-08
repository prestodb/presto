package com.facebook.presto.sql;

import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import org.antlr.runtime.RecognitionException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.parser.SqlParser.createExpression;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestExpressionInterpreter
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
        assertOptimizedEquals("null = null", "null");

        assertOptimizedEquals("'a' = 'b'", "false");
        assertOptimizedEquals("'a' = 'a'", "true");
        assertOptimizedEquals("'a' = null", "null");
        assertOptimizedEquals("null = 'a'", "null");

        assertOptimizedEquals("boundLong = 1234", "true");
        assertOptimizedEquals("boundDouble = 12.34", "true");
        assertOptimizedEquals("boundString = 'hello'", "true");
        assertOptimizedEquals("boundLong = a", "1234 = a");
    }

    @Test
    public void testFunctionCall()
        throws Exception
    {
        assertOptimizedEquals("abs(-5)", "5");
        assertOptimizedEquals("abs(-10-5)", "15");
        assertOptimizedEquals("abs(-boundLong + 1)", "1233");
        assertOptimizedEquals("abs(-boundLong)", "1234");
        assertOptimizedEquals("abs(a)", "abs(a)");
        assertOptimizedEquals("abs(a + 1)", "abs(a + 1)");
    }

    @Test
    public void testBetween()
            throws Exception
    {
        assertOptimizedEquals("3 between 2 and 4", "true");
        assertOptimizedEquals("2 between 3 and 4", "false");
        assertOptimizedEquals("null between 2 and 4", "null");
        assertOptimizedEquals("3 between null and 4", "null");
        assertOptimizedEquals("3 between 2 and null", "null");

        assertOptimizedEquals("'c' between 'b' and 'd'", "true");
        assertOptimizedEquals("'b' between 'c' and 'd'", "false");
        assertOptimizedEquals("null between 'b' and 'd'", "null");
        assertOptimizedEquals("'c' between null and 'd'", "null");
        assertOptimizedEquals("'c' between 'b' and null", "null");

        assertOptimizedEquals("boundLong between 1000 and 2000", "true");
        assertOptimizedEquals("boundLong between 3 and 4", "false");
        assertOptimizedEquals("boundString between 'e' and 'i'", "true");
        assertOptimizedEquals("boundString between 'a' and 'b'", "false");

        assertOptimizedEquals("boundLong between a and 2000 + 1", "1234 between a and 2001");
        assertOptimizedEquals("boundString between a and 'bar'", "'hello' between a and 'bar'");
    }

    public void testExtract()
            throws RecognitionException
    {
        DateTime dateTime = new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        long millis = dateTime.getMillis();

        assertOptimizedEquals("extract (CENTURY from " + millis + ")", "20");
        assertOptimizedEquals("extract (YEAR from " + millis + ")", "2001");
        assertOptimizedEquals("extract (QUARTER from " + millis + ")", "3");
        assertOptimizedEquals("extract (MONTH from " + millis + ")", "8");
        assertOptimizedEquals("extract (WEEK from " + millis + ")", "34");
        assertOptimizedEquals("extract (DOW from " + millis + ")", "3");
        assertOptimizedEquals("extract (DOY from " + millis + ")", "234");
        assertOptimizedEquals("extract (DAY from " + millis + ")", "22");
        assertOptimizedEquals("extract (HOUR from " + millis + ")", "3");
        assertOptimizedEquals("extract (MINUTE from " + millis + ")", "4");
        assertOptimizedEquals("extract (SECOND from " + millis + ")", "5");
        assertOptimizedEquals("extract (TIMEZONE_HOUR from " + millis + ")", "0");
        assertOptimizedEquals("extract (TIMEZONE_MINUTE from " + millis + ")", "0");


        assertOptimizedEquals("extract (CENTURY from boundTimestamp)", "20");
        assertOptimizedEquals("extract (YEAR from boundTimestamp)", "2001");
        assertOptimizedEquals("extract (QUARTER from boundTimestamp)", "3");
        assertOptimizedEquals("extract (MONTH from boundTimestamp)", "8");
        assertOptimizedEquals("extract (WEEK from boundTimestamp)", "34");
        assertOptimizedEquals("extract (DOW from boundTimestamp)", "3");
        assertOptimizedEquals("extract (DOY from boundTimestamp)", "234");
        assertOptimizedEquals("extract (DAY from boundTimestamp)", "22");
        assertOptimizedEquals("extract (HOUR from boundTimestamp)", "3");
        assertOptimizedEquals("extract (MINUTE from boundTimestamp)", "4");
        assertOptimizedEquals("extract (SECOND from boundTimestamp)", "5");
        assertOptimizedEquals("extract (TIMEZONE_HOUR from boundTimestamp)", "0");
        assertOptimizedEquals("extract (TIMEZONE_MINUTE from boundTimestamp)", "0");

        assertOptimizedEquals("extract (YEAR from a)", "extract (YEAR from a)");
        assertOptimizedEquals("extract (SECOND from boundTimestamp + 1000)", "6");
    }

    private void assertOptimizedEquals(String actual, String expected)
            throws RecognitionException
    {
        ExpressionInterpreter interpreter = new ExpressionInterpreter(new SymbolResolver()
        {
            @Override
            public Object getValue(Symbol symbol)
            {
                switch (symbol.getName().toLowerCase()) {
                    case "boundlong":
                        return 1234L;
                    case "boundstring":
                        return Slices.wrappedBuffer("hello".getBytes(UTF_8));
                    case "bounddouble":
                        return 12.34;
                    case "boundtimestamp":
                        return new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis();
                }

                return new QualifiedNameReference(symbol.toQualifiedName());
            }
        }, new TestingMetadata());

        assertEquals(interpreter.process(createExpression(actual), null), interpreter.process(createExpression(expected), null));
    }

}
