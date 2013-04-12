package com.facebook.presto.sql;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.InputResolver;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
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
    public void testNonDeterministicFunctionCall()
            throws Exception
    {
        // optimize should do nothing
        assertOptimizedEquals("random()", "random()");

        // evaluate should execute
        Object value = evaluate("random()");
        Assert.assertTrue(value instanceof Double);
        double randomValue = (double) value;
        Assert.assertTrue(0 <= randomValue && randomValue < 1);
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

    @Test
    public void testExtract()
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

    @Test
    public void testIn()
            throws Exception
    {
        assertOptimizedEquals("3 in (2, 4, 3, 5)", "true");
        assertOptimizedEquals("3 in (2, 4, 9, 5)", "false");
        assertOptimizedEquals("3 in (2, null, 3, 5)", "true");

        assertOptimizedEquals("'foo' in ('bar', 'baz', 'foo', 'blah')", "true");
        assertOptimizedEquals("'foo' in ('bar', 'baz', 'buz', 'blah')", "false");
        assertOptimizedEquals("'foo' in ('bar', null, 'foo', 'blah')", "true");

        assertOptimizedEquals("null in (2, null, 3, 5)", "null");
        assertOptimizedEquals("3 in (2, null)", "null");

        assertOptimizedEquals("boundLong in (2, 1234, 3, 5)", "true");
        assertOptimizedEquals("boundLong in (2, 4, 3, 5)", "false");
        assertOptimizedEquals("1234 in (2, boundLong, 3, 5)", "true");
        assertOptimizedEquals("99 in (2, boundLong, 3, 5)", "false");
        assertOptimizedEquals("boundLong in (2, boundLong, 3, 5)", "true");

        assertOptimizedEquals("boundString in ('bar', 'hello', 'foo', 'blah')", "true");
        assertOptimizedEquals("boundString in ('bar', 'baz', 'foo', 'blah')", "false");
        assertOptimizedEquals("'hello' in ('bar', boundString, 'foo', 'blah')", "true");
        assertOptimizedEquals("'baz' in ('bar', boundString, 'foo', 'blah')", "false");

        assertOptimizedEquals("boundLong in (2, 1234, a, 5)", "true");
        assertOptimizedEquals("boundString in ('bar', 'hello', a, 'blah')", "true");

        assertOptimizedEquals("boundLong in (2, 4, a, b, 9)", "1234 in (a, b)");
        assertOptimizedEquals("a in (2, 4, boundLong, b, 5)", "a in (2, 4, 1234, b, 5)");
    }

    @Test
    public void testCurrenTimestamp()
            throws Exception
    {
        long current = System.currentTimeMillis();
        assertOptimizedEquals("current_timestamp >= " + current, "true");
        assertOptimizedEquals("current_timestamp > " + current + TimeUnit.MINUTES.toMillis(1), "false");
    }

    @Test
    public void testCastToString()
            throws Exception
    {
        // long
        assertOptimizedEquals("cast(123 as VARCHAR)", "'123'");
        assertOptimizedEquals("cast(-123 as VARCHAR)", "'-123'");

        // double
        assertOptimizedEquals("cast(123.0 as VARCHAR)", "'123.0'");
        assertOptimizedEquals("cast(-123.0 as VARCHAR)", "'-123.0'");
        assertOptimizedEquals("cast(123.456 as VARCHAR)", "'123.456'");
        assertOptimizedEquals("cast(-123.456 as VARCHAR)", "'-123.456'");

        // boolean
        assertOptimizedEquals("cast(true as VARCHAR)", "'true'");
        assertOptimizedEquals("cast(false as VARCHAR)", "'false'");

        // string
        assertOptimizedEquals("cast('xyz' as VARCHAR)", "'xyz'");

        // null
        assertOptimizedEquals("cast(null as VARCHAR)", "null");
    }

    @Test
    public void testCastToBoolean()
            throws Exception
    {
        // long
        assertOptimizedEquals("cast(123 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(-123 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(0 as BOOLEAN)", "false");

        // boolean
        assertOptimizedEquals("cast(true as BOOLEAN)", "true");
        assertOptimizedEquals("cast(false as BOOLEAN)", "false");

        // string
        assertOptimizedEquals("cast('true' as BOOLEAN)", "true");
        assertOptimizedEquals("cast('false' as BOOLEAN)", "false");
        assertOptimizedEquals("cast('t' as BOOLEAN)", "true");
        assertOptimizedEquals("cast('f' as BOOLEAN)", "false");
        assertOptimizedEquals("cast('1' as BOOLEAN)", "true");
        assertOptimizedEquals("cast('0' as BOOLEAN)", "false");

        // null
        assertOptimizedEquals("cast(null as BOOLEAN)", "null");

        // double
        assertInvalidCast("cast(123.0 as BOOLEAN)");
    }

    @Test
    public void testCastToLong()
            throws Exception
    {
        // long
        assertOptimizedEquals("cast(0 as BIGINT)", "0");
        assertOptimizedEquals("cast(123 as BIGINT)", "123");
        assertOptimizedEquals("cast(-123 as BIGINT)", "-123");

        // double
        assertOptimizedEquals("cast(123.0 as BIGINT)", "123");
        assertOptimizedEquals("cast(-123.0 as BIGINT)", "-123");
        assertOptimizedEquals("cast(123.456 as BIGINT)", "123");
        assertOptimizedEquals("cast(-123.456 as BIGINT)", "-123");

        // boolean
        assertOptimizedEquals("cast(true as BIGINT)", "1");
        assertOptimizedEquals("cast(false as BIGINT)", "0");

        // string
        assertOptimizedEquals("cast('123' as BIGINT)", "123");
        assertOptimizedEquals("cast('-123' as BIGINT)", "-123");

        // null
        assertOptimizedEquals("cast(null as BIGINT)", "null");
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        // long
        assertOptimizedEquals("cast(0 as DOUBLE)", "0.0");
        assertOptimizedEquals("cast(123 as DOUBLE)", "123.0");
        assertOptimizedEquals("cast(-123 as DOUBLE)", "-123.0");

        // double
        assertOptimizedEquals("cast(123.0 as DOUBLE)", "123.0");
        assertOptimizedEquals("cast(-123.0 as DOUBLE)", "-123.0");
        assertOptimizedEquals("cast(123.456 as DOUBLE)", "123.456");
        assertOptimizedEquals("cast(-123.456 as DOUBLE)", "-123.456");

        // string
        assertOptimizedEquals("cast('0' as DOUBLE)", "0.0");
        assertOptimizedEquals("cast('123' as DOUBLE)", "123.0");
        assertOptimizedEquals("cast('-123' as DOUBLE)", "-123.0");
        assertOptimizedEquals("cast('123.0' as DOUBLE)", "123.0");
        assertOptimizedEquals("cast('-123.0' as DOUBLE)", "-123.0");
        assertOptimizedEquals("cast('123.456' as DOUBLE)", "123.456");
        assertOptimizedEquals("cast('-123.456' as DOUBLE)", "-123.456");

        // null
        assertOptimizedEquals("cast(null as DOUBLE)", "null");

        // boolean
        assertInvalidCast("cast(true as DOUBLE)");
        assertInvalidCast("cast(false as DOUBLE)");
    }

    @Test
    public void testCastOptimization()
            throws Exception
    {
        assertOptimizedEquals("cast(boundLong as VARCHAR)", "'1234'");
        assertOptimizedEquals("cast(boundLong + 1 as VARCHAR)", "'1235'");
        assertOptimizedEquals("cast(unbound as VARCHAR)", "cast(unbound as VARCHAR)");
    }

    @Test
    public void testReservedWithDoubleQuotes()
            throws Exception
    {
        assertOptimizedEquals("\"time\"", "\"time\"");
    }

    @Test
    public void testSearchCase()
            throws Exception
    {
        assertOptimizedEquals("case " +
                "when true then 33 " +
                "end",
                "33");
        assertOptimizedEquals("case " +
                "when false then 1 " +
                "else 33 " +
                "end",
                "33");

        assertOptimizedEquals("case " +
                "when boundLong = 1234 then 33 " +
                "end",
                "33");
        assertOptimizedEquals("case " +
                "when true then boundLong " +
                "end",
                "1234");
        assertOptimizedEquals("case " +
                "when false then 1 " +
                "else boundLong " +
                "end",
                "1234");

        assertOptimizedEquals("case " +
                "when boundLong = 1234 then 33 " +
                "else a " +
                "end",
                "33");
        assertOptimizedEquals("case " +
                "when true then boundLong " +
                "else a " +
                "end",
                "1234");
        assertOptimizedEquals("case " +
                "when false then a " +
                "else boundLong " +
                "end",
                "1234");

        assertOptimizedEquals("case " +
                "when a = 1234 then 33 " +
                "else 1 " +
                "end",
                "" +
                        "case " +
                        "when a = 1234 then 33 " +
                        "else 1 " +
                        "end");

    }

    @Test
    public void testSimpleCase()
            throws Exception
    {
        assertOptimizedEquals("case true " +
                "when true then 33 " +
                "end",
                "33");
        assertOptimizedEquals("case true " +
                "when false then 1 " +
                "else 33 end",
                "33");

        assertOptimizedEquals("case boundLong " +
                "when 1234 then 33 " +
                "end",
                "33");
        assertOptimizedEquals("case 1234 " +
                "when boundLong then 33 " +
                "end",
                "33");
        assertOptimizedEquals("case true " +
                "when true then boundLong " +
                "end",
                "1234");
        assertOptimizedEquals("case true " +
                "when false then 1 " +
                "else boundLong " +
                "end",
                "1234");

        assertOptimizedEquals("case boundLong " +
                "when 1234 then 33 " +
                "else a " +
                "end",
                "33");
        assertOptimizedEquals("case true " +
                "when true then boundLong " +
                "else a " +
                "end",
                "1234");
        assertOptimizedEquals("case true " +
                "when false then a " +
                "else boundLong " +
                "end",
                "1234");

        assertOptimizedEquals("case a " +
                "when 1234 then 33 " +
                "else 1 " +
                "end",
                "" +
                        "case a " +
                        "when 1234 then 33 " +
                        "else 1 " +
                        "end");
    }

    @Test
    public void testIf()
            throws Exception
    {
        assertOptimizedEquals("IF(2 = 2, 3, 4)", "3");
        assertOptimizedEquals("IF(1 = 2, 3, 4)", "4");

        assertOptimizedEquals("IF(true, 3, 4)", "3");
        assertOptimizedEquals("IF(false, 3, 4)", "4");
        assertOptimizedEquals("IF(null, 3, 4)", "4");

        assertOptimizedEquals("IF(true, 3, null)", "3");
        assertOptimizedEquals("IF(false, 3, null)", "null");
        assertOptimizedEquals("IF(true, null, 4)", "null");
        assertOptimizedEquals("IF(false, null, 4)", "4");
        assertOptimizedEquals("IF(true, null, null)", "null");
        assertOptimizedEquals("IF(false, null, null)", "null");

        assertOptimizedEquals("IF(true, 3.5, 4.2)", "3.5");
        assertOptimizedEquals("IF(false, 3.5, 4.2)", "4.2");

        assertOptimizedEquals("IF(true, 'foo', 'bar')", "'foo'");
        assertOptimizedEquals("IF(false, 'foo', 'bar')", "'bar'");

        assertOptimizedEquals("IF(a, 1 + 2, 3 + 4)", "IF(a, 3, 7)");
    }

    private static void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertEquals(optimize(actual), optimize(expected));
    }

    private static void assertInvalidCast(@Language("SQL") String expression)
    {
        try {
            Object value = optimize(expression);
            Assert.fail(String.format("Expected '%s' to fail but it returned %s", expression, value));
        }
        catch (IllegalArgumentException e) {
            // success
        }
    }

    private static Object optimize(String expression)
    {
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(new SymbolResolver()
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
        }, new MetadataManager(), new Session(null, DEFAULT_CATALOG, DEFAULT_SCHEMA));

        Expression parsedExpression = createExpression(expression);

        // verify roundtrip
        Expression roundtrip = createExpression(ExpressionFormatter.formatExpression(parsedExpression));
        assertEquals(parsedExpression, roundtrip);

        return interpreter.process(parsedExpression, null);
    }

    private static Object evaluate(String expression)
    {
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionInterpreter(new InputResolver()
        {
            @Override
            public Object getValue(Input input)
            {
                throw new UnsupportedOperationException();
            }
        }, new MetadataManager(), new Session(null, DEFAULT_CATALOG, DEFAULT_SCHEMA));

        Expression parsedExpression = createExpression(expression);

        // verify roundtrip
        Expression roundtrip = createExpression(ExpressionFormatter.formatExpression(parsedExpression));
        assertEquals(parsedExpression, roundtrip);

        return interpreter.process(parsedExpression, null);
    }
}
