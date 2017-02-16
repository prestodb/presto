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
package com.facebook.presto.sql;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.util.maps.IdentityLinkedHashMap;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionInterpreter;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestExpressionInterpreter
{
    private static final int TEST_VARCHAR_TYPE_LENGTH = 17;
    private static final Map<Symbol, Type> SYMBOL_TYPES = ImmutableMap.<Symbol, Type>builder()
            .put(new Symbol("bound_integer"), INTEGER)
            .put(new Symbol("bound_long"), BIGINT)
            .put(new Symbol("bound_string"), createVarcharType(TEST_VARCHAR_TYPE_LENGTH))
            .put(new Symbol("bound_varbinary"), VarbinaryType.VARBINARY)
            .put(new Symbol("bound_double"), DOUBLE)
            .put(new Symbol("bound_boolean"), BOOLEAN)
            .put(new Symbol("bound_date"), DATE)
            .put(new Symbol("bound_time"), TIME)
            .put(new Symbol("bound_timestamp"), TIMESTAMP)
            .put(new Symbol("bound_pattern"), VARCHAR)
            .put(new Symbol("bound_null_string"), VARCHAR)
            .put(new Symbol("time"), BIGINT) // for testing reserved identifiers
            .put(new Symbol("unbound_integer"), INTEGER)
            .put(new Symbol("unbound_long"), BIGINT)
            .put(new Symbol("unbound_long2"), BIGINT)
            .put(new Symbol("unbound_string"), VARCHAR)
            .put(new Symbol("unbound_double"), DOUBLE)
            .put(new Symbol("unbound_boolean"), BOOLEAN)
            .put(new Symbol("unbound_date"), DATE)
            .put(new Symbol("unbound_time"), TIME)
            .put(new Symbol("unbound_timestamp"), TIMESTAMP)
            .put(new Symbol("unbound_interval"), INTERVAL_DAY_TIME)
            .put(new Symbol("unbound_pattern"), VARCHAR)
            .put(new Symbol("unbound_null_string"), VARCHAR)
            .build();

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

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
        assertOptimizedEquals("null and null", "null");

        assertOptimizedEquals("unbound_string='z' and true", "unbound_string='z'");
        assertOptimizedEquals("unbound_string='z' and false", "false");
        assertOptimizedEquals("true and unbound_string='z'", "unbound_string='z'");
        assertOptimizedEquals("false and unbound_string='z'", "false");

        assertOptimizedEquals("bound_string='z' and bound_long=1+1", "bound_string='z' and bound_long=2");
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
        assertOptimizedEquals("null or null", "null");

        assertOptimizedEquals("false or null", "null");
        assertOptimizedEquals("null or false", "null");

        assertOptimizedEquals("bound_string='z' or true", "true");
        assertOptimizedEquals("bound_string='z' or false", "bound_string='z'");
        assertOptimizedEquals("true or bound_string='z'", "true");
        assertOptimizedEquals("false or bound_string='z'", "bound_string='z'");

        assertOptimizedEquals("bound_string='z' or bound_long=1+1", "bound_string='z' or bound_long=2");
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
        assertOptimizedEquals("bound_integer = 1234", "true");
        assertOptimizedEquals("bound_integer = 12340000000", "false");
        assertOptimizedEquals("bound_long = BIGINT '1234'", "true");
        assertOptimizedEquals("bound_long = 1234", "true");
        assertOptimizedEquals("bound_double = 12.34", "true");
        assertOptimizedEquals("bound_string = 'hello'", "true");
        assertOptimizedEquals("bound_long = unbound_long", "1234 = unbound_long");

        assertOptimizedEquals("10151082135029368 = 10151082135029369", "false");

        assertOptimizedEquals("bound_varbinary = X'a b'", "true");
        assertOptimizedEquals("bound_varbinary = X'a d'", "false");
    }

    @Test
    public void testIsDistinctFrom()
            throws Exception
    {
        assertOptimizedEquals("null is distinct from null", "false");

        assertOptimizedEquals("3 is distinct from 4", "true");
        assertOptimizedEquals("3 is distinct from BIGINT '4'", "true");
        assertOptimizedEquals("3 is distinct from 4000000000", "true");
        assertOptimizedEquals("3 is distinct from 3", "false");
        assertOptimizedEquals("3 is distinct from null", "true");
        assertOptimizedEquals("null is distinct from 3", "true");

        assertOptimizedEquals("10151082135029368 is distinct from 10151082135029369", "true");
    }

    @Test
    public void testIsNull()
            throws Exception
    {
        assertOptimizedEquals("null is null", "true");
        assertOptimizedEquals("1 is null", "false");
        assertOptimizedEquals("10000000000 is null", "false");
        assertOptimizedEquals("BIGINT '1' is null", "false");
        assertOptimizedEquals("1.0 is null", "false");
        assertOptimizedEquals("'a' is null", "false");
        assertOptimizedEquals("true is null", "false");
        assertOptimizedEquals("null+1 is null", "true");
        assertOptimizedEquals("unbound_string is null", "unbound_string is null");
        assertOptimizedEquals("unbound_long+(1+1) is null", "unbound_long+2 is null");
    }

    @Test
    public void testIsNotNull()
            throws Exception
    {
        assertOptimizedEquals("null is not null", "false");
        assertOptimizedEquals("1 is not null", "true");
        assertOptimizedEquals("10000000000 is not null", "true");
        assertOptimizedEquals("BIGINT '1' is not null", "true");
        assertOptimizedEquals("1.0 is not null", "true");
        assertOptimizedEquals("'a' is not null", "true");
        assertOptimizedEquals("true is not null", "true");
        assertOptimizedEquals("null+1 is not null", "false");
        assertOptimizedEquals("unbound_string is not null", "unbound_string is not null");
        assertOptimizedEquals("unbound_long+(1+1) is not null", "unbound_long+2 is not null");
    }

    @Test
    public void testNullIf()
            throws Exception
    {
        assertOptimizedEquals("nullif(true, true)", "null");
        assertOptimizedEquals("nullif(true, false)", "true");
        assertOptimizedEquals("nullif(null, false)", "null");
        assertOptimizedEquals("nullif(true, null)", "true");

        assertOptimizedEquals("nullif('a', 'a')", "null");
        assertOptimizedEquals("nullif('a', 'b')", "'a'");
        assertOptimizedEquals("nullif(null, 'b')", "null");
        assertOptimizedEquals("nullif('a', null)", "'a'");

        assertOptimizedEquals("nullif(1, 1)", "null");
        assertOptimizedEquals("nullif(1, 2)", "1");
        assertOptimizedEquals("nullif(1, BIGINT '2')", "1");
        assertOptimizedEquals("nullif(1, 20000000000)", "1");
        assertOptimizedEquals("nullif(1.0, 1)", "null");
        assertOptimizedEquals("nullif(10000000000.0, 10000000000)", "null");
        assertOptimizedEquals("nullif(1.1, 1)", "1.1");
        assertOptimizedEquals("nullif(1.1, 1.1)", "null");
        assertOptimizedEquals("nullif(1, 2-1)", "null");
        assertOptimizedEquals("nullif(null, null)", "null");
        assertOptimizedEquals("nullif(1, null)", "1");
        assertOptimizedEquals("nullif(unbound_long, 1)", "nullif(unbound_long, 1)");
        assertOptimizedEquals("nullif(unbound_long, unbound_long2)", "nullif(unbound_long, unbound_long2)");
        assertOptimizedEquals("nullif(unbound_long, unbound_long2+(1+1))", "nullif(unbound_long, unbound_long2+2)");
    }

    @Test
    public void testNegative()
            throws Exception
    {
        assertOptimizedEquals("-(1)", "-1");
        assertOptimizedEquals("-(BIGINT '1')", "BIGINT '-1'");
        assertOptimizedEquals("-(unbound_long+1)", "-(unbound_long+1)");
        assertOptimizedEquals("-(1+1)", "-2");
        assertOptimizedEquals("-(1+ BIGINT '1')", "BIGINT '-2'");
        assertOptimizedEquals("-(CAST(NULL AS BIGINT))", "null");
        assertOptimizedEquals("-(unbound_long+(1+1))", "-(unbound_long+2)");
    }

    @Test
    public void testNot()
            throws Exception
    {
        assertOptimizedEquals("not true", "false");
        assertOptimizedEquals("not false", "true");
        assertOptimizedEquals("not null", "null");
        assertOptimizedEquals("not 1=1", "false");
        assertOptimizedEquals("not 1=BIGINT '1'", "false");
        assertOptimizedEquals("not 1!=1", "true");
        assertOptimizedEquals("not unbound_long=1", "not unbound_long=1");
        assertOptimizedEquals("not unbound_long=(1+1)", "not unbound_long=2");
    }

    @Test
    public void testFunctionCall()
            throws Exception
    {
        assertOptimizedEquals("abs(-5)", "5");
        assertOptimizedEquals("abs(-10-5)", "15");
        assertOptimizedEquals("abs(-bound_integer + 1)", "1233");
        assertOptimizedEquals("abs(-bound_long + 1)", "1233");
        assertOptimizedEquals("abs(-bound_long + BIGINT '1')", "1233");
        assertOptimizedEquals("abs(-bound_long)", "1234");
        assertOptimizedEquals("abs(unbound_long)", "abs(unbound_long)");
        assertOptimizedEquals("abs(unbound_long + 1)", "abs(unbound_long + 1)");
    }

    @Test
    public void testNonDeterministicFunctionCall()
            throws Exception
    {
        // optimize should do nothing
        assertOptimizedEquals("random()", "random()");

        // evaluate should execute
        Object value = evaluate("random()");
        assertTrue(value instanceof Double);
        double randomValue = (double) value;
        assertTrue(0 <= randomValue && randomValue < 1);
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

        assertOptimizedEquals("'cc' between 'b' and 'd'", "true");
        assertOptimizedEquals("'b' between 'cc' and 'd'", "false");
        assertOptimizedEquals("null between 'b' and 'd'", "null");
        assertOptimizedEquals("'cc' between null and 'd'", "null");
        assertOptimizedEquals("'cc' between 'b' and null", "null");

        assertOptimizedEquals("bound_integer between 1000 and 2000", "true");
        assertOptimizedEquals("bound_integer between 3 and 4", "false");
        assertOptimizedEquals("bound_long between 1000 and 2000", "true");
        assertOptimizedEquals("bound_long between 3 and 4", "false");
        assertOptimizedEquals("bound_long between bound_integer and (bound_long + 1)", "true");
        assertOptimizedEquals("bound_string between 'e' and 'i'", "true");
        assertOptimizedEquals("bound_string between 'a' and 'b'", "false");

        assertOptimizedEquals("bound_long between unbound_long and 2000 + 1", "1234 between unbound_long and 2001");
        assertOptimizedEquals(
                "bound_string between unbound_string and 'bar'",
                format("CAST('hello' AS VARCHAR(%s)) between unbound_string and 'bar'", TEST_VARCHAR_TYPE_LENGTH));
    }

    @Test
    public void testExtract()
    {
        DateTime dateTime = new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC);
        double seconds = dateTime.getMillis() / 1000.0;

        assertOptimizedEquals("extract (YEAR from from_unixtime(" + seconds + "))", "2001");
        assertOptimizedEquals("extract (QUARTER from from_unixtime(" + seconds + "))", "3");
        assertOptimizedEquals("extract (MONTH from from_unixtime(" + seconds + "))", "8");
        assertOptimizedEquals("extract (WEEK from from_unixtime(" + seconds + "))", "34");
        assertOptimizedEquals("extract (DOW from from_unixtime(" + seconds + "))", "3");
        assertOptimizedEquals("extract (DOY from from_unixtime(" + seconds + "))", "234");
        assertOptimizedEquals("extract (DAY from from_unixtime(" + seconds + "))", "22");
        assertOptimizedEquals("extract (HOUR from from_unixtime(" + seconds + "))", "3");
        assertOptimizedEquals("extract (MINUTE from from_unixtime(" + seconds + "))", "4");
        assertOptimizedEquals("extract (SECOND from from_unixtime(" + seconds + "))", "5");
        assertOptimizedEquals("extract (TIMEZONE_HOUR from from_unixtime(" + seconds + ", 7, 9))", "7");
        assertOptimizedEquals("extract (TIMEZONE_MINUTE from from_unixtime(" + seconds + ", 7, 9))", "9");

        assertOptimizedEquals("extract (YEAR from bound_timestamp)", "2001");
        assertOptimizedEquals("extract (QUARTER from bound_timestamp)", "3");
        assertOptimizedEquals("extract (MONTH from bound_timestamp)", "8");
        assertOptimizedEquals("extract (WEEK from bound_timestamp)", "34");
        assertOptimizedEquals("extract (DOW from bound_timestamp)", "3");
        assertOptimizedEquals("extract (DOY from bound_timestamp)", "234");
        assertOptimizedEquals("extract (DAY from bound_timestamp)", "22");
        assertOptimizedEquals("extract (HOUR from bound_timestamp)", "3");
        assertOptimizedEquals("extract (MINUTE from bound_timestamp)", "4");
        assertOptimizedEquals("extract (SECOND from bound_timestamp)", "5");
        // todo reenable when cast as timestamp with time zone is implemented
        // todo add bound timestamp with time zone
        //assertOptimizedEquals("extract (TIMEZONE_HOUR from bound_timestamp)", "0");
        //assertOptimizedEquals("extract (TIMEZONE_MINUTE from bound_timestamp)", "0");

        assertOptimizedEquals("extract (YEAR from unbound_timestamp)", "extract (YEAR from unbound_timestamp)");
        assertOptimizedEquals("extract (SECOND from bound_timestamp + INTERVAL '3' SECOND)", "8");
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

        assertOptimizedEquals("bound_integer in (2, 1234, 3, 5)", "true");
        assertOptimizedEquals("bound_integer in (2, 4, 3, 5)", "false");
        assertOptimizedEquals("1234 in (2, bound_integer, 3, 5)", "true");
        assertOptimizedEquals("99 in (2, bound_integer, 3, 5)", "false");
        assertOptimizedEquals("bound_integer in (2, bound_integer, 3, 5)", "true");

        assertOptimizedEquals("bound_long in (2, 1234, 3, 5)", "true");
        assertOptimizedEquals("bound_long in (2, 4, 3, 5)", "false");
        assertOptimizedEquals("1234 in (2, bound_long, 3, 5)", "true");
        assertOptimizedEquals("99 in (2, bound_long, 3, 5)", "false");
        assertOptimizedEquals("bound_long in (2, bound_long, 3, 5)", "true");

        assertOptimizedEquals("bound_string in ('bar', 'hello', 'foo', 'blah')", "true");
        assertOptimizedEquals("bound_string in ('bar', 'baz', 'foo', 'blah')", "false");
        assertOptimizedEquals("'hello' in ('bar', bound_string, 'foo', 'blah')", "true");
        assertOptimizedEquals("'baz' in ('bar', bound_string, 'foo', 'blah')", "false");

        assertOptimizedEquals("bound_long in (2, 1234, unbound_long, 5)", "true");
        assertOptimizedEquals("bound_string in ('bar', 'hello', unbound_string, 'blah')", "true");

        assertOptimizedEquals("bound_long in (2, 4, unbound_long, unbound_long2, 9)", "1234 in (unbound_long, unbound_long2)");
        assertOptimizedEquals("unbound_long in (2, 4, bound_long, unbound_long2, 5)", "unbound_long in (2, 4, 1234, unbound_long2, 5)");
    }

    @Test
    public void testCurrentTimestamp()
            throws Exception
    {
        double current = TEST_SESSION.getStartTime() / 1000.0;
        assertOptimizedEquals("current_timestamp = from_unixtime(" + current + ")", "true");
        double future = current + TimeUnit.MINUTES.toSeconds(1);
        assertOptimizedEquals("current_timestamp > from_unixtime(" + future + ")", "false");
    }

    @Test
    public void testCastToString()
            throws Exception
    {
        // integer
        assertOptimizedEquals("cast(123 as VARCHAR(20))", "'123'");
        assertOptimizedEquals("cast(-123 as VARCHAR(20))", "'-123'");

        // bigint
        assertOptimizedEquals("cast(BIGINT '123' as VARCHAR)", "'123'");
        assertOptimizedEquals("cast(12300000000 as VARCHAR)", "'12300000000'");
        assertOptimizedEquals("cast(-12300000000 as VARCHAR)", "'-12300000000'");

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
        // integer
        assertOptimizedEquals("cast(123 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(-123 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(0 as BOOLEAN)", "false");

        // bigint
        assertOptimizedEquals("cast(12300000000 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(-12300000000 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(BIGINT '0' as BOOLEAN)", "false");

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
        assertOptimizedEquals("cast(123.45 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(-123.45 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(0.0 as BOOLEAN)", "false");
    }

    @Test
    public void testCastToBigint()
            throws Exception
    {
        // integer
        assertOptimizedEquals("cast(0 as BIGINT)", "0");
        assertOptimizedEquals("cast(123 as BIGINT)", "123");
        assertOptimizedEquals("cast(-123 as BIGINT)", "-123");

        // bigint
        assertOptimizedEquals("cast(BIGINT '0' as BIGINT)", "0");
        assertOptimizedEquals("cast(BIGINT '123' as BIGINT)", "123");
        assertOptimizedEquals("cast(BIGINT '-123' as BIGINT)", "-123");

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
    public void testCastToInteger()
            throws Exception
    {
        // integer
        assertOptimizedEquals("cast(0 as INTEGER)", "0");
        assertOptimizedEquals("cast(123 as INTEGER)", "123");
        assertOptimizedEquals("cast(-123 as INTEGER)", "-123");

        // bigint
        assertOptimizedEquals("cast(BIGINT '0' as INTEGER)", "0");
        assertOptimizedEquals("cast(BIGINT '123' as INTEGER)", "123");
        assertOptimizedEquals("cast(BIGINT '-123' as INTEGER)", "-123");

        // double
        assertOptimizedEquals("cast(123.0 as INTEGER)", "123");
        assertOptimizedEquals("cast(-123.0 as INTEGER)", "-123");
        assertOptimizedEquals("cast(123.456 as INTEGER)", "123");
        assertOptimizedEquals("cast(-123.456 as INTEGER)", "-123");

        // boolean
        assertOptimizedEquals("cast(true as INTEGER)", "1");
        assertOptimizedEquals("cast(false as INTEGER)", "0");

        // string
        assertOptimizedEquals("cast('123' as INTEGER)", "123");
        assertOptimizedEquals("cast('-123' as INTEGER)", "-123");

        // null
        assertOptimizedEquals("cast(null as INTEGER)", "null");
    }

    @Test
    public void testCastToDouble()
            throws Exception
    {
        // integer
        assertOptimizedEquals("cast(0 as DOUBLE)", "0.0");
        assertOptimizedEquals("cast(123 as DOUBLE)", "123.0");
        assertOptimizedEquals("cast(-123 as DOUBLE)", "-123.0");

        // bigint
        assertOptimizedEquals("cast(BIGINT '0' as DOUBLE)", "0.0");
        assertOptimizedEquals("cast(12300000000 as DOUBLE)", "12300000000.0");
        assertOptimizedEquals("cast(-12300000000 as DOUBLE)", "-12300000000.0");

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
        assertOptimizedEquals("cast(true as DOUBLE)", "1.0");
        assertOptimizedEquals("cast(false as DOUBLE)", "0.0");
    }

    @Test
    public void testCastOptimization()
            throws Exception
    {
        assertOptimizedEquals("cast(bound_integer as VARCHAR)", "'1234'");
        assertOptimizedEquals("cast(bound_long as VARCHAR)", "'1234'");
        assertOptimizedEquals("cast(bound_integer + 1 as VARCHAR)", "'1235'");
        assertOptimizedEquals("cast(bound_long + 1 as VARCHAR)", "'1235'");
        assertOptimizedEquals("cast(unbound_string as VARCHAR)", "cast(unbound_string as VARCHAR)");
    }

    @Test
    public void testTryCast()
    {
        assertOptimizedEquals("try_cast(null as BIGINT)", "null");
        assertOptimizedEquals("try_cast(123 as BIGINT)", "123");
        assertOptimizedEquals("try_cast(null as INTEGER)", "null");
        assertOptimizedEquals("try_cast(123 as INTEGER)", "123");
        assertOptimizedEquals("try_cast('foo' as VARCHAR)", "'foo'");
        assertOptimizedEquals("try_cast('foo' as BIGINT)", "null");
        assertOptimizedEquals("try_cast(unbound_string as BIGINT)", "try_cast(unbound_string as BIGINT)");
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
                        "when false then 10000000000 " +
                        "else 33 " +
                        "end",
                "33");

        assertOptimizedEquals("case " +
                        "when bound_long = 1234 then 33 " +
                        "end",
                "33");
        assertOptimizedEquals("case " +
                        "when true then bound_long " +
                        "end",
                "1234");
        assertOptimizedEquals("case " +
                        "when false then 1 " +
                        "else bound_long " +
                        "end",
                "1234");

        assertOptimizedEquals("case " +
                        "when bound_integer = 1234 then 33 " +
                        "end",
                "33");
        assertOptimizedEquals("case " +
                        "when true then bound_integer " +
                        "end",
                "1234");
        assertOptimizedEquals("case " +
                        "when false then 1 " +
                        "else bound_integer " +
                        "end",
                "1234");

        assertOptimizedEquals("case " +
                        "when bound_long = 1234 then 33 " +
                        "else unbound_long " +
                        "end",
                "33");
        assertOptimizedEquals("case " +
                        "when true then bound_long " +
                        "else unbound_long " +
                        "end",
                "1234");
        assertOptimizedEquals("case " +
                        "when false then unbound_long " +
                        "else bound_long " +
                        "end",
                "1234");

        assertOptimizedEquals("case " +
                        "when bound_integer = 1234 then 33 " +
                        "else unbound_integer " +
                        "end",
                "33");
        assertOptimizedEquals("case " +
                        "when true then bound_integer " +
                        "else unbound_integer " +
                        "end",
                "1234");
        assertOptimizedEquals("case " +
                        "when false then unbound_integer " +
                        "else bound_integer " +
                        "end",
                "1234");

        assertOptimizedEquals("case " +
                        "when unbound_long = 1234 then 33 " +
                        "else 1 " +
                        "end",
                "" +
                        "case " +
                        "when unbound_long = 1234 then 33 " +
                        "else 1 " +
                        "end");

        assertOptimizedMatches("case when 0 / 0 = 0 then 1 end",
                "case when cast(fail() as boolean) then 1 end");

        assertOptimizedMatches("if(false, 1, 0 / 0)", "cast(fail() as integer)");
    }

    @Test
    public void testSimpleCase()
            throws Exception
    {
        assertOptimizedEquals("case 1 " +
                        "when 1 then 32 + 1 " +
                        "when 1 then 34 " +
                        "end",
                "33");

        assertOptimizedEquals("case null " +
                        "when true then 33 " +
                        "end",
                "null");
        assertOptimizedEquals("case null " +
                        "when true then 33 " +
                        "else 33 " +
                        "end",
                "33");
        assertOptimizedEquals("case 33 " +
                        "when null then 1 " +
                        "else 33 " +
                        "end",
                "33");

        assertOptimizedEquals("case null " +
                        "when true then 3300000000 " +
                        "end",
                "null");
        assertOptimizedEquals("case null " +
                        "when true then 3300000000 " +
                        "else 3300000000 " +
                        "end",
                "3300000000");
        assertOptimizedEquals("case 33 " +
                        "when null then 3300000000 " +
                        "else 33 " +
                        "end",
                "33");

        assertOptimizedEquals("case true " +
                        "when true then 33 " +
                        "end",
                "33");
        assertOptimizedEquals("case true " +
                        "when false then 1 " +
                        "else 33 end",
                "33");

        assertOptimizedEquals("case bound_long " +
                        "when 1234 then 33 " +
                        "end",
                "33");
        assertOptimizedEquals("case 1234 " +
                        "when bound_long then 33 " +
                        "end",
                "33");
        assertOptimizedEquals("case true " +
                        "when true then bound_long " +
                        "end",
                "1234");
        assertOptimizedEquals("case true " +
                        "when false then 1 " +
                        "else bound_long " +
                        "end",
                "1234");

        assertOptimizedEquals("case bound_integer " +
                        "when 1234 then 33 " +
                        "end",
                "33");
        assertOptimizedEquals("case 1234 " +
                        "when bound_integer then 33 " +
                        "end",
                "33");
        assertOptimizedEquals("case true " +
                        "when true then bound_integer " +
                        "end",
                "1234");
        assertOptimizedEquals("case true " +
                        "when false then 1 " +
                        "else bound_integer " +
                        "end",
                "1234");

        assertOptimizedEquals("case bound_long " +
                        "when 1234 then 33 " +
                        "else unbound_long " +
                        "end",
                "33");
        assertOptimizedEquals("case true " +
                        "when true then bound_long " +
                        "else unbound_long " +
                        "end",
                "1234");
        assertOptimizedEquals("case true " +
                        "when false then unbound_long " +
                        "else bound_long " +
                        "end",
                "1234");

        assertOptimizedEquals("case unbound_long " +
                        "when 1234 then 33 " +
                        "else 1 " +
                        "end",
                "" +
                        "case unbound_long " +
                        "when 1234 then 33 " +
                        "else 1 " +
                        "end");

        assertOptimizedEquals("case 33 " +
                        "when 0 then 0 " +
                        "when 33 then unbound_long " +
                        "else 1 " +
                        "end",
                "unbound_long");
        assertOptimizedEquals("case 33 " +
                        "when 0 then 0 " +
                        "when 33 then 1 " +
                        "when unbound_long then 2 " +
                        "else 1 " +
                        "end",
                "1");
        assertOptimizedEquals("case 33 " +
                        "when unbound_long then 0 " +
                        "when 1 then 1 " +
                        "when 33 then 2 " +
                        "else 0 " +
                        "end",
                "case 33 " +
                        "when unbound_long then 0 " +
                        "else 2 " +
                        "end");
        assertOptimizedEquals("case 33 " +
                        "when 0 then 0 " +
                        "when 1 then 1 " +
                        "else unbound_long " +
                        "end",
                "unbound_long");
        assertOptimizedEquals("case 33 " +
                        "when unbound_long then 0 " +
                        "when 1 then 1 " +
                        "when unbound_long2 then 2 " +
                        "else 3 " +
                        "end",
                "case 33 " +
                        "when unbound_long then 0 " +
                        "when unbound_long2 then 2 " +
                        "else 3 " +
                        "end");

        assertOptimizedEquals("case true " +
                        "when unbound_long = 1 then 1 " +
                        "when 0 / 0 = 0 then 2 " +
                        "else 33 end",
                "" +
                        "case true " +
                        "when unbound_long = 1 then 1 " +
                        "when 0 / 0 = 0 then 2 else 33 " +
                        "end");

        assertOptimizedEquals("case bound_long " +
                        "when 123 * 10 + unbound_long then 1 = 1 " +
                        "else 1 = 2 " +
                        "end",
                "" +
                        "case bound_long when 1230 + unbound_long then true " +
                        "else false " +
                        "end");

        assertOptimizedEquals("case bound_long " +
                        "when unbound_long then 2 + 2 " +
                        "end",
                "" +
                        "case bound_long " +
                        "when unbound_long then 4 " +
                        "end");

        assertOptimizedEquals("case bound_long " +
                        "when unbound_long then 2 + 2 " +
                        "when 1 then null " +
                        "when 2 then null " +
                        "end",
                "" +
                        "case bound_long " +
                        "when unbound_long then 4 " +
                        "end");

        assertOptimizedMatches("case 1 " +
                        "when unbound_long then 1 " +
                        "when 0 / 0 then 2 " +
                        "else 1 " +
                        "end",
                "" +
                        "case BIGINT '1' " +
                        "when unbound_long then 1 " +
                        "when cast(fail() AS integer) then 2 " +
                        "else 1 " +
                        "end");

        assertOptimizedMatches("case 1 " +
                        "when 0 / 0 then 1 " +
                        "when 0 / 0 then 2 " +
                        "else 1 " +
                        "end",
                "" +
                        "case 1 " +
                        "when cast(fail() as integer) then 1 " +
                        "when cast(fail() as integer) then 2 " +
                        "else 1 " +
                        "end");
    }

    @Test
    public void testCoalesce()
            throws Exception
    {
        assertOptimizedEquals("coalesce(2 * 3 * unbound_long, 1 - 1, null)", "coalesce(6 * unbound_long, 0)");
        assertOptimizedEquals("coalesce(2 * 3 * unbound_long, 1.0/2.0, null)", "coalesce(6 * unbound_long, 0.5)");
        assertOptimizedEquals("coalesce(unbound_long, 2, 1.0/2.0, 12.34, null)", "coalesce(unbound_long, 2.0, 0.5, 12.34)");
        assertOptimizedEquals("coalesce(2 * 3 * unbound_integer, 1 - 1, null)", "coalesce(6 * unbound_integer, 0)");
        assertOptimizedEquals("coalesce(2 * 3 * unbound_integer, 1.0/2.0, null)", "coalesce(6 * unbound_integer, 0.5)");
        assertOptimizedEquals("coalesce(unbound_integer, 2, 1.0/2.0, 12.34, null)", "coalesce(unbound_integer, 2.0, 0.5, 12.34)");
        assertOptimizedMatches("coalesce(0 / 0 > 1, unbound_boolean, 0 / 0 = 0)",
                "coalesce(cast(fail() as boolean), unbound_boolean, cast(fail() as boolean))");
    }

    @Test
    public void testIf()
            throws Exception
    {
        assertOptimizedEquals("IF(2 = 2, 3, 4)", "3");
        assertOptimizedEquals("IF(1 = 2, 3, 4)", "4");
        assertOptimizedEquals("IF(1 = 2, BIGINT '3', 4)", "4");
        assertOptimizedEquals("IF(1 = 2, 3000000000, 4)", "4");

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

        // todo optimize case statement
        assertOptimizedEquals("IF(unbound_boolean, 1 + 2, 3 + 4)", "CASE WHEN unbound_boolean THEN (1 + 2) ELSE (3 + 4) END");
        assertOptimizedEquals("IF(unbound_boolean, BIGINT '1' + 2, 3 + 4)", "CASE WHEN unbound_boolean THEN (BIGINT '1' + 2) ELSE (3 + 4) END");
    }

    @Test
    public void testTry()
            throws Exception
    {
        assertOptimizedEquals("TRY(2/1)", "2");
        assertOptimizedEquals("TRY(2/0)", "null");
        assertOptimizedEquals("COALESCE(TRY(2/0), 0)", "0");

        assertOptimizedEquals("TRY(CAST (CAST (bound_long AS VARCHAR) AS BIGINT))", "bound_long");
        assertOptimizedEquals("TRY(CAST (CAST (bound_integer AS VARCHAR) AS INTEGER))", "bound_integer");
        assertOptimizedEquals("TRY(CAST (CONCAT('a', CAST (bound_long AS VARCHAR)) AS BIGINT))", "null");
        assertOptimizedEquals("COALESCE(TRY(CAST (CONCAT('a', CAST (bound_long AS VARCHAR)) AS BIGINT)), 0)", "0");

        assertOptimizedEquals("TRY(ABS(-2))", "2");
        assertOptimizedEquals("42 + TRY(ABS(-9223372036854775807 - 1))", "null");

        assertOptimizedEquals("JSON_FORMAT(TRY(JSON '[]')) || unbound_string", "'[]' || unbound_string");
        assertOptimizedEquals("JSON_FORMAT(TRY(JSON 'INVALID')) || unbound_string", "null");
    }

    @Test
    public void testLike()
            throws Exception
    {
        assertOptimizedEquals("'a' LIKE 'a'", "true");
        assertOptimizedEquals("'' LIKE 'a'", "false");
        assertOptimizedEquals("'abc' LIKE 'a'", "false");

        assertOptimizedEquals("'a' LIKE '_'", "true");
        assertOptimizedEquals("'' LIKE '_'", "false");
        assertOptimizedEquals("'abc' LIKE '_'", "false");

        assertOptimizedEquals("'a' LIKE '%'", "true");
        assertOptimizedEquals("'' LIKE '%'", "true");
        assertOptimizedEquals("'abc' LIKE '%'", "true");

        assertOptimizedEquals("'abc' LIKE '___'", "true");
        assertOptimizedEquals("'ab' LIKE '___'", "false");
        assertOptimizedEquals("'abcd' LIKE '___'", "false");

        assertOptimizedEquals("'abc' LIKE 'abc'", "true");
        assertOptimizedEquals("'xyz' LIKE 'abc'", "false");
        assertOptimizedEquals("'abc0' LIKE 'abc'", "false");
        assertOptimizedEquals("'0abc' LIKE 'abc'", "false");

        assertOptimizedEquals("'abc' LIKE 'abc%'", "true");
        assertOptimizedEquals("'abc0' LIKE 'abc%'", "true");
        assertOptimizedEquals("'0abc' LIKE 'abc%'", "false");

        assertOptimizedEquals("'abc' LIKE '%abc'", "true");
        assertOptimizedEquals("'0abc' LIKE '%abc'", "true");
        assertOptimizedEquals("'abc0' LIKE '%abc'", "false");

        assertOptimizedEquals("'abc' LIKE '%abc%'", "true");
        assertOptimizedEquals("'0abc' LIKE '%abc%'", "true");
        assertOptimizedEquals("'abc0' LIKE '%abc%'", "true");
        assertOptimizedEquals("'0abc0' LIKE '%abc%'", "true");
        assertOptimizedEquals("'xyzw' LIKE '%abc%'", "false");

        assertOptimizedEquals("'abc' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0abc' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'abc0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0abc0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'ab01c' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0ab01c' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'ab01c0' LIKE '%ab%c%'", "true");
        assertOptimizedEquals("'0ab01c0' LIKE '%ab%c%'", "true");

        assertOptimizedEquals("'xyzw' LIKE '%ab%c%'", "false");

        // ensure regex chars are escaped
        assertOptimizedEquals("'\' LIKE '\'", "true");
        assertOptimizedEquals("'.*' LIKE '.*'", "true");
        assertOptimizedEquals("'[' LIKE '['", "true");
        assertOptimizedEquals("']' LIKE ']'", "true");
        assertOptimizedEquals("'{' LIKE '{'", "true");
        assertOptimizedEquals("'}' LIKE '}'", "true");
        assertOptimizedEquals("'?' LIKE '?'", "true");
        assertOptimizedEquals("'+' LIKE '+'", "true");
        assertOptimizedEquals("'(' LIKE '('", "true");
        assertOptimizedEquals("')' LIKE ')'", "true");
        assertOptimizedEquals("'|' LIKE '|'", "true");
        assertOptimizedEquals("'^' LIKE '^'", "true");
        assertOptimizedEquals("'$' LIKE '$'", "true");

        assertOptimizedEquals("null LIKE '%'", "null");
        assertOptimizedEquals("'a' LIKE null", "null");
        assertOptimizedEquals("'a' LIKE '%' ESCAPE null", "null");

        assertOptimizedEquals("'%' LIKE 'z%' ESCAPE 'z'", "true");
    }

    @Test
    public void testLikeOptimization()
            throws Exception
    {
        assertOptimizedEquals("unbound_string LIKE 'abc'", "unbound_string = CAST('abc' AS VARCHAR)");

        assertOptimizedEquals("unbound_string LIKE '' ESCAPE '#'", "unbound_string LIKE '' ESCAPE '#'");
        assertOptimizedEquals("unbound_string LIKE 'abc' ESCAPE '#'", "unbound_string = CAST('abc' AS VARCHAR)");
        assertOptimizedEquals("unbound_string LIKE 'a#_b' ESCAPE '#'", "unbound_string = CAST('a_b' AS VARCHAR)");
        assertOptimizedEquals("unbound_string LIKE 'a#%b' ESCAPE '#'", "unbound_string = CAST('a%b' AS VARCHAR)");
        assertOptimizedEquals("unbound_string LIKE 'a#_##b' ESCAPE '#'", "unbound_string = CAST('a_#b' AS VARCHAR)");
        assertOptimizedEquals("unbound_string LIKE 'a#__b' ESCAPE '#'", "unbound_string LIKE 'a#__b' ESCAPE '#'");
        assertOptimizedEquals("unbound_string LIKE 'a##%b' ESCAPE '#'", "unbound_string LIKE 'a##%b' ESCAPE '#'");

        assertOptimizedEquals("bound_string LIKE bound_pattern", "true");
        assertOptimizedEquals("'abc' LIKE bound_pattern", "false");

        assertOptimizedEquals("unbound_string LIKE bound_pattern", "unbound_string LIKE bound_pattern");

        assertOptimizedEquals("unbound_string LIKE unbound_pattern ESCAPE unbound_string", "unbound_string LIKE unbound_pattern ESCAPE unbound_string");
    }

    @Test
    public void testInvalidLike()
    {
        assertThrows(PrestoException.class, () -> optimize("unbound_string LIKE 'abc' ESCAPE ''"));
        assertThrows(PrestoException.class, () -> optimize("unbound_string LIKE 'abc' ESCAPE 'bc'"));
        assertThrows(PrestoException.class, () -> optimize("unbound_string LIKE '#' ESCAPE '#'"));
        assertThrows(PrestoException.class, () -> optimize("unbound_string LIKE '#abc' ESCAPE '#'"));
        assertThrows(PrestoException.class, () -> optimize("unbound_string LIKE 'ab#' ESCAPE '#'"));
    }

    @Test
    public void testFailedExpressionOptimization()
            throws Exception
    {
        assertOptimizedEquals("if(unbound_boolean, 1, 0 / 0)", "CASE WHEN unbound_boolean THEN 1 ELSE 0 / 0 END");
        assertOptimizedEquals("if(unbound_boolean, 0 / 0, 1)", "CASE WHEN unbound_boolean THEN 0 / 0 ELSE 1 END");

        assertOptimizedMatches("CASE unbound_long WHEN 1 THEN 1 WHEN 0 / 0 THEN 2 END",
                "CASE unbound_long WHEN BIGINT '1' THEN 1 WHEN cast(fail() as bigint) THEN 2 END");

        assertOptimizedMatches("CASE unbound_boolean WHEN true THEN 1 ELSE 0 / 0 END",
                "CASE unbound_boolean WHEN true THEN 1 ELSE cast(fail() as integer) END");

        assertOptimizedMatches("CASE bound_long WHEN unbound_long THEN 1 WHEN 0 / 0 THEN 2 ELSE 1 END",
                "CASE BIGINT '1234' WHEN unbound_long THEN 1 WHEN cast(fail() as bigint) THEN 2 ELSE 1 END");

        assertOptimizedMatches("case when unbound_boolean then 1 when 0 / 0 = 0 then 2 end",
                "case when unbound_boolean then 1 when cast(fail() as boolean) then 2 end");

        assertOptimizedMatches("case when unbound_boolean then 1 else 0 / 0  end",
                "case when unbound_boolean then 1 else cast(fail() as integer) end");

        assertOptimizedMatches("case when unbound_boolean then 0 / 0 else 1 end",
                "case when unbound_boolean then cast(fail() as integer) else 1 end");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testOptimizeDivideByZero()
            throws Exception
    {
        optimize("0 / 0");
    }

    @Test
    public void testMassiveArrayConstructor()
    {
        optimize(format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "(bound_long + " + i + ")").iterator())));
        optimize(format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "(bound_integer + " + i + ")").iterator())));
        optimize(format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "'" + i + "'").iterator())));
        optimize(format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(0, 10_000).mapToObj(i -> "ARRAY['" + i + "']").iterator())));
    }

    @Test
    public void testArrayConstructor()
    {
        optimize("ARRAY []");
        assertOptimizedEquals("ARRAY [(unbound_long + 0), (unbound_long + 1), (unbound_long + 2)]",
                "array_constructor((unbound_long + 0), (unbound_long + 1), (unbound_long + 2))");
        assertOptimizedEquals("ARRAY [(bound_long + 0), (unbound_long + 1), (bound_long + 2)]",
                "array_constructor((bound_long + 0), (unbound_long + 1), (bound_long + 2))");
        assertOptimizedEquals("ARRAY [(bound_long + 0), (unbound_long + 1), NULL]",
                "array_constructor((bound_long + 0), (unbound_long + 1), NULL)");
    }

    @Test
    public void testRowConstructor()
    {
        optimize("ROW(NULL)");
        optimize("ROW(1)");
        optimize("ROW(unbound_long + 0)");
        optimize("ROW(unbound_long + unbound_long2, unbound_string, unbound_double)");
        optimize("ROW(unbound_boolean, FALSE, ARRAY[unbound_long, unbound_long2], unbound_null_string, unbound_interval)");
        optimize("ARRAY [ROW(unbound_string, unbound_double), ROW(unbound_string, 0.0)]");
        optimize("ARRAY [ROW('string', unbound_double), ROW('string', bound_double)]");
        optimize("ROW(ROW(NULL), ROW(ROW(ROW(ROW('rowception')))))");
        optimize("ROW(unbound_string, bound_string)");

        optimize("ARRAY [ROW(unbound_string, unbound_double), ROW(CAST(bound_string AS VARCHAR), 0.0)]");
        optimize("ARRAY [ROW(CAST(bound_string AS VARCHAR), 0.0), ROW(unbound_string, unbound_double)]");

        optimize("ARRAY [ROW(unbound_string, unbound_double), CAST(NULL AS ROW(VARCHAR, DOUBLE))]");
        optimize("ARRAY [CAST(NULL AS ROW(VARCHAR, DOUBLE)), ROW(unbound_string, unbound_double)]");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testArraySubscriptConstantNegativeIndex()
    {
        optimize("ARRAY [1, 2, 3][-1]");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testArraySubscriptConstantZeroIndex()
    {
        optimize("ARRAY [1, 2, 3][0]");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testMapSubscriptMissingKey()
    {
        optimize("MAP(ARRAY [1, 2], ARRAY [3, 4])[-1]");
    }

    @Test
    public void testMapSubscriptConstantIndexes()
    {
        optimize("MAP(ARRAY [1, 2], ARRAY [3, 4])[1]");
        optimize("MAP(ARRAY [BIGINT '1', 2], ARRAY [3, 4])[1]");
        optimize("MAP(ARRAY [1, 2], ARRAY [3, 4])[2]");
        optimize("MAP(ARRAY [ARRAY[1,1]], ARRAY['a'])[ARRAY[1,1]]");
    }

    @Test(timeOut = 60000)
    public void testLikeInvalidUtf8()
    {
        assertLike(new byte[] {'a', 'b', 'c'}, "%b%", true);
        assertLike(new byte[] {'a', 'b', 'c', (byte) 0xFF, 'x', 'y'}, "%b%", true);
    }

    @Test
    public void testLiterals()
    {
        optimize("date '2013-04-03' + unbound_interval");
        optimize("time '03:04:05.321' + unbound_interval");
        optimize("time '03:04:05.321 UTC' + unbound_interval");
        optimize("timestamp '2013-04-03 03:04:05.321' + unbound_interval");
        optimize("timestamp '2013-04-03 03:04:05.321 UTC' + unbound_interval");

        optimize("interval '3' day * unbound_long");
        optimize("interval '3' year * unbound_long");

        assertEquals(optimize("X'1234'"), Slices.wrappedBuffer((byte) 0x12, (byte) 0x34));
    }

    private static void assertLike(byte[] value, String pattern, boolean expected)
    {
        Expression predicate = new LikePredicate(
                rawStringLiteral(Slices.wrappedBuffer(value)),
                new StringLiteral(pattern),
                null);
        assertEquals(evaluate(predicate), expected);
    }

    private static StringLiteral rawStringLiteral(final Slice slice)
    {
        return new StringLiteral(slice.toStringUtf8())
        {
            @Override
            public Slice getSlice()
            {
                return slice;
            }
        };
    }

    private static void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertEquals(optimize(actual), optimize(expected));
    }

    private static void assertOptimizedMatches(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        // replaces FunctionCalls to FailureFunction by fail()
        Object actualOptimized = optimize(actual);
        if (actualOptimized instanceof Expression) {
            actualOptimized = ExpressionTreeRewriter.rewriteWith(new FailedFunctionRewriter(), (Expression) actualOptimized);
        }
        assertEquals(
                actualOptimized,
                rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expected)));
    }

    private static Object optimize(@Language("SQL") String expression)
    {
        assertRoundTrip(expression);

        Expression parsedExpression = FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);

        IdentityLinkedHashMap<Expression, Type> expressionTypes = getExpressionTypes(TEST_SESSION, METADATA, SQL_PARSER, SYMBOL_TYPES, parsedExpression, emptyList());
        ExpressionInterpreter interpreter = expressionOptimizer(parsedExpression, METADATA, TEST_SESSION, expressionTypes);
        return interpreter.optimize(symbol -> {
            switch (symbol.getName().toLowerCase(ENGLISH)) {
                case "bound_integer":
                    return 1234L;
                case "bound_long":
                    return 1234L;
                case "bound_string":
                    return utf8Slice("hello");
                case "bound_double":
                    return 12.34;
                case "bound_date":
                    return new LocalDate(2001, 8, 22).toDateMidnight(DateTimeZone.UTC).getMillis();
                case "bound_time":
                    return new LocalTime(3, 4, 5, 321).toDateTime(new DateTime(0, DateTimeZone.UTC)).getMillis();
                case "bound_timestamp":
                    return new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis();
                case "bound_pattern":
                    return utf8Slice("%el%");
                case "bound_timestamp_with_timezone":
                    return new SqlTimestampWithTimeZone(new DateTime(1970, 1, 1, 1, 0, 0, 999, DateTimeZone.UTC).getMillis(), getTimeZoneKey("Z"));
                case "bound_varbinary":
                    return Slices.wrappedBuffer((byte) 0xab);
            }

            return symbol.toSymbolReference();
        });
    }

    private static Object evaluate(String expression)
    {
        assertRoundTrip(expression);

        Expression parsedExpression = FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);

        return evaluate(parsedExpression);
    }

    private static void assertRoundTrip(String expression)
    {
        assertEquals(SQL_PARSER.createExpression(expression),
                SQL_PARSER.createExpression(formatExpression(SQL_PARSER.createExpression(expression), Optional.empty())));
    }

    private static Object evaluate(Expression expression)
    {
        IdentityLinkedHashMap<Expression, Type> expressionTypes = getExpressionTypes(TEST_SESSION, METADATA, SQL_PARSER, SYMBOL_TYPES, expression, emptyList());
        ExpressionInterpreter interpreter = expressionInterpreter(expression, METADATA, TEST_SESSION, expressionTypes);

        return interpreter.evaluate((RecordCursor) null);
    }

    private static class FailedFunctionRewriter
            extends ExpressionRewriter<Object>
    {
        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Object context, ExpressionTreeRewriter<Object> treeRewriter)
        {
            if (node.getName().equals(QualifiedName.of("fail"))) {
                return new FunctionCall(QualifiedName.of("fail"), ImmutableList.of());
            }
            return node;
        }
    }
}
