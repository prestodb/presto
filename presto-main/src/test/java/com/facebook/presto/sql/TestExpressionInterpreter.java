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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.BlockSerdeUtil;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.SERIALIZABLE;
import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionInterpreter;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.expressionOptimizer;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.rowExpressionInterpreter;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
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
    private static final TypeProvider SYMBOL_TYPES = TypeProvider.viewOf(ImmutableMap.<String, Type>builder()
            .put("bound_integer", INTEGER)
            .put("bound_long", BIGINT)
            .put("bound_string", createVarcharType(TEST_VARCHAR_TYPE_LENGTH))
            .put("bound_varbinary", VarbinaryType.VARBINARY)
            .put("bound_double", DOUBLE)
            .put("bound_boolean", BOOLEAN)
            .put("bound_date", DATE)
            .put("bound_time", TIME)
            .put("bound_timestamp", TIMESTAMP)
            .put("bound_pattern", VARCHAR)
            .put("bound_null_string", VARCHAR)
            .put("bound_decimal_short", createDecimalType(5, 2))
            .put("bound_decimal_long", createDecimalType(23, 3))
            .put("time", BIGINT) // for testing reserved identifiers
            .put("unbound_integer", INTEGER)
            .put("unbound_long", BIGINT)
            .put("unbound_long2", BIGINT)
            .put("unbound_long3", BIGINT)
            .put("unbound_string", VARCHAR)
            .put("unbound_double", DOUBLE)
            .put("unbound_boolean", BOOLEAN)
            .put("unbound_date", DATE)
            .put("unbound_time", TIME)
            .put("unbound_array", new ArrayType(BIGINT))
            .put("unbound_timestamp", TIMESTAMP)
            .put("unbound_interval", INTERVAL_DAY_TIME)
            .put("unbound_pattern", VARCHAR)
            .put("unbound_null_string", VARCHAR)
            .build());

    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);
    private static final BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager();

    @BeforeClass
    public void setup()
    {
        METADATA.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of(APPLY_FUNCTION));
    }

    @Test
    public void testAnd()
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
        assertOptimizedEquals("random() > 0 and random() > 0", "random() > 0 and random() > 0");
    }

    @Test
    public void testOr()
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
        assertOptimizedEquals("random() > 0 or random() > 0", "random() > 0 or random() > 0");
    }

    @Test
    public void testComparison()
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

        assertOptimizedEquals("1.1 = 1.1", "true");
        assertOptimizedEquals("9876543210.9874561203 = 9876543210.9874561203", "true");
        assertOptimizedEquals("bound_decimal_short = 123.45", "true");
        assertOptimizedEquals("bound_decimal_long = 12345678901234567890.123", "true");
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertOptimizedEquals("null is distinct from null", "false");

        assertOptimizedEquals("3 is distinct from 4", "true");
        assertOptimizedEquals("3 is distinct from BIGINT '4'", "true");
        assertOptimizedEquals("3 is distinct from 4000000000", "true");
        assertOptimizedEquals("3 is distinct from 3", "false");
        assertOptimizedEquals("3 is distinct from null", "true");
        assertOptimizedEquals("null is distinct from 3", "true");

        assertOptimizedEquals("10151082135029368 is distinct from 10151082135029369", "true");

        assertOptimizedEquals("1.1 is distinct from 1.1", "false");
        assertOptimizedEquals("9876543210.9874561203 is distinct from NULL", "true");
        assertOptimizedEquals("bound_decimal_short is distinct from NULL", "true");
        assertOptimizedEquals("bound_decimal_long is distinct from 12345678901234567890.123", "false");
    }

    @Test
    public void testIsNull()
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
        assertOptimizedEquals("1.1 is null", "false");
        assertOptimizedEquals("9876543210.9874561203 is null", "false");
        assertOptimizedEquals("bound_decimal_short is null", "false");
        assertOptimizedEquals("bound_decimal_long is null", "false");
    }

    @Test
    public void testIsNotNull()
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
        assertOptimizedEquals("1.1 is not null", "true");
        assertOptimizedEquals("9876543210.9874561203 is not null", "true");
        assertOptimizedEquals("bound_decimal_short is not null", "true");
        assertOptimizedEquals("bound_decimal_long is not null", "true");
    }

    @Test
    public void testNullIf()
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
        assertOptimizedEquals("nullif(1.0E0, 1)", "null");
        assertOptimizedEquals("nullif(10000000000.0E0, 10000000000)", "null");
        assertOptimizedEquals("nullif(1.1E0, 1)", "1.1E0");
        assertOptimizedEquals("nullif(1.1E0, 1.1E0)", "null");
        assertOptimizedEquals("nullif(1, 2-1)", "null");
        assertOptimizedEquals("nullif(null, null)", "null");
        assertOptimizedEquals("nullif(1, null)", "1");
        assertOptimizedEquals("nullif(unbound_long, 1)", "nullif(unbound_long, 1)");
        assertOptimizedEquals("nullif(unbound_long, unbound_long2)", "nullif(unbound_long, unbound_long2)");
        assertOptimizedEquals("nullif(unbound_long, unbound_long2+(1+1))", "nullif(unbound_long, unbound_long2+2)");

        assertOptimizedEquals("nullif(1.1, 1.2)", "1.1");
        assertOptimizedEquals("nullif(9876543210.9874561203, 9876543210.9874561203)", "null");
        assertOptimizedEquals("nullif(bound_decimal_short, 123.45)", "null");
        assertOptimizedEquals("nullif(bound_decimal_long, 12345678901234567890.123)", "null");
        assertOptimizedEquals("nullif(ARRAY[CAST(1 AS BIGINT)], ARRAY[CAST(1 AS BIGINT)]) IS NULL", "true");
        assertOptimizedEquals("nullif(ARRAY[CAST(1 AS BIGINT)], ARRAY[CAST(NULL AS BIGINT)]) IS NULL", "false");
        assertOptimizedEquals("nullif(ARRAY[CAST(NULL AS BIGINT)], ARRAY[CAST(NULL AS BIGINT)]) IS NULL", "false");
    }

    @Test
    public void testNegative()
    {
        assertOptimizedEquals("-(1)", "-1");
        assertOptimizedEquals("-(BIGINT '1')", "BIGINT '-1'");
        assertOptimizedEquals("-(unbound_long+1)", "-(unbound_long+1)");
        assertOptimizedEquals("-(1+1)", "-2");
        assertOptimizedEquals("-(1+ BIGINT '1')", "BIGINT '-2'");
        assertOptimizedEquals("-(CAST(NULL AS BIGINT))", "null");
        assertOptimizedEquals("-(unbound_long+(1+1))", "-(unbound_long+2)");
        assertOptimizedEquals("-(1.1+1.2)", "-2.3");
        assertOptimizedEquals("-(9876543210.9874561203-9876543210.9874561203)", "CAST(0 AS DECIMAL(20,10))");
        assertOptimizedEquals("-(bound_decimal_short+123.45)", "-246.90");
        assertOptimizedEquals("-(bound_decimal_long-12345678901234567890.123)", "CAST(0 AS DECIMAL(20,10))");
    }

    @Test
    public void testNot()
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
    {
        assertOptimizedEquals("abs(-5)", "5");
        assertOptimizedEquals("abs(-10-5)", "15");
        assertOptimizedEquals("abs(-bound_integer + 1)", "1233");
        assertOptimizedEquals("abs(-bound_long + 1)", "1233");
        assertOptimizedEquals("abs(-bound_long + BIGINT '1')", "1233");
        assertOptimizedEquals("abs(-bound_long)", "1234");
        assertOptimizedEquals("abs(unbound_long)", "abs(unbound_long)");
        assertOptimizedEquals("abs(unbound_long + 1)", "abs(unbound_long + 1)");
        assertOptimizedEquals("cast(json_parse(unbound_string) as map(varchar, varchar))", "cast(json_parse(unbound_string) as map(varchar, varchar))");
        assertOptimizedEquals("cast(json_parse(unbound_string) as array(varchar))", "cast(json_parse(unbound_string) as array(varchar))");
        assertOptimizedEquals("cast(json_parse(unbound_string) as row(bigint, varchar))", "cast(json_parse(unbound_string) as row(bigint, varchar))");
    }

    @Test
    public void testNonDeterministicFunctionCall()
    {
        // optimize should do nothing
        assertOptimizedEquals("random()", "random()");

        // evaluate should execute
        Object value = evaluate("random()", false);
        assertTrue(value instanceof Double);
        double randomValue = (double) value;
        assertTrue(0 <= randomValue && randomValue < 1);
    }

    @Test
    public void testBetween()
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

        assertOptimizedEquals("1.15 between 1.1 and 1.2", "true");
        assertOptimizedEquals("9876543210.98745612035 between 9876543210.9874561203 and 9876543210.9874561204", "true");
        assertOptimizedEquals("123.455 between bound_decimal_short and 123.46", "true");
        assertOptimizedEquals("12345678901234567890.1235 between bound_decimal_long and 12345678901234567890.123", "false");
    }

    @Test
    public void testExtract()
    {
        DateTime dateTime = new DateTime(2001, 8, 22, 3, 4, 5, 321, getDateTimeZone(TEST_SESSION.getTimeZoneKey()));
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
        assertOptimizedEquals("extract (DOW from bound_timestamp)", "2");
        assertOptimizedEquals("extract (DOY from bound_timestamp)", "233");
        assertOptimizedEquals("extract (DAY from bound_timestamp)", "21");
        assertOptimizedEquals("extract (HOUR from bound_timestamp)", "16");
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

        assertOptimizedEquals("1.15 in (1.1, 1.2, 1.3, 1.15)", "true");
        assertOptimizedEquals("9876543210.98745612035 in (9876543210.9874561203, 9876543210.9874561204, 9876543210.98745612035)", "true");
        assertOptimizedEquals("bound_decimal_short in (123.455, 123.46, 123.45)", "true");
        assertOptimizedEquals("bound_decimal_long in (12345678901234567890.123, 9876543210.9874561204, 9876543210.98745612035)", "true");
        assertOptimizedEquals("bound_decimal_long in (9876543210.9874561204, null, 9876543210.98745612035)", "null");
    }

    @Test
    public void testInComplexTypes()
    {
        assertEvaluatedEquals("ARRAY[null] IN (ARRAY[null])", "null");
        assertEvaluatedEquals("ARRAY[1] IN (ARRAY[null])", "null");
        assertEvaluatedEquals("ARRAY[null] IN (ARRAY[1])", "null");
        assertEvaluatedEquals("ARRAY[1, null] IN (ARRAY[1, null])", "null");
        assertEvaluatedEquals("ARRAY[1, null] IN (ARRAY[2, null])", "false");
        assertEvaluatedEquals("ARRAY[1, null] IN (ARRAY[1, null], ARRAY[2, null])", "null");
        assertEvaluatedEquals("ARRAY[1, null] IN (ARRAY[1, null], ARRAY[2, null], ARRAY[1, null])", "null");
        assertEvaluatedEquals("ARRAY[ARRAY[1, 2], ARRAY[3, 4]] in (ARRAY[ARRAY[1, 2], ARRAY[3, NULL]])", "null");

        assertEvaluatedEquals("ROW(1) IN (ROW(1))", "true");
        assertEvaluatedEquals("ROW(1) IN (ROW(2))", "false");
        assertEvaluatedEquals("ROW(1) IN (ROW(2), ROW(1), ROW(2))", "true");
        assertEvaluatedEquals("ROW(1) IN (null)", "null");
        assertEvaluatedEquals("ROW(1) IN (null, ROW(1))", "true");
        assertEvaluatedEquals("ROW(1, null) IN (ROW(2, null), null)", "null");
        assertEvaluatedEquals("ROW(null) IN (ROW(null))", "null");
        assertEvaluatedEquals("ROW(1) IN (ROW(null))", "null");
        assertEvaluatedEquals("ROW(null) IN (ROW(1))", "null");
        assertEvaluatedEquals("ROW(1, null) IN (ROW(1, null))", "null");
        assertEvaluatedEquals("ROW(1, null) IN (ROW(2, null))", "false");
        assertEvaluatedEquals("ROW(1, null) IN (ROW(1, null), ROW(2, null))", "null");
        assertEvaluatedEquals("ROW(1, null) IN (ROW(1, null), ROW(2, null), ROW(1, null))", "null");

        assertEvaluatedEquals("MAP(ARRAY[1], ARRAY[1]) IN (MAP(ARRAY[1], ARRAY[1]))", "true");
        assertEvaluatedEquals("MAP(ARRAY[1], ARRAY[1]) IN (null)", "null");
        assertEvaluatedEquals("MAP(ARRAY[1], ARRAY[1]) IN (null, MAP(ARRAY[1], ARRAY[1]))", "true");
        assertEvaluatedEquals("MAP(ARRAY[1], ARRAY[1]) IN (MAP(ARRAY[1, 2], ARRAY[1, null]))", "false");
        assertEvaluatedEquals("MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 2], ARRAY[2, null]), null)", "null");
        assertEvaluatedEquals("MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 2], ARRAY[1, null]))", "null");
        assertEvaluatedEquals("MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 3], ARRAY[1, null]))", "false");
        assertEvaluatedEquals("MAP(ARRAY[1], ARRAY[null]) IN (MAP(ARRAY[1], ARRAY[null]))", "null");
        assertEvaluatedEquals("MAP(ARRAY[1], ARRAY[1]) IN (MAP(ARRAY[1], ARRAY[null]))", "null");
        assertEvaluatedEquals("MAP(ARRAY[1], ARRAY[null]) IN (MAP(ARRAY[1], ARRAY[1]))", "null");
        assertEvaluatedEquals("MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 2], ARRAY[1, null]))", "null");
        assertEvaluatedEquals("MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 3], ARRAY[1, null]))", "false");
        assertEvaluatedEquals("MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 2], ARRAY[2, null]))", "false");
        assertEvaluatedEquals("MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 2], ARRAY[1, null]), MAP(ARRAY[1, 2], ARRAY[2, null]))", "null");
        assertEvaluatedEquals("MAP(ARRAY[1, 2], ARRAY[1, null]) IN (MAP(ARRAY[1, 2], ARRAY[1, null]), MAP(ARRAY[1, 2], ARRAY[2, null]), MAP(ARRAY[1, 2], ARRAY[1, null]))", "null");
    }

    @Test
    public void testCurrentTimestamp()
    {
        double current = TEST_SESSION.getStartTime() / 1000.0;
        assertOptimizedEquals("current_timestamp = from_unixtime(" + current + ")", "true");
        double future = current + TimeUnit.MINUTES.toSeconds(1);
        assertOptimizedEquals("current_timestamp > from_unixtime(" + future + ")", "false");
    }

    @Test
    public void testCurrentUser()
            throws Exception
    {
        assertOptimizedEquals("current_user", "'" + TEST_SESSION.getUser() + "'");
    }

    @Test
    public void testCastToString()
    {
        // integer
        assertOptimizedEquals("cast(123 as VARCHAR(20))", "'123'");
        assertOptimizedEquals("cast(-123 as VARCHAR(20))", "'-123'");

        // bigint
        assertOptimizedEquals("cast(BIGINT '123' as VARCHAR)", "'123'");
        assertOptimizedEquals("cast(12300000000 as VARCHAR)", "'12300000000'");
        assertOptimizedEquals("cast(-12300000000 as VARCHAR)", "'-12300000000'");

        // double
        assertOptimizedEquals("cast(123.0E0 as VARCHAR)", "'123.0'");
        assertOptimizedEquals("cast(-123.0E0 as VARCHAR)", "'-123.0'");
        assertOptimizedEquals("cast(123.456E0 as VARCHAR)", "'123.456'");
        assertOptimizedEquals("cast(-123.456E0 as VARCHAR)", "'-123.456'");

        // boolean
        assertOptimizedEquals("cast(true as VARCHAR)", "'true'");
        assertOptimizedEquals("cast(false as VARCHAR)", "'false'");

        // string
        assertOptimizedEquals("cast('xyz' as VARCHAR)", "'xyz'");
        assertOptimizedEquals("cast(cast('abcxyz' as VARCHAR(3)) as VARCHAR(5))", "'abc'");

        // null
        assertOptimizedEquals("cast(null as VARCHAR)", "null");

        // decimal
        assertOptimizedEquals("cast(1.1 as VARCHAR)", "'1.1'");
        // TODO enabled when DECIMAL is default for literal: assertOptimizedEquals("cast(12345678901234567890.123 as VARCHAR)", "'12345678901234567890.123'");
    }

    @Test
    public void testCastToBoolean()
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
        assertOptimizedEquals("cast(123.45E0 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(-123.45E0 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(0.0E0 as BOOLEAN)", "false");

        // decimal
        assertOptimizedEquals("cast(0.00 as BOOLEAN)", "false");
        assertOptimizedEquals("cast(7.8 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(12345678901234567890.123 as BOOLEAN)", "true");
        assertOptimizedEquals("cast(00000000000000000000.000 as BOOLEAN)", "false");
    }

    @Test
    public void testCastToBigint()
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
        assertOptimizedEquals("cast(123.0E0 as BIGINT)", "123");
        assertOptimizedEquals("cast(-123.0E0 as BIGINT)", "-123");
        assertOptimizedEquals("cast(123.456E0 as BIGINT)", "123");
        assertOptimizedEquals("cast(-123.456E0 as BIGINT)", "-123");

        // boolean
        assertOptimizedEquals("cast(true as BIGINT)", "1");
        assertOptimizedEquals("cast(false as BIGINT)", "0");

        // string
        assertOptimizedEquals("cast('123' as BIGINT)", "123");
        assertOptimizedEquals("cast('-123' as BIGINT)", "-123");

        // null
        assertOptimizedEquals("cast(null as BIGINT)", "null");

        // decimal
        assertOptimizedEquals("cast(DECIMAL '1.01' as BIGINT)", "1");
        assertOptimizedEquals("cast(DECIMAL '7.8' as BIGINT)", "8");
        assertOptimizedEquals("cast(DECIMAL '1234567890.123' as BIGINT)", "1234567890");
        assertOptimizedEquals("cast(DECIMAL '00000000000000000000.000' as BIGINT)", "0");
    }

    @Test
    public void testCastToInteger()
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
        assertOptimizedEquals("cast(123.0E0 as INTEGER)", "123");
        assertOptimizedEquals("cast(-123.0E0 as INTEGER)", "-123");
        assertOptimizedEquals("cast(123.456E0 as INTEGER)", "123");
        assertOptimizedEquals("cast(-123.456E0 as INTEGER)", "-123");

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
    {
        // integer
        assertOptimizedEquals("cast(0 as DOUBLE)", "0.0E0");
        assertOptimizedEquals("cast(123 as DOUBLE)", "123.0E0");
        assertOptimizedEquals("cast(-123 as DOUBLE)", "-123.0E0");

        // bigint
        assertOptimizedEquals("cast(BIGINT '0' as DOUBLE)", "0.0E0");
        assertOptimizedEquals("cast(12300000000 as DOUBLE)", "12300000000.0E0");
        assertOptimizedEquals("cast(-12300000000 as DOUBLE)", "-12300000000.0E0");

        // double
        assertOptimizedEquals("cast(123.0E0 as DOUBLE)", "123.0E0");
        assertOptimizedEquals("cast(-123.0E0 as DOUBLE)", "-123.0E0");
        assertOptimizedEquals("cast(123.456E0 as DOUBLE)", "123.456E0");
        assertOptimizedEquals("cast(-123.456E0 as DOUBLE)", "-123.456E0");

        // string
        assertOptimizedEquals("cast('0' as DOUBLE)", "0.0E0");
        assertOptimizedEquals("cast('123' as DOUBLE)", "123.0E0");
        assertOptimizedEquals("cast('-123' as DOUBLE)", "-123.0E0");
        assertOptimizedEquals("cast('123.0E0' as DOUBLE)", "123.0E0");
        assertOptimizedEquals("cast('-123.0E0' as DOUBLE)", "-123.0E0");
        assertOptimizedEquals("cast('123.456E0' as DOUBLE)", "123.456E0");
        assertOptimizedEquals("cast('-123.456E0' as DOUBLE)", "-123.456E0");

        // null
        assertOptimizedEquals("cast(null as DOUBLE)", "null");

        // boolean
        assertOptimizedEquals("cast(true as DOUBLE)", "1.0E0");
        assertOptimizedEquals("cast(false as DOUBLE)", "0.0E0");

        // decimal
        assertOptimizedEquals("cast(1.01 as DOUBLE)", "DOUBLE '1.01'");
        assertOptimizedEquals("cast(7.8 as DOUBLE)", "DOUBLE '7.8'");
        assertOptimizedEquals("cast(1234567890.123 as DOUBLE)", "DOUBLE '1234567890.123'");
        assertOptimizedEquals("cast(00000000000000000000.000 as DOUBLE)", "DOUBLE '0.0'");
    }

    @Test
    public void testCastToDecimal()
    {
        // long
        assertOptimizedEquals("cast(0 as DECIMAL(1,0))", "DECIMAL '0'");
        assertOptimizedEquals("cast(123 as DECIMAL(3,0))", "DECIMAL '123'");
        assertOptimizedEquals("cast(-123 as DECIMAL(3,0))", "DECIMAL '-123'");
        assertOptimizedEquals("cast(-123 as DECIMAL(20,10))", "cast(-123 as DECIMAL(20,10))");

        // double
        assertOptimizedEquals("cast(0E0 as DECIMAL(1,0))", "DECIMAL '0'");
        assertOptimizedEquals("cast(123.2E0 as DECIMAL(4,1))", "DECIMAL '123.2'");
        assertOptimizedEquals("cast(-123.0E0 as DECIMAL(3,0))", "DECIMAL '-123'");
        assertOptimizedEquals("cast(-123.55E0 as DECIMAL(20,10))", "cast(-123.55 as DECIMAL(20,10))");

        // string
        assertOptimizedEquals("cast('0' as DECIMAL(1,0))", "DECIMAL '0'");
        assertOptimizedEquals("cast('123.2' as DECIMAL(4,1))", "DECIMAL '123.2'");
        assertOptimizedEquals("cast('-123.0' as DECIMAL(3,0))", "DECIMAL '-123'");
        assertOptimizedEquals("cast('-123.55' as DECIMAL(20,10))", "cast(-123.55 as DECIMAL(20,10))");

        // null
        assertOptimizedEquals("cast(null as DECIMAL(1,0))", "null");
        assertOptimizedEquals("cast(null as DECIMAL(20,10))", "null");

        // boolean
        assertOptimizedEquals("cast(true as DECIMAL(1,0))", "DECIMAL '1'");
        assertOptimizedEquals("cast(false as DECIMAL(4,1))", "DECIMAL '000.0'");
        assertOptimizedEquals("cast(true as DECIMAL(3,0))", "DECIMAL '001'");
        assertOptimizedEquals("cast(false as DECIMAL(20,10))", "cast(0 as DECIMAL(20,10))");

        // decimal
        assertOptimizedEquals("cast(0.0 as DECIMAL(1,0))", "DECIMAL '0'");
        assertOptimizedEquals("cast(123.2 as DECIMAL(4,1))", "DECIMAL '123.2'");
        assertOptimizedEquals("cast(-123.0 as DECIMAL(3,0))", "DECIMAL '-123'");
        assertOptimizedEquals("cast(-123.55 as DECIMAL(20,10))", "cast(-123.55 as DECIMAL(20,10))");
    }

    @Test
    public void testCastOptimization()
    {
        assertOptimizedEquals("cast(unbound_string as VARCHAR)", "cast(unbound_string as VARCHAR)");
        assertOptimizedMatches("cast(unbound_string as VARCHAR)", "unbound_string");
        assertOptimizedMatches("cast(unbound_integer as INTEGER)", "unbound_integer");
        assertOptimizedMatches("cast(unbound_string as VARCHAR(10))", "cast(unbound_string as VARCHAR(10))");
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
        assertOptimizedEquals("try_cast('foo' as DECIMAL(2,1))", "null");
    }

    @Test
    public void testReservedWithDoubleQuotes()
    {
        assertOptimizedEquals("\"time\"", "\"time\"");
    }

    @Test
    public void testSearchCase()
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

        assertOptimizedMatches("if(false, 1, 0 / 0)", "cast(fail(8, 'ignored failure message') as integer)");

        assertOptimizedEquals("case " +
                        "when false then 2.2 " +
                        "when true then 2.2 " +
                        "end",
                "2.2");

        assertOptimizedEquals("case " +
                        "when false then 1234567890.0987654321 " +
                        "when true then 3.3 " +
                        "end",
                "CAST(3.3 AS DECIMAL(20,10))");

        assertOptimizedEquals("case " +
                        "when false then 1 " +
                        "when true then 2.2 " +
                        "end",
                "2.2");

        assertOptimizedEquals("case when ARRAY[CAST(1 AS BIGINT)] = ARRAY[CAST(1 AS BIGINT)] then 'matched' else 'not_matched' end", "'matched'");
        assertOptimizedEquals("case when ARRAY[CAST(2 AS BIGINT)] = ARRAY[CAST(1 AS BIGINT)] then 'matched' else 'not_matched' end", "'not_matched'");
        assertOptimizedEquals("case when ARRAY[CAST(null AS BIGINT)] = ARRAY[CAST(1 AS BIGINT)] then 'matched' else 'not_matched' end", "'not_matched'");
    }

    @Test
    public void testSimpleCase()
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
                        "when cast(fail(8, 'ignored failure message') AS integer) then 2 " +
                        "else 1 " +
                        "end");

        assertOptimizedMatches("case 1 " +
                        "when 0 / 0 then 1 " +
                        "when 0 / 0 then 2 " +
                        "else 1 " +
                        "end",
                "" +
                        "case 1 " +
                        "when cast(fail(8, 'ignored failure message') as integer) then 1 " +
                        "when cast(fail(8, 'ignored failure message') as integer) then 2 " +
                        "else 1 " +
                        "end");

        assertOptimizedEquals("case true " +
                        "when false then 2.2 " +
                        "when true then 2.2 " +
                        "end",
                "2.2");

        // TODO enabled when DECIMAL is default for literal:
//        assertOptimizedEquals("case true " +
//                        "when false then 1234567890.0987654321 " +
//                        "when true then 3.3 " +
//                        "end",
//                "CAST(3.3 AS DECIMAL(20,10))");

        assertOptimizedEquals("case true " +
                        "when false then 1 " +
                        "when true then 2.2 " +
                        "end",
                "2.2");

        assertOptimizedEquals("case ARRAY[CAST(1 AS BIGINT)] when ARRAY[CAST(1 AS BIGINT)] then 'matched' else 'not_matched' end", "'matched'");
        assertOptimizedEquals("case ARRAY[CAST(2 AS BIGINT)] when ARRAY[CAST(1 AS BIGINT)] then 'matched' else 'not_matched' end", "'not_matched'");
        assertOptimizedEquals("case ARRAY[CAST(null AS BIGINT)] when ARRAY[CAST(1 AS BIGINT)] then 'matched' else 'not_matched' end", "'not_matched'");
    }

    @Test
    public void testCoalesce()
    {
        assertOptimizedEquals("coalesce(null, null)", "coalesce(null, null)");
        assertOptimizedEquals("coalesce(2 * 3 * unbound_long, 1 - 1, null)", "coalesce(6 * unbound_long, 0)");
        assertOptimizedEquals("coalesce(2 * 3 * unbound_long, 1.0E0/2.0E0, null)", "coalesce(6 * unbound_long, 0.5E0)");
        assertOptimizedEquals("coalesce(unbound_long, 2, 1.0E0/2.0E0, 12.34E0, null)", "coalesce(unbound_long, 2.0E0, 0.5E0, 12.34E0)");
        assertOptimizedEquals("coalesce(2 * 3 * unbound_integer, 1 - 1, null)", "coalesce(6 * unbound_integer, 0)");
        assertOptimizedEquals("coalesce(2 * 3 * unbound_integer, 1.0E0/2.0E0, null)", "coalesce(6 * unbound_integer, 0.5E0)");
        assertOptimizedEquals("coalesce(unbound_integer, 2, 1.0E0/2.0E0, 12.34E0, null)", "coalesce(unbound_integer, 2.0E0, 0.5E0, 12.34E0)");
        assertOptimizedMatches("coalesce(0 / 0 > 1, unbound_boolean, 0 / 0 = 0)",
                "coalesce(cast(fail(8, 'ignored failure message') as boolean), unbound_boolean)");
        assertOptimizedMatches("coalesce(unbound_long, unbound_long)", "unbound_long");
        assertOptimizedMatches("coalesce(2 * unbound_long, 2 * unbound_long)", "BIGINT '2' * unbound_long");
        assertOptimizedMatches("coalesce(unbound_long, unbound_long2, unbound_long)", "coalesce(unbound_long, unbound_long2)");
        assertOptimizedMatches("coalesce(unbound_long, unbound_long2, unbound_long, unbound_long3)", "coalesce(unbound_long, unbound_long2, unbound_long3)");
        assertOptimizedEquals("coalesce(6, unbound_long2, unbound_long, unbound_long3)", "6");
        assertOptimizedEquals("coalesce(2 * 3, unbound_long2, unbound_long, unbound_long3)", "6");
        assertOptimizedMatches("coalesce(unbound_long, coalesce(unbound_long, 1))", "coalesce(unbound_long, BIGINT '1')");
        assertOptimizedMatches("coalesce(coalesce(unbound_long, coalesce(unbound_long, 1)), unbound_long2)", "coalesce(unbound_long, BIGINT '1')");
        assertOptimizedMatches("coalesce(unbound_long, 2, coalesce(unbound_long, 1))", "coalesce(unbound_long, BIGINT '2')");
        assertOptimizedMatches("coalesce(coalesce(unbound_long, coalesce(unbound_long2, unbound_long3)), 1)", "coalesce(unbound_long, unbound_long2, unbound_long3, BIGINT '1')");
        assertOptimizedMatches("coalesce(unbound_double, coalesce(random(), unbound_double))", "coalesce(unbound_double, random())");
        assertOptimizedMatches("coalesce(random(), random(), 5)", "coalesce(random(), random(), 5E0)");
        assertOptimizedMatches("coalesce(unbound_long, coalesce(unbound_long, 1))", "coalesce(unbound_long, BIGINT '1')");
        assertOptimizedMatches("coalesce(coalesce(unbound_long, coalesce(unbound_long, 1)), unbound_long2)", "coalesce(unbound_long, BIGINT '1')");
        assertOptimizedMatches("coalesce(unbound_long, 2, coalesce(unbound_long, 1))", "coalesce(unbound_long, BIGINT '2')");
        assertOptimizedMatches("coalesce(coalesce(unbound_long, coalesce(unbound_long2, unbound_long3)), 1)", "coalesce(unbound_long, unbound_long2, unbound_long3, BIGINT '1')");
        assertOptimizedMatches("coalesce(unbound_double, coalesce(random(), unbound_double))", "coalesce(unbound_double, random())");
    }

    @Test
    public void testIf()
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

        assertOptimizedEquals("IF(true, 3.5E0, 4.2E0)", "3.5E0");
        assertOptimizedEquals("IF(false, 3.5E0, 4.2E0)", "4.2E0");

        assertOptimizedEquals("IF(true, 'foo', 'bar')", "'foo'");
        assertOptimizedEquals("IF(false, 'foo', 'bar')", "'bar'");

        assertOptimizedEquals("IF(true, 1.01, 1.02)", "1.01");
        assertOptimizedEquals("IF(false, 1.01, 1.02)", "1.02");
        assertOptimizedEquals("IF(true, 1234567890.123, 1.02)", "1234567890.123");
        assertOptimizedEquals("IF(false, 1.01, 1234567890.123)", "1234567890.123");
    }

    @Test
    public void testLike()
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
        assertOptimizedEquals("'a' LIKE unbound_string ESCAPE null", "null");

        assertOptimizedEquals("'%' LIKE 'z%' ESCAPE 'z'", "true");

        assertRowExpressionEquals(SERIALIZABLE, "'%' LIKE 'z%' ESCAPE 'z'", "true");
        assertRowExpressionEquals(SERIALIZABLE, "'%' LIKE 'z%'", "false");
    }

    @Test
    public void testLikeOptimization()
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
        assertDoNotOptimize("unbound_string LIKE 'abc%'", SERIALIZABLE);

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
    public void testLambda()
    {
        assertDoNotOptimize("transform(unbound_array, x -> x + x)", OPTIMIZED);
        assertOptimizedEquals("transform(ARRAY[1, 5], x -> x + x)", "transform(ARRAY[1, 5], x -> x + x)");
        assertOptimizedEquals("transform(sequence(1, 5), x -> x + x)", "transform(sequence(1, 5), x -> x + x)");
        assertRowExpressionEquals(
                OPTIMIZED,
                "transform(sequence(1, unbound_long), x -> cast(json_parse('[1, 2]') AS ARRAY<INTEGER>)[1] + x)",
                "transform(sequence(1, unbound_long), x -> 1 + x)");
        assertRowExpressionEquals(
                OPTIMIZED,
                "transform(sequence(1, unbound_long), x -> cast(json_parse('[1, 2]') AS ARRAY<INTEGER>)[1] + 1)",
                "transform(sequence(1, unbound_long), x -> 2)");
        assertEquals(evaluate("reduce(ARRAY[1, 5], 0, (x, y) -> x + y, x -> x)", true), 6L);
    }

    @Test
    public void testBind()
    {
        assertOptimizedEquals("apply(90, \"$internal$bind\"(9, (x, y) -> x + y))", "apply(90, \"$internal$bind\"(9, (x, y) -> x + y))");
        evaluate("apply(90, \"$internal$bind\"(9, (x, y) -> x + y))", true);
        evaluate("apply(900, \"$internal$bind\"(90, 9, (x, y, z) -> x + y + z))", true);
    }

    @Test
    public void testFailedExpressionOptimization()
    {
        assertOptimizedMatches("CASE unbound_long WHEN 1 THEN 1 WHEN 0 / 0 THEN 2 END",
                "CASE unbound_long WHEN BIGINT '1' THEN 1 WHEN cast(fail(8, 'ignored failure message') as bigint) THEN 2 END");

        assertOptimizedMatches("CASE unbound_boolean WHEN true THEN 1 ELSE 0 / 0 END",
                "CASE unbound_boolean WHEN true THEN 1 ELSE cast(fail(8, 'ignored failure message') as integer) END");

        assertOptimizedMatches("CASE bound_long WHEN unbound_long THEN 1 WHEN 0 / 0 THEN 2 ELSE 1 END",
                "CASE BIGINT '1234' WHEN unbound_long THEN 1 WHEN cast(fail(8, 'ignored failure message') as bigint) THEN 2 ELSE 1 END");

        assertOptimizedMatches("case when unbound_boolean then 1 when 0 / 0 = 0 then 2 end",
                "case when unbound_boolean then 1 when cast(fail(8, 'ignored failure message') as boolean) then 2 end");

        assertOptimizedMatches("case when unbound_boolean then 1 else 0 / 0  end",
                "case when unbound_boolean then 1 else cast(fail(8, 'ignored failure message') as integer) end");

        assertOptimizedMatches("case when unbound_boolean then 0 / 0 else 1 end",
                "case when unbound_boolean then cast(fail(8, 'ignored failure message') as integer) else 1 end");
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testOptimizeDivideByZero()
    {
        optimize("0 / 0");
    }

    @Test
    public void testMassiveArray()
    {
        assertRowExpressionEquals(
                OPTIMIZED,
                "SEQUENCE(1, 999)",
                format("ARRAY [%s]", Joiner.on(", ").join(IntStream.range(1, 1000).mapToObj(i -> "(BIGINT '" + i + "')").iterator())));
        assertDoNotOptimize("SEQUENCE(1, 1000)", SERIALIZABLE);
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
        optimize("ARRAY [ROW(unbound_string, unbound_double), ROW(unbound_string, 0.0E0)]");
        optimize("ARRAY [ROW('string', unbound_double), ROW('string', bound_double)]");
        optimize("ROW(ROW(NULL), ROW(ROW(ROW(ROW('rowception')))))");
        optimize("ROW(unbound_string, bound_string)");

        optimize("ARRAY [ROW(unbound_string, unbound_double), ROW(CAST(bound_string AS VARCHAR), 0.0E0)]");
        optimize("ARRAY [ROW(CAST(bound_string AS VARCHAR), 0.0E0), ROW(unbound_string, unbound_double)]");

        optimize("ARRAY [ROW(unbound_string, unbound_double), CAST(NULL AS ROW(VARCHAR, DOUBLE))]");
        optimize("ARRAY [CAST(NULL AS ROW(VARCHAR, DOUBLE)), ROW(unbound_string, unbound_double)]");
    }

    @Test
    public void testDereference()
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
    public void testRowDereference()
    {
        optimize("CAST(null AS ROW(a VARCHAR, b BIGINT)).a");
    }

    @Test
    public void testRowSubscript()
    {
        assertOptimizedEquals("ROW (1, 'a', true)[3]", "true");
        assertOptimizedEquals("ROW (1, 'a', ROW (2, 'b', ROW (3, 'c')))[3][3][2]", "'c'");
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
                Optional.empty());
        assertEquals(evaluate(predicate, true), expected);
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

    private static void assertRowExpressionEquals(Level level, @Language("SQL") String actual, @Language("SQL") String expected)
    {
        Object actualResult = optimize(toRowExpression(expression(actual)), level);
        Object expectedResult = optimize(toRowExpression(expression(expected)), level);
        if (actualResult instanceof Block && expectedResult instanceof Block) {
            assertEquals(blockToSlice((Block) actualResult), blockToSlice((Block) expectedResult));
            return;
        }
        assertEquals(actualResult, expectedResult);
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

        Expression parsedExpression = expression(expression);
        Object expressionResult = optimize(parsedExpression);

        RowExpression rowExpression = toRowExpression(parsedExpression);
        Object rowExpressionResult = optimize(rowExpression, OPTIMIZED);
        assertExpressionAndRowExpressionEquals(expressionResult, rowExpressionResult);
        return expressionResult;
    }

    private static Expression expression(String expression)
    {
        return FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);
    }

    private static RowExpression toRowExpression(Expression expression)
    {
        return TRANSLATOR.translate(expression, SYMBOL_TYPES);
    }

    private static Object optimize(Expression expression)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(TEST_SESSION, METADATA, SQL_PARSER, SYMBOL_TYPES, expression, emptyList(), WarningCollector.NOOP);
        ExpressionInterpreter interpreter = expressionOptimizer(expression, METADATA, TEST_SESSION, expressionTypes);
        return interpreter.optimize(variable -> {
            Symbol symbol = new Symbol(variable.getName());
            Object value = symbolConstant(symbol);
            if (value == null) {
                return symbol.toSymbolReference();
            }
            return value;
        });
    }

    private static Object optimize(RowExpression expression, Level level)
    {
        return new RowExpressionInterpreter(expression, METADATA, TEST_SESSION.toConnectorSession(), level).optimize(variable -> {
            Symbol symbol = new Symbol(variable.getName());
            Object value = symbolConstant(symbol);
            if (value == null) {
                return new VariableReferenceExpression(symbol.getName(), SYMBOL_TYPES.get(symbol.toSymbolReference()));
            }
            return value;
        });
    }

    private static void assertDoNotOptimize(@Language("SQL") String expression, Level optimizationLevel)
    {
        assertRoundTrip(expression);
        Expression translatedExpression = expression(expression);
        RowExpression rowExpression = toRowExpression(translatedExpression);

        Object expressionResult = optimize(translatedExpression);
        if (expressionResult instanceof Expression) {
            expressionResult = toRowExpression((Expression) expressionResult);
        }
        Object rowExpressionResult = optimize(rowExpression, optimizationLevel);
        assertRowExpressionEvaluationEquals(expressionResult, rowExpressionResult);
        assertRowExpressionEvaluationEquals(rowExpressionResult, rowExpression);
    }

    private static Object symbolConstant(Symbol symbol)
    {
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
            case "bound_decimal_short":
                return 12345L;
            case "bound_decimal_long":
                return Decimals.encodeUnscaledValue(new BigInteger("12345678901234567890123"));
        }
        return null;
    }

    private static void assertExpressionAndRowExpressionEquals(Object expressionResult, Object rowExpressionResult)
    {
        if (rowExpressionResult instanceof RowExpression) {
            // Cannot be completely evaluated into a constant; compare expressions
            assertTrue(expressionResult instanceof Expression);

            // It is tricky to check the equivalence of an expression and a row expression.
            // We rely on the optimized translator to fill the gap.
            RowExpression translated = TRANSLATOR.translateAndOptimize((Expression) expressionResult, SYMBOL_TYPES);
            assertRowExpressionEvaluationEquals(translated, rowExpressionResult);
        }
        else {
            // We have constants; directly compare
            assertRowExpressionEvaluationEquals(expressionResult, rowExpressionResult);
        }
    }

    /**
     * Assert the evaluation result of two row expressions equivalent
     * no matter they are constants or remaining row expressions.
     */
    private static void assertRowExpressionEvaluationEquals(Object left, Object right)
    {
        if (right instanceof RowExpression) {
            assertTrue(left instanceof RowExpression);
            // assertEquals(((RowExpression) left).getType(), ((RowExpression) right).getType());
            if (left instanceof ConstantExpression) {
                if (isRemovableCast(right)) {
                    assertRowExpressionEvaluationEquals(left, ((CallExpression) right).getArguments().get(0));
                    return;
                }
                assertTrue(right instanceof ConstantExpression);
                assertRowExpressionEvaluationEquals(((ConstantExpression) left).getValue(), ((ConstantExpression) left).getValue());
            }
            else if (left instanceof InputReferenceExpression || left instanceof VariableReferenceExpression) {
                assertEquals(left, right);
            }
            else if (left instanceof CallExpression) {
                assertTrue(right instanceof CallExpression);
                assertEquals(((CallExpression) left).getFunctionHandle(), ((CallExpression) right).getFunctionHandle());
                assertEquals(((CallExpression) left).getArguments().size(), ((CallExpression) right).getArguments().size());
                for (int i = 0; i < ((CallExpression) left).getArguments().size(); i++) {
                    assertRowExpressionEvaluationEquals(((CallExpression) left).getArguments().get(i), ((CallExpression) right).getArguments().get(i));
                }
            }
            else if (left instanceof SpecialFormExpression) {
                assertTrue(right instanceof SpecialFormExpression);
                assertEquals(((SpecialFormExpression) left).getForm(), ((SpecialFormExpression) right).getForm());
                assertEquals(((SpecialFormExpression) left).getArguments().size(), ((SpecialFormExpression) right).getArguments().size());
                for (int i = 0; i < ((SpecialFormExpression) left).getArguments().size(); i++) {
                    assertRowExpressionEvaluationEquals(((SpecialFormExpression) left).getArguments().get(i), ((SpecialFormExpression) right).getArguments().get(i));
                }
            }
            else {
                assertTrue(left instanceof LambdaDefinitionExpression);
                assertTrue(right instanceof LambdaDefinitionExpression);
                assertEquals(((LambdaDefinitionExpression) left).getArguments(), ((LambdaDefinitionExpression) right).getArguments());
                assertEquals(((LambdaDefinitionExpression) left).getArgumentTypes(), ((LambdaDefinitionExpression) right).getArgumentTypes());
                assertRowExpressionEvaluationEquals(((LambdaDefinitionExpression) left).getBody(), ((LambdaDefinitionExpression) right).getBody());
            }
        }
        else {
            // We have constants; directly compare
            if (left instanceof Block) {
                assertTrue(right instanceof Block);
                assertEquals(blockToSlice((Block) left), blockToSlice((Block) right));
            }
            else {
                assertEquals(left, right);
            }
        }
    }

    private static boolean isRemovableCast(Object value)
    {
        if (value instanceof CallExpression &&
                new FunctionResolution(METADATA.getFunctionAndTypeManager()).isCastFunction(((CallExpression) value).getFunctionHandle())) {
            Type targetType = ((CallExpression) value).getType();
            Type sourceType = ((CallExpression) value).getArguments().get(0).getType();
            return METADATA.getFunctionAndTypeManager().canCoerce(sourceType, targetType);
        }
        return false;
    }

    private static Slice blockToSlice(Block block)
    {
        // This function is strictly for testing use only
        SliceOutput sliceOutput = new DynamicSliceOutput(1000);
        BlockSerdeUtil.writeBlock(blockEncodingSerde, sliceOutput, block);
        return sliceOutput.slice();
    }

    private static void assertEvaluatedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertEquals(evaluate(actual, true), evaluate(expected, true));
    }

    private static Object evaluate(String expression, boolean deterministic)
    {
        assertRoundTrip(expression);

        Expression parsedExpression = FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);

        return evaluate(parsedExpression, deterministic);
    }

    private static void assertRoundTrip(String expression)
    {
        ParsingOptions parsingOptions = createParsingOptions(TEST_SESSION);
        assertEquals(SQL_PARSER.createExpression(expression, parsingOptions),
                SQL_PARSER.createExpression(formatExpression(SQL_PARSER.createExpression(expression, parsingOptions), Optional.empty()), parsingOptions));
    }

    private static Object evaluate(Expression expression, boolean deterministic)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(TEST_SESSION, METADATA, SQL_PARSER, SYMBOL_TYPES, expression, emptyList(), WarningCollector.NOOP);
        Object expressionResult = expressionInterpreter(expression, METADATA, TEST_SESSION, expressionTypes).evaluate();
        Object rowExpressionResult = rowExpressionInterpreter(TRANSLATOR.translateAndOptimize(expression), METADATA, TEST_SESSION.toConnectorSession()).evaluate();

        if (deterministic) {
            assertExpressionAndRowExpressionEquals(expressionResult, rowExpressionResult);
        }
        return expressionResult;
    }

    private static class FailedFunctionRewriter
            extends ExpressionRewriter<Object>
    {
        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Object context, ExpressionTreeRewriter<Object> treeRewriter)
        {
            if (node.getName().equals(QualifiedName.of("fail"))) {
                return new FunctionCall(QualifiedName.of("fail"), ImmutableList.of(node.getArguments().get(0), new StringLiteral("ignored failure message")));
            }
            return node;
        }
    }
}
