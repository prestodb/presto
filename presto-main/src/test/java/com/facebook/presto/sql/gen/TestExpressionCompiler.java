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
package com.facebook.presto.sql.gen;

import com.facebook.presto.operator.scalar.BitwiseFunctions;
import com.facebook.presto.operator.scalar.DateTimeFunctions;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.operator.scalar.JoniRegexpFunctions;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.JsonPath;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.tree.Extract.Field;
import com.facebook.presto.type.LikeFunctions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.joni.Regex;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.LongStream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.scalar.JoniRegexpCasts.joniRegexp;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static java.lang.Math.cos;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExpressionCompiler
{
    private static final Boolean[] booleanValues = {true, false, null};
    private static final Integer[] smallInts = {9, 10, 11, -9, -10, -11, null};
    private static final Integer[] extremeInts = {101510, /*Long.MIN_VALUE,*/ Integer.MAX_VALUE};
    private static final Integer[] intLefts = ObjectArrays.concat(smallInts, extremeInts, Integer.class);
    private static final Integer[] intRights = {3, -3, 101510823, null};
    private static final Integer[] intMiddle = {9, -3, 88, null};
    private static final Double[] doubleLefts = {9.0, 10.0, 11.0, -9.0, -10.0, -11.0, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1,
            Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_NORMAL, null};
    private static final Double[] doubleRights = {3.0, -3.0, 3.1, -3.1, null};
    private static final Double[] doubleMiddle = {9.0, -3.1, 88.0, null};
    private static final String[] stringLefts = {"hello", "foo", "mellow", "fellow", "", null};
    private static final String[] stringRights = {"hello", "foo", "bar", "baz", "", null};
    private static final Long[] longLefts = {9L, 10L, 11L, -9L, -10L, -11L, null};
    private static final Long[] longRights = {3L, -3L, 10151082135029369L, null};
    private static final BigDecimal[] decimalLefts = {new BigDecimal("9.0"), new BigDecimal("10.0"), new BigDecimal("11.0"), new BigDecimal("-9.0"),
            new BigDecimal("-10.0"), new BigDecimal("-11.0"), new BigDecimal("9.1"), new BigDecimal("10.1"),
            new BigDecimal("11.1"), new BigDecimal("-9.1"), new BigDecimal("-10.1"), new BigDecimal("-11.1"),
            new BigDecimal("9223372036.5477"), new BigDecimal("-9223372036.5477"), null};
    private static final BigDecimal[] decimalRights = {new BigDecimal("3.0"), new BigDecimal("-3.0"), new BigDecimal("3.1"), new BigDecimal("-3.1"), null};
    private static final BigDecimal[] decimalMiddle = {new BigDecimal("9.0"), new BigDecimal("-3.1"), new BigDecimal("88.0"), null};

    private static final DateTime[] dateTimeValues = {
            new DateTime(2001, 1, 22, 3, 4, 5, 321, UTC),
            new DateTime(1960, 1, 22, 3, 4, 5, 321, UTC),
            new DateTime(1970, 1, 1, 0, 0, 0, 0, UTC),
            null
    };

    private static final String[] jsonValues = {
            "{}",
            "{\"fuu\": {\"bar\": 1}}",
            "{\"fuu\": null}",
            "{\"fuu\": 1}",
            "{\"fuu\": 1, \"bar\": \"abc\"}",
            null
    };
    private static final String[] jsonPatterns = {
            "$",
            "$.fuu",
            "$.fuu[0]",
            "$.bar",
            null
    };

    private static final Logger log = Logger.get(TestExpressionCompiler.class);
    private static final boolean PARALLEL = false;

    private long start;
    private ListeningExecutorService executor;
    private FunctionAssertions functionAssertions;
    private List<ListenableFuture<?>> futures;

    @BeforeClass
    public void setupClass()
    {
        Logging.initialize();
        if (PARALLEL) {
            executor = listeningDecorator(newFixedThreadPool(getRuntime().availableProcessors() * 2, daemonThreadsNamed("completer-%s")));
        }
        else {
            executor = newDirectExecutorService();
        }
        functionAssertions = new FunctionAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        closeAllRuntimeException(functionAssertions);
        functionAssertions = null;
    }

    @BeforeMethod
    public void setUp()
    {
        start = System.nanoTime();
        futures = new ArrayList<>();
    }

    @AfterMethod
    public void tearDown(Method method)
    {
        assertTrue(Futures.allAsList(futures).isDone(), "Expression test futures are not complete");
        log.info("FINISHED %s in %s verified %s expressions", method.getName(), Duration.nanosSince(start), futures.size());
    }

    @Test
    public void smokedTest()
            throws Exception
    {
        assertExecute("cast(true as boolean)", BOOLEAN, true);
        assertExecute("true", BOOLEAN, true);
        assertExecute("false", BOOLEAN, false);
        assertExecute("42", INTEGER, 42);
        assertExecute("'foo'", createVarcharType(3), "foo");
        assertExecute("4.2E0", DOUBLE, 4.2);
        assertExecute("10000000000 + 1", BIGINT, 10000000001L);
        assertExecute("4.2", createDecimalType(2, 1), new SqlDecimal(BigInteger.valueOf(42), 2, 1));
        assertExecute("DECIMAL '4.2'", createDecimalType(2, 1), new SqlDecimal(BigInteger.valueOf(42), 2, 1));
        assertExecute("X' 1 f'", VARBINARY, new SqlVarbinary(Slices.wrappedBuffer((byte) 0x1f).getBytes()));
        assertExecute("X' '", VARBINARY, new SqlVarbinary(new byte[0]));
        assertExecute("bound_integer", INTEGER, 1234);
        assertExecute("bound_long", BIGINT, 1234L);
        assertExecute("bound_string", VARCHAR, "hello");
        assertExecute("bound_double", DOUBLE, 12.34);
        assertExecute("bound_boolean", BOOLEAN, true);
        assertExecute("bound_timestamp", BIGINT, new DateTime(2001, 8, 22, 3, 4, 5, 321, UTC).getMillis());
        assertExecute("bound_pattern", VARCHAR, "%el%");
        assertExecute("bound_null_string", VARCHAR, null);
        assertExecute("bound_timestamp_with_timezone", TIMESTAMP_WITH_TIME_ZONE, new SqlTimestampWithTimeZone(new DateTime(1970, 1, 1, 0, 1, 0, 999, DateTimeZone.UTC).getMillis(), TimeZoneKey.getTimeZoneKey("Z")));
        assertExecute("bound_binary_literal", VARBINARY, new SqlVarbinary(new byte[] {(byte) 0xab}));

        // todo enable when null output type is supported
        // assertExecute("null", null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void filterFunction()
            throws Exception
    {
        assertFilter("true", true);
        assertFilter("false", false);
        assertFilter("bound_integer = 1234", true);
        assertFilter("bound_integer = BIGINT '1234'", true);
        assertFilter("bound_long = 1234", true);
        assertFilter("bound_long = BIGINT '1234'", true);
        assertFilter("bound_long = 5678", false);
        assertFilter("bound_null_string is null", true);
        assertFilter("bound_null_string = 'foo'", false);

        // todo enable when null output type is supported
        // assertFilter("null", false);
        assertFilter("cast(null as boolean)", false);
        assertFilter("nullif(true, true)", false);

        assertFilter("true AND cast(null as boolean) AND true", false);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testUnaryOperators()
            throws Exception
    {
        assertExecute("cast(null as boolean) is null", BOOLEAN, true);

        for (Boolean value : booleanValues) {
            assertExecute(generateExpression("%s", value), BOOLEAN, value == null ? null : value);
            assertExecute(generateExpression("%s is null", value), BOOLEAN, value == null);
            assertExecute(generateExpression("%s is not null", value), BOOLEAN, value != null);
        }

        for (Integer value : intLefts) {
            Long longValue = value == null ? null : value * 10000000000L;
            assertExecute(generateExpression("%s", value), INTEGER, value == null ? null : value);
            assertExecute(generateExpression("- (%s)", value), INTEGER, value == null ? null : -value);
            assertExecute(generateExpression("%s", longValue), BIGINT, value == null ? null : longValue);
            assertExecute(generateExpression("- (%s)", longValue), BIGINT, value == null ? null : -longValue);
            assertExecute(generateExpression("%s is null", value), BOOLEAN, value == null);
            assertExecute(generateExpression("%s is not null", value), BOOLEAN, value != null);
        }

        for (Double value : doubleLefts) {
            assertExecute(generateExpression("%s", value), DOUBLE, value == null ? null : value);
            assertExecute(generateExpression("- (%s)", value), DOUBLE, value == null ? null : -value);
            assertExecute(generateExpression("%s is null", value), BOOLEAN, value == null);
            assertExecute(generateExpression("%s is not null", value), BOOLEAN, value != null);
        }

        for (BigDecimal value : decimalLefts) {
            assertExecute(generateExpression("%s", value), value == null ? null : value);
            assertExecute(generateExpression("- (%s)", value), value == null ? null : value.negate());
            assertExecute(generateExpression("%s is null", value), BOOLEAN, value == null);
            assertExecute(generateExpression("%s is not null", value), BOOLEAN, value != null);
        }

        for (String value : stringLefts) {
            assertExecute(generateExpression("%s", value), varcharType(value), value == null ? null : value);
            assertExecute(generateExpression("%s is null", value), BOOLEAN, value == null);
            assertExecute(generateExpression("%s is not null", value), BOOLEAN, value != null);
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testFilterEmptyInput()
            throws Exception
    {
        assertFilterWithNoInputColumns("true", true);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsBoolean()
            throws Exception
    {
        assertExecute("nullif(cast(null as boolean), true)", BOOLEAN, null);
        for (Boolean left : booleanValues) {
            for (Boolean right : booleanValues) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : left == right);
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : left != right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), BOOLEAN, nullIf(left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN, !Objects.equals(left, right));
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsIntegralIntegral()
            throws Exception
    {
        for (Integer left : smallInts) {
            for (Integer right : intRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : (long) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : (long) left != right);
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : (long) left > right);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : (long) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : (long) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : (long) left <= right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), INTEGER, nullIf(left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN, !Objects.equals(left, right));

                assertExecute(generateExpression("%s + %s", left, right), INTEGER, left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), INTEGER, left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), INTEGER, left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), INTEGER, left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), INTEGER, left == null || right == null ? null : left % right);

                Long longLeft = left == null ? null : left * 1000000000L;
                assertExecute(generateExpression("%s + %s", longLeft, right), BIGINT, longLeft == null || right == null ? null : longLeft + right);
                assertExecute(generateExpression("%s - %s", longLeft, right), BIGINT, longLeft == null || right == null ? null : longLeft - right);
                assertExecute(generateExpression("%s * %s", longLeft, right), BIGINT, longLeft == null || right == null ? null : longLeft * right);
                assertExecute(generateExpression("%s / %s", longLeft, right), BIGINT, longLeft == null || right == null ? null : longLeft / right);
                assertExecute(generateExpression("%s %% %s", longLeft, right), BIGINT, longLeft == null || right == null ? null : longLeft % right);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsIntegralDouble()
            throws Exception
    {
        for (Integer left : intLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left != right);
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left > right);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left <= right);

                Object expectedNullIf = nullIf(left, right);
                for (String expression : generateExpression("nullif(%s, CAST(%s as DOUBLE))", left, right)) {
                    functionAssertions.assertFunction(expression, INTEGER, expectedNullIf);
                }

                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN, !Objects.equals(left == null ? null : left.doubleValue(), right));

                assertExecute(generateExpression("%s + %s", left, right), DOUBLE, left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), DOUBLE, left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), DOUBLE, left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), DOUBLE, left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), DOUBLE, left == null || right == null ? null : left % right);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsDoubleIntegral()
            throws Exception
    {
        for (Double left : doubleLefts) {
            for (Integer right : intRights) {
                assertExecute(generateExpression("CAST(%s as DOUBLE) = %s", left, right), BOOLEAN, left == null || right == null ? null : left == (double) right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) <> %s", left, right), BOOLEAN, left == null || right == null ? null : left != (double) right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) > %s", left, right), BOOLEAN, left == null || right == null ? null : left > (double) right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) < %s", left, right), BOOLEAN, left == null || right == null ? null : left < (double) right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) >= %s", left, right), BOOLEAN, left == null || right == null ? null : left >= (double) right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) <= %s", left, right), BOOLEAN, left == null || right == null ? null : left <= (double) right);

                assertExecute(generateExpression("nullif(CAST(%s as DOUBLE), %s)", left, right), DOUBLE, nullIf(left, right));
                assertExecute(generateExpression("CAST(%s as DOUBLE) is distinct from %s", left, right), BOOLEAN, !Objects.equals(left, right == null ? null : right.doubleValue()));

                assertExecute(generateExpression("CAST(%s as DOUBLE) + %s", left, right), DOUBLE, left == null || right == null ? null : left + right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) - %s", left, right), DOUBLE, left == null || right == null ? null : left - right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) * %s", left, right), DOUBLE, left == null || right == null ? null : left * right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) / %s", left, right), DOUBLE, left == null || right == null ? null : left / right);
                assertExecute(generateExpression("CAST(%s as DOUBLE) %% %s", left, right), DOUBLE, left == null || right == null ? null : left % right);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsDoubleDouble()
            throws Exception
    {
        for (Double left : doubleLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left != right);
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left > right);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : (double) left <= right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), DOUBLE, nullIf(left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN, !Objects.equals(left, right));

                assertExecute(generateExpression("%s + %s", left, right), DOUBLE, left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), DOUBLE, left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), DOUBLE, left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), DOUBLE, left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), DOUBLE, left == null || right == null ? null : left % right);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsDecimalBigint()
            throws Exception
    {
        for (BigDecimal left : decimalLefts) {
            for (Long right : longRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : left.equals(new BigDecimal(right)));
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : !left.equals(new BigDecimal(right)));
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(new BigDecimal(right)) > 0);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(new BigDecimal(right)) < 0);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(new BigDecimal(right)) >= 0);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(new BigDecimal(right)) <= 0);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), BigDecimal.class.cast(nullIf(left, right)));

                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN,
                        !Objects.equals(left, right == null ? null : new BigDecimal(right)));

                // arithmetic operators are already tested in TestDecimalOperators
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsBigintDecimal()
            throws Exception
    {
        for (Long left : longLefts) {
            for (BigDecimal right : decimalRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).equals(right));
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : !new BigDecimal(left).equals(right));
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).compareTo(right) > 0);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).compareTo(right) < 0);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).compareTo(right) >= 0);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).compareTo(right) <= 0);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), BIGINT, left);

                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN,
                        !Objects.equals(left == null ? null : new BigDecimal(left), right));

                // arithmetic operators are already tested in TestDecimalOperators
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsDecimalInteger()
            throws Exception
    {
        for (BigDecimal left : decimalLefts) {
            for (Integer right : intRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : left.equals(new BigDecimal(right)));
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : !left.equals(new BigDecimal(right)));
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(new BigDecimal(right)) > 0);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(new BigDecimal(right)) < 0);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(new BigDecimal(right)) >= 0);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(new BigDecimal(right)) <= 0);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), BigDecimal.class.cast(nullIf(left, right)));

                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN,
                        !Objects.equals(left, right == null ? null : new BigDecimal(right)));

                // arithmetic operators are already tested in TestDecimalOperators
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsIntegerDecimal()
            throws Exception
    {
        for (Integer left : intLefts) {
            for (BigDecimal right : decimalRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).equals(right));
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : !new BigDecimal(left).equals(right));
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).compareTo(right) > 0);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).compareTo(right) < 0);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).compareTo(right) >= 0);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : new BigDecimal(left).compareTo(right) <= 0);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), INTEGER, left);

                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN,
                        !Objects.equals(left == null ? null : new BigDecimal(left), right));

                // arithmetic operators are already tested in TestDecimalOperators
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsDecimalDouble()
            throws Exception
    {
        for (BigDecimal left : decimalLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : left.doubleValue() == right);
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : left.doubleValue() != right);
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : left.doubleValue() > right);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : left.doubleValue() < right);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : left.doubleValue() >= right);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : left.doubleValue() <= right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), BigDecimal.class.cast(nullIf(left, right)));

                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN, !Objects.equals(left == null ? null : left.doubleValue(), right));

                assertExecute(generateExpression("%s + %s", left, right), DOUBLE, left == null || right == null ? null : left.doubleValue() + right);
                assertExecute(generateExpression("%s - %s", left, right), DOUBLE, left == null || right == null ? null : left.doubleValue() - right);
                assertExecute(generateExpression("%s * %s", left, right), DOUBLE, left == null || right == null ? null : left.doubleValue() * right);
                assertExecute(generateExpression("%s / %s", left, right), DOUBLE, left == null || right == null ? null : left.doubleValue() / right);
                assertExecute(generateExpression("%s %% %s", left, right), DOUBLE, left == null || right == null ? null : left.doubleValue() % right);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsDoubleDecimal()
            throws Exception
    {
        for (Double left : doubleLefts) {
            for (BigDecimal right : decimalRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : left == right.doubleValue());
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : left != right.doubleValue());
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : left > right.doubleValue());
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : left < right.doubleValue());
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : left >= right.doubleValue());
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : left <= right.doubleValue());

                assertExecute(generateExpression("nullif(%s, %s)", left, right), DOUBLE, nullIf(left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN, !Objects.equals(left, right == null ? null : right.doubleValue()));

                assertExecute(generateExpression("%s + %s", left, right), DOUBLE, left == null || right == null ? null : left + right.doubleValue());
                assertExecute(generateExpression("%s - %s", left, right), DOUBLE, left == null || right == null ? null : left - right.doubleValue());
                assertExecute(generateExpression("%s * %s", left, right), DOUBLE, left == null || right == null ? null : left * right.doubleValue());
                assertExecute(generateExpression("%s / %s", left, right), DOUBLE, left == null || right == null ? null : left / right.doubleValue());
                assertExecute(generateExpression("%s %% %s", left, right), DOUBLE, left == null || right == null ? null : left % right.doubleValue());
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsString()
            throws Exception
    {
        for (String left : stringLefts) {
            for (String right : stringRights) {
                assertExecute(generateExpression("%s = %s", left, right), BOOLEAN, left == null || right == null ? null : left.equals(right));
                assertExecute(generateExpression("%s <> %s", left, right), BOOLEAN, left == null || right == null ? null : !left.equals(right));
                assertExecute(generateExpression("%s > %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(right) > 0);
                assertExecute(generateExpression("%s < %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(right) < 0);
                assertExecute(generateExpression("%s >= %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(right) >= 0);
                assertExecute(generateExpression("%s <= %s", left, right), BOOLEAN, left == null || right == null ? null : left.compareTo(right) <= 0);

                assertExecute(generateExpression("%s || %s", left, right), VARCHAR, left == null || right == null ? null : left + right);

                assertExecute(generateExpression("%s is distinct from %s", left, right), BOOLEAN, !Objects.equals(left, right));

                assertExecute(generateExpression("nullif(%s, %s)", left, right), varcharType(left), nullIf(left, right));
            }
        }

        Futures.allAsList(futures).get();
    }

    private static VarcharType varcharType(String... values)
    {
        return varcharType(Arrays.asList(values));
    }

    private static VarcharType varcharType(List<String> values)
    {
        if (values.stream().anyMatch(Objects::isNull)) {
            return VARCHAR;
        }
        return createVarcharType(values.stream().mapToInt(String::length).max().getAsInt());
    }

    private static Object nullIf(Object left, Object right)
    {
        if (left == null) {
            return null;
        }
        if (right == null) {
            return left;
        }

        if (left.equals(right)) {
            return null;
        }

        if ((left instanceof Double || right instanceof Double) && ((Number) left).doubleValue() == ((Number) right).doubleValue()) {
            return null;
        }

        return left;
    }

    @Test
    public void testTernaryOperatorsLongLong()
            throws Exception
    {
        for (Integer first : intLefts) {
            for (Integer second : intLefts) {
                for (Integer third : intRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            BOOLEAN,
                            first == null || second == null || third == null ? null : second <= first && first <= third);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testTernaryOperatorsLongDouble()
            throws Exception
    {
        for (Integer first : intLefts) {
            for (Double second : doubleLefts) {
                for (Integer third : intRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            BOOLEAN,
                            first == null || second == null || third == null ? null : second <= first && first <= third);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testTernaryOperatorsDoubleDouble()
            throws Exception
    {
        for (Double first : doubleLefts) {
            for (Double second : doubleLefts) {
                for (Integer third : intRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            BOOLEAN,
                            first == null || second == null || third == null ? null : second <= first && first <= third);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testTernaryOperatorsString()
            throws Exception
    {
        for (String first : stringLefts) {
            for (String second : stringLefts) {
                for (String third : stringRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            BOOLEAN,
                            first == null || second == null || third == null ? null : second.compareTo(first) <= 0 && first.compareTo(third) <= 0);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testTernaryOperatorsLongDecimal()
            throws Exception
    {
        for (Long first : longLefts) {
            for (BigDecimal second : decimalMiddle) {
                for (Long third : longRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            BOOLEAN,
                            first == null || second == null || third == null ? null : second.compareTo(new BigDecimal(first)) <= 0 && first <= third);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testTernaryOperatorsDecimalDouble()
            throws Exception
    {
        for (BigDecimal first : decimalLefts) {
            for (Double second : doubleMiddle) {
                for (BigDecimal third : decimalRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            BOOLEAN,
                            first == null || second == null || third == null ? null : second <= first.doubleValue() && first.compareTo(third) <= 0);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testCast()
            throws Exception
    {
        for (Boolean value : booleanValues) {
            assertExecute(generateExpression("cast(%s as boolean)", value), BOOLEAN, value == null ? null : (value ? true : false));
            assertExecute(generateExpression("cast(%s as integer)", value), INTEGER, value == null ? null : (value ? 1 : 0));
            assertExecute(generateExpression("cast(%s as bigint)", value), BIGINT, value == null ? null : (value ? 1L : 0L));
            assertExecute(generateExpression("cast(%s as double)", value), DOUBLE, value == null ? null : (value ? 1.0 : 0.0));
            assertExecute(generateExpression("cast(%s as varchar)", value), VARCHAR, value == null ? null : (value ? "true" : "false"));
        }

        for (Integer value : intLefts) {
            assertExecute(generateExpression("cast(%s as boolean)", value), BOOLEAN, value == null ? null : (value != 0L ? true : false));
            assertExecute(generateExpression("cast(%s as integer)", value), INTEGER, value == null ? null : value);
            assertExecute(generateExpression("cast(%s as bigint)", value), BIGINT, value == null ? null : (long) value);
            assertExecute(generateExpression("cast(%s as double)", value), DOUBLE, value == null ? null : value.doubleValue());
            assertExecute(generateExpression("cast(%s as varchar)", value), VARCHAR, value == null ? null : String.valueOf(value));
        }

        for (Double value : doubleLefts) {
            assertExecute(generateExpression("cast(%s as boolean)", value), BOOLEAN, value == null ? null : (value != 0.0 ? true : false));
            if (value == null || (value >= Long.MIN_VALUE && value < Long.MAX_VALUE)) {
                assertExecute(generateExpression("cast(%s as bigint)", value), BIGINT, value == null ? null : value.longValue());
            }
            assertExecute(generateExpression("cast(%s as double)", value), DOUBLE, value == null ? null : value);
            assertExecute(generateExpression("cast(%s as varchar)", value), VARCHAR, value == null ? null : String.valueOf(value));
        }

        assertExecute("cast('true' as boolean)", BOOLEAN, true);
        assertExecute("cast('true' as BOOLEAN)", BOOLEAN, true);
        assertExecute("cast('tRuE' as BOOLEAN)", BOOLEAN, true);
        assertExecute("cast('false' as BOOLEAN)", BOOLEAN, false);
        assertExecute("cast('fAlSe' as BOOLEAN)", BOOLEAN, false);
        assertExecute("cast('t' as BOOLEAN)", BOOLEAN, true);
        assertExecute("cast('T' as BOOLEAN)", BOOLEAN, true);
        assertExecute("cast('f' as BOOLEAN)", BOOLEAN, false);
        assertExecute("cast('F' as BOOLEAN)", BOOLEAN, false);
        assertExecute("cast('1' as BOOLEAN)", BOOLEAN, true);
        assertExecute("cast('0' as BOOLEAN)", BOOLEAN, false);

        for (Integer value : intLefts) {
            if (value != null) {
                assertExecute(generateExpression("cast(%s as integer)", String.valueOf(value)), INTEGER, value == null ? null : value);
                assertExecute(generateExpression("cast(%s as bigint)", String.valueOf(value)), BIGINT, value == null ? null : (long) value);
            }
        }
        for (Double value : doubleLefts) {
            if (value != null) {
                assertExecute(generateExpression("cast(%s as double)", String.valueOf(value)), DOUBLE, value == null ? null : value);
            }
        }
        for (String value : stringLefts) {
            assertExecute(generateExpression("cast(%s as varchar)", value), VARCHAR, value == null ? null : value);
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testTryCast()
            throws Exception
    {
        assertExecute("try_cast(null as integer)", INTEGER, null);
        assertExecute("try_cast('123' as integer)", INTEGER, 123);
        assertExecute("try_cast(null as bigint)", BIGINT, null);
        assertExecute("try_cast('123' as bigint)", BIGINT, 123L);
        assertExecute("try_cast('foo' as varchar)", VARCHAR, "foo");
        assertExecute("try_cast('foo' as bigint)", BIGINT, null);
        assertExecute("try_cast('foo' as integer)", INTEGER, null);
        assertExecute("try_cast('2001-08-22' as timestamp)", TIMESTAMP, sqlTimestampOf(2001, 8, 22, 0, 0, 0, 0, UTC, UTC_KEY, TEST_SESSION));
        assertExecute("try_cast(bound_string as bigint)", BIGINT, null);
        assertExecute("try_cast(cast(null as varchar) as bigint)", BIGINT, null);
        assertExecute("try_cast(bound_long / 13  as bigint)", BIGINT, 94L);
        assertExecute("coalesce(try_cast('123' as bigint), 456)", BIGINT, 123L);
        assertExecute("coalesce(try_cast('foo' as bigint), 456)", BIGINT, 456L);
        assertExecute("concat('foo', cast('bar' as varchar))", VARCHAR, "foobar");
        assertExecute("try_cast(try_cast(123 as varchar) as bigint)", BIGINT, 123L);
        assertExecute("try_cast('foo' as varchar) || try_cast('bar' as varchar)", VARCHAR, "foobar");

        Futures.allAsList(futures).get();
    }

    @Test
    public void testAnd()
            throws Exception
    {
        assertExecute("true and true", BOOLEAN, true);
        assertExecute("true and false", BOOLEAN, false);
        assertExecute("false and true", BOOLEAN, false);
        assertExecute("false and false", BOOLEAN, false);

        assertExecute("true and cast(null as boolean)", BOOLEAN, null);
        assertExecute("false and cast(null as boolean)", BOOLEAN, false);
        assertExecute("cast(null as boolean) and true", BOOLEAN, null);
        assertExecute("cast(null as boolean) and false", BOOLEAN, false);
        assertExecute("cast(null as boolean) and cast(null as boolean)", BOOLEAN, null);

        assertExecute("true and null", BOOLEAN, null);
        assertExecute("false and null", BOOLEAN, false);
        assertExecute("null and true", BOOLEAN, null);
        assertExecute("null and false", BOOLEAN, false);
        assertExecute("null and null", BOOLEAN, null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testOr()
            throws Exception
    {
        assertExecute("true or true", BOOLEAN, true);
        assertExecute("true or false", BOOLEAN, true);
        assertExecute("false or true", BOOLEAN, true);
        assertExecute("false or false", BOOLEAN, false);

        assertExecute("true or cast(null as boolean)", BOOLEAN, true);
        assertExecute("false or cast(null as boolean)", BOOLEAN, null);
        assertExecute("cast(null as boolean) or true", BOOLEAN, true);
        assertExecute("cast(null as boolean) or false", BOOLEAN, null);
        assertExecute("cast(null as boolean) or cast(null as boolean)", BOOLEAN, null);

        assertExecute("true or null", BOOLEAN, true);
        assertExecute("false or null", BOOLEAN, null);
        assertExecute("null or true", BOOLEAN, true);
        assertExecute("null or false", BOOLEAN, null);
        assertExecute("null or null", BOOLEAN, null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testNot()
            throws Exception
    {
        assertExecute("not true", BOOLEAN, false);
        assertExecute("not false", BOOLEAN, true);

        assertExecute("not cast(null as boolean)", BOOLEAN, null);

        assertExecute("not null", BOOLEAN, null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testIf()
            throws Exception
    {
        assertExecute("if(null and true, BIGINT '1', 0)", BIGINT, 0L);
        assertExecute("if(null and true, 1, 0)", INTEGER, 0);
        for (Boolean condition : booleanValues) {
            for (String trueValue : stringLefts) {
                for (String falseValue : stringRights) {
                    assertExecute(
                            generateExpression("if(%s, %s, %s)", condition, trueValue, falseValue),
                            varcharType(trueValue, falseValue),
                            condition != null && condition ? trueValue : falseValue);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testSimpleCase()
            throws Exception
    {
        for (Double value : doubleLefts) {
            for (Double firstTest : doubleMiddle) {
                for (Double secondTest : doubleRights) {
                    String expected;
                    if (value == null) {
                        expected = "else";
                    }
                    else if (firstTest != null && (double) value == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && (double) value == secondTest) {
                        expected = "second";
                    }
                    else {
                        expected = "else";
                    }
                    assertExecute(generateExpression("case %s when %s then 'first' when %s then 'second' else 'else' end", value, firstTest, secondTest), createVarcharType(6), expected);
                }
            }
        }
        for (Integer value : intLefts) {
            for (Integer firstTest : intMiddle) {
                for (Integer secondTest : intRights) {
                    String expected;
                    if (value == null) {
                        expected = null;
                    }
                    else if (firstTest != null && firstTest.equals(value)) {
                        expected = "first";
                    }
                    else if (secondTest != null && secondTest.equals(value)) {
                        expected = "second";
                    }
                    else {
                        expected = null;
                    }
                    assertExecute(generateExpression("case %s when %s then 'first' when %s then 'second' end", value, firstTest, secondTest), createVarcharType(6), expected);
                }
            }
        }

        for (BigDecimal value : decimalLefts) {
            for (BigDecimal firstTest : decimalMiddle) {
                for (BigDecimal secondTest : decimalRights) {
                    String expected;
                    if (value == null) {
                        expected = null;
                    }
                    else if (firstTest != null && firstTest.equals(value)) {
                        expected = "first";
                    }
                    else if (secondTest != null && secondTest.equals(value)) {
                        expected = "second";
                    }
                    else {
                        expected = null;
                    }
                    assertExecute(generateExpression("case %s when %s then 'first' when %s then 'second' end", value, firstTest, secondTest), createVarcharType(6), expected);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testSearchCaseSingle()
            throws Exception
    {
        // assertExecute("case when null and true then 1 else 0 end", 0L);
        for (Double value : doubleLefts) {
            for (Integer firstTest : intLefts) {
                for (Double secondTest : doubleRights) {
                    String expected;
                    if (value == null) {
                        expected = "else";
                    }
                    else if (firstTest != null && (double) value == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && (double) value == secondTest) {
                        expected = "second";
                    }
                    else {
                        expected = "else";
                    }
                    List<String> expressions = formatExpression("case when %s = %s then 'first' when %s = %s then 'second' else 'else' end",
                            Arrays.asList(value, firstTest, value, secondTest),
                            ImmutableList.of("double", "bigint", "double", "double"));
                    assertExecute(expressions, createVarcharType(6), expected);
                }
            }
        }

        for (Double value : doubleLefts) {
            for (Long firstTest : longLefts) {
                for (BigDecimal secondTest : decimalRights) {
                    String expected;
                    if (value == null) {
                        expected = "else";
                    }
                    else if (firstTest != null && (double) value == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && value == secondTest.doubleValue()) {
                        expected = "second";
                    }
                    else {
                        expected = "else";
                    }
                    List<String> expressions = formatExpression("case when %s = %s then 'first' when %s = %s then 'second' else 'else' end",
                            Arrays.asList(value, firstTest, value, secondTest),
                            ImmutableList.of("double", "bigint", "double", "decimal(1,0)"));
                    assertExecute(expressions, createVarcharType(6), expected);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testSearchCaseMultiple()
            throws Exception
    {
        for (Double value : doubleLefts) {
            for (Integer firstTest : intLefts) {
                for (Double secondTest : doubleRights) {
                    String expected;
                    if (value == null) {
                        expected = null;
                    }
                    else if (firstTest != null && (double) value == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && (double) value == secondTest) {
                        expected = "second";
                    }
                    else {
                        expected = null;
                    }
                    List<String> expressions = formatExpression("case when %s = %s then 'first' when %s = %s then 'second' end",
                            Arrays.asList(value, firstTest, value, secondTest),
                            ImmutableList.of("double", "bigint", "double", "double"));
                    assertExecute(expressions, createVarcharType(6), expected);
                }
            }
        }

        for (BigDecimal value : decimalLefts) {
            for (Long firstTest : longLefts) {
                for (Double secondTest : doubleRights) {
                    String expected;
                    if (value == null) {
                        expected = null;
                    }
                    else if (firstTest != null && value.doubleValue() == firstTest) {
                        expected = "first";
                    }
                    else if (secondTest != null && value.doubleValue() == secondTest) {
                        expected = "second";
                    }
                    else {
                        expected = null;
                    }
                    List<String> expressions = formatExpression("case when %s = %s then 'first' when %s = %s then 'second' end",
                            Arrays.asList(value, firstTest, value, secondTest),
                            ImmutableList.of("decimal(14,4)", "bigint", "decimal(14,4)", "double"));
                    assertExecute(expressions, createVarcharType(6), expected);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testIn()
            throws Exception
    {
        for (Boolean value : booleanValues) {
            assertExecute(generateExpression("%s in (true)", value), BOOLEAN, value == null ? null : value == Boolean.TRUE);
            assertExecute(generateExpression("%s in (null, true)", value), BOOLEAN, value == null ? null : value == Boolean.TRUE ? true : null);
            assertExecute(generateExpression("%s in (true, null)", value), BOOLEAN, value == null ? null : value == Boolean.TRUE ? true : null);
            assertExecute(generateExpression("%s in (false)", value), BOOLEAN, value == null ? null : value == Boolean.FALSE);
            assertExecute(generateExpression("%s in (null, false)", value), BOOLEAN, value == null ? null : value == Boolean.FALSE ? true : null);
            assertExecute(generateExpression("%s in (null)", value), BOOLEAN, null);
        }

        for (Integer value : intLefts) {
            List<Integer> testValues = Arrays.asList(33, 9, -9, -33);
            assertExecute(generateExpression("%s in (33, 9, -9, -33)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 33, 9, -9, -33)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (CAST(null AS BIGINT), 33, 9, -9, -33)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (33, null, 9, -9, -33)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (33, CAST(null AS BIGINT), 9, -9, -33)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);

            // compare a long to in containing doubles
            assertExecute(generateExpression("%s in (33, 9.0E0, -9, -33)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 33, 9.0E0, -9, -33)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (33.0E0, null, 9.0E0, -9, -33)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
        }

        for (Double value : doubleLefts) {
            List<Double> testValues = Arrays.asList(33.0, 9.0, -9.0, -33.0);
            assertExecute(generateExpression("%s in (33.0E0, 9.0E0, -9.0E0, -33.0E0)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 33.0E0, 9.0E0, -9.0E0, -33.0E0)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (33.0E0, null, 9.0E0, -9.0E0, -33.0E0)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);

            // compare a double to in containing longs
            assertExecute(generateExpression("%s in (33.0E0, 9, -9, -33.0E0)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 33.0E0, 9, -9, -33.0E0)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (33.0E0, null, 9, -9, -33.0E0)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);

            // compare to dynamically computed values
            testValues = Arrays.asList(33.0, cos(9.0), cos(-9.0), -33.0);
            assertExecute(generateExpression("cos(%s) in (33.0E0, cos(9.0E0), cos(-9.0E0), -33.0E0)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(cos(value)));
            assertExecute(generateExpression("cos(%s) in (null, 33.0E0, cos(9.0E0), cos(-9.0E0), -33.0E0)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(cos(value)) ? true : null);
        }

        for (BigDecimal value : decimalLefts) {
            List<BigDecimal> testValues = ImmutableList.of(new BigDecimal("9.0"), new BigDecimal("10.0"), new BigDecimal("-11.0"), new BigDecimal("9223372036.5477"));
            assertExecute(generateExpression("%s in (9.0, 10.0, -11.0, 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 9.0, 10.0, -11.0, 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (CAST(null AS DECIMAL(1,0)), 9.0, 10.0, -11.0, 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (9.0, CAST(null AS DECIMAL(1,0)), 10.0, -11.0, 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (9.0, null, 10.0, -11.0, 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);

            // compare a long to in containing doubles
            assertExecute(generateExpression("%s in (9.0, 10.0, CAST(-11.0 as DOUBLE), 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 9.0, 10.0, CAST(-11.0 as DOUBLE), 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (null, 9, 10.0, CAST(-11.0 as DOUBLE), 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (CAST(9.0 as DOUBLE), null, 10.0, CAST(-11.0 as DOUBLE), 9223372036.5477)", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
        }

        for (String value : stringLefts) {
            List<String> testValues = Arrays.asList("what?", "foo", "mellow", "end");
            assertExecute(generateExpression("%s in ('what?', 'foo', 'mellow', 'end')", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 'what?', 'foo', 'mellow', 'end')", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in ('what?', null, 'foo', 'mellow', 'end')", value),
                    BOOLEAN,
                    value == null ? null : testValues.contains(value) ? true : null);
        }

        // Test null-handling in default case of InCodeGenerator
        assertExecute("1 in (100, 101, if(rand()>=0, 1), if(rand()<0, 1))", BOOLEAN, true);
        assertExecute("1 in (100, 101, if(rand()<0, 1), if(rand()>=0, 1))", BOOLEAN, true);
        assertExecute("2 in (100, 101, if(rand()>=0, 1), if(rand()<0, 1))", BOOLEAN, null);
        assertExecute("2 in (100, 101, if(rand()<0, 1), if(rand()>=0, 1))", BOOLEAN, null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testHugeIn()
            throws Exception
    {
        String intValues = range(2000, 7000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertExecute("bound_integer in (1234, " + intValues + ")", BOOLEAN, true);
        assertExecute("bound_integer in (" + intValues + ")", BOOLEAN, false);

        String longValues = LongStream.range(Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 5000L)
                .mapToObj(Long::toString)
                .collect(joining(", "));
        assertExecute("bound_long in (1234, " + longValues + ")", BOOLEAN, true);
        assertExecute("bound_long in (" + longValues + ")", BOOLEAN, false);

        String doubleValues = range(2000, 7000).asDoubleStream()
                .mapToObj(this::formatDoubleToScientificNotation)
                .collect(joining(", "));
        assertExecute("bound_double in (12.34E0, " + doubleValues + ")", BOOLEAN, true);
        assertExecute("bound_double in (" + doubleValues + ")", BOOLEAN, false);

        String stringValues = range(2000, 7000)
                .mapToObj(i -> format("'%s'", i))
                .collect(joining(", "));
        assertExecute("bound_string in ('hello', " + stringValues + ")", BOOLEAN, true);
        assertExecute("bound_string in (" + stringValues + ")", BOOLEAN, false);

        String timestampValues = range(0, 2_000)
                .mapToObj(i -> format("TIMESTAMP '1970-01-01 01:01:0%s.%s+01:00'", i / 1000, i % 1000))
                .collect(joining(", "));
        assertExecute("bound_timestamp_with_timezone in (" + timestampValues + ")", BOOLEAN, true);
        assertExecute("bound_timestamp_with_timezone in (TIMESTAMP '1970-01-01 01:01:00.0+02:00')", BOOLEAN, false);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testFunctionCall()
            throws Exception
    {
        for (Integer left : intLefts) {
            for (Integer right : intRights) {
                assertExecute(generateExpression("bitwise_and(%s, %s)", left, right), BIGINT, left == null || right == null ? null : BitwiseFunctions.bitwiseAnd(left, right));
            }
        }

        for (Integer left : intLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("mod(%s, %s)", left, right), DOUBLE, left == null || right == null ? null : MathFunctions.mod(left, right));
            }
        }

        for (Double left : doubleLefts) {
            for (Integer right : intRights) {
                assertExecute(generateExpression("mod(%s, %s)", left, right), DOUBLE, left == null || right == null ? null : MathFunctions.mod(left, right));
            }
        }

        for (Double left : doubleLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("mod(%s, %s)", left, right), DOUBLE, left == null || right == null ? null : MathFunctions.mod(left, right));
            }
        }

        for (Double left : doubleLefts) {
            for (BigDecimal right : decimalRights) {
                assertExecute(generateExpression("mod(%s, %s)", left, right), DOUBLE, left == null || right == null ? null : MathFunctions.mod(left, right.doubleValue()));
            }
        }

        for (BigDecimal left : decimalLefts) {
            for (Long right : longRights) {
                assertExecute(generateExpression("power(%s, %s)", left, right), DOUBLE, left == null || right == null ? null : MathFunctions.power(left.doubleValue(), right));
            }
        }

        for (String value : stringLefts) {
            for (Integer start : intLefts) {
                for (Integer length : intRights) {
                    String expected;
                    if (value == null || start == null || length == null) {
                        expected = null;
                    }
                    else {
                        expected = StringFunctions.substr(utf8Slice(value), start, length).toStringUtf8();
                    }
                    VarcharType expectedType = value != null ? createVarcharType(value.length()) : VARCHAR;

                    assertExecute(generateExpression("substr(%s, %s, %s)", value, start, length), expectedType, expected);
                }
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testFunctionCallRegexp()
            throws Exception
    {
        for (String value : stringLefts) {
            for (String pattern : stringRights) {
                assertExecute(generateExpression("regexp_like(%s, %s)", value, pattern),
                        BOOLEAN,
                        value == null || pattern == null ? null : JoniRegexpFunctions.regexpLike(utf8Slice(value), joniRegexp(utf8Slice(pattern))));
                assertExecute(generateExpression("regexp_replace(%s, %s)", value, pattern),
                        value == null ? VARCHAR : createVarcharType(value.length()),
                        value == null || pattern == null ? null : JoniRegexpFunctions.regexpReplace(utf8Slice(value), joniRegexp(utf8Slice(pattern))));
                assertExecute(generateExpression("regexp_extract(%s, %s)", value, pattern),
                        value == null ? VARCHAR : createVarcharType(value.length()),
                        value == null || pattern == null ? null : JoniRegexpFunctions.regexpExtract(utf8Slice(value), joniRegexp(utf8Slice(pattern))));
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testFunctionCallJson()
            throws Exception
    {
        for (String value : jsonValues) {
            for (String pattern : jsonPatterns) {
                assertExecute(generateExpression("json_extract(%s, %s)", value, pattern),
                        JSON,
                        value == null || pattern == null ? null : JsonFunctions.jsonExtract(utf8Slice(value), new JsonPath(pattern)));
                assertExecute(generateExpression("json_extract_scalar(%s, %s)", value, pattern),
                        value == null ? createUnboundedVarcharType() : createVarcharType(value.length()),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtractScalar(utf8Slice(value), new JsonPath(pattern)));

                assertExecute(generateExpression("json_extract(%s, %s || '')", value, pattern),
                        JSON,
                        value == null || pattern == null ? null : JsonFunctions.jsonExtract(utf8Slice(value), new JsonPath(pattern)));
                assertExecute(generateExpression("json_extract_scalar(%s, %s || '')", value, pattern),
                        value == null ? createUnboundedVarcharType() : createVarcharType(value.length()),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtractScalar(utf8Slice(value), new JsonPath(pattern)));
            }
        }

        assertExecute("json_array_contains('[1, 2, 3]', 2)", BOOLEAN, true);
        assertExecute("json_array_contains('[1, 2, 3]', BIGINT '2')", BOOLEAN, true);
        assertExecute("json_array_contains('[2.5E0]', 2.5E0)", BOOLEAN, true);
        assertExecute("json_array_contains('[false, true]', true)", BOOLEAN, true);
        assertExecute("json_array_contains('[5]', 3)", BOOLEAN, false);
        assertExecute("json_array_contains('[', 9)", BOOLEAN, null);
        assertExecute("json_array_length('[')", BIGINT, null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testFunctionWithSessionCall()
            throws Exception
    {
        assertExecute("now()", TIMESTAMP_WITH_TIME_ZONE, new SqlTimestampWithTimeZone(TEST_SESSION.getStartTime(), TEST_SESSION.getTimeZoneKey()));
        assertExecute("current_timestamp", TIMESTAMP_WITH_TIME_ZONE, new SqlTimestampWithTimeZone(TEST_SESSION.getStartTime(), TEST_SESSION.getTimeZoneKey()));

        Futures.allAsList(futures).get();
    }

    @Test
    public void testExtract()
            throws Exception
    {
        for (DateTime left : dateTimeValues) {
            for (Field field : Field.values()) {
                Long expected = null;
                Long millis = null;
                if (left != null) {
                    millis = left.getMillis();
                    expected = callExtractFunction(TEST_SESSION.toConnectorSession(), millis, field);
                }
                assertExecute(generateExpression("extract(" + field.toString() + " from from_unixtime(%s / 1000.0E0, 0, 0))", millis), BIGINT, expected);
            }
        }

        Futures.allAsList(futures).get();
    }

    @SuppressWarnings("fallthrough")
    private static long callExtractFunction(ConnectorSession session, long value, Field field)
    {
        switch (field) {
            case YEAR:
                return DateTimeFunctions.yearFromTimestamp(session, value);
            case QUARTER:
                return DateTimeFunctions.quarterFromTimestamp(session, value);
            case MONTH:
                return DateTimeFunctions.monthFromTimestamp(session, value);
            case WEEK:
                return DateTimeFunctions.weekFromTimestamp(session, value);
            case DAY:
            case DAY_OF_MONTH:
                return DateTimeFunctions.dayFromTimestamp(session, value);
            case DAY_OF_WEEK:
            case DOW:
                return DateTimeFunctions.dayOfWeekFromTimestamp(session, value);
            case YEAR_OF_WEEK:
            case YOW:
                return DateTimeFunctions.yearOfWeekFromTimestamp(session, value);
            case DAY_OF_YEAR:
            case DOY:
                return DateTimeFunctions.dayOfYearFromTimestamp(session, value);
            case HOUR:
                return DateTimeFunctions.hourFromTimestamp(session, value);
            case MINUTE:
                return DateTimeFunctions.minuteFromTimestamp(session, value);
            case SECOND:
                return DateTimeFunctions.secondFromTimestamp(value);
            case TIMEZONE_MINUTE:
                return DateTimeFunctions.timeZoneMinuteFromTimestampWithTimeZone(packDateTimeWithZone(value, session.getTimeZoneKey()));
            case TIMEZONE_HOUR:
                return DateTimeFunctions.timeZoneHourFromTimestampWithTimeZone(packDateTimeWithZone(value, session.getTimeZoneKey()));
        }
        throw new AssertionError("Unhandled field: " + field);
    }

    @Test
    public void testLike()
            throws Exception
    {
        for (String value : stringLefts) {
            for (String pattern : stringLefts) {
                Boolean expected = null;
                if (value != null && pattern != null) {
                    Regex regex = LikeFunctions.likePattern(utf8Slice(pattern), utf8Slice("\\"));
                    expected = LikeFunctions.likeVarchar(utf8Slice(value), regex);
                }
                assertExecute(generateExpression("%s like %s", value, pattern), BOOLEAN, expected);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testCoalesce()
            throws Exception
    {
        assertExecute("coalesce(9, 1)", INTEGER, 9);
        assertExecute("coalesce(9, null)", INTEGER, 9);
        assertExecute("coalesce(9, BIGINT '1')", BIGINT, 9L);
        assertExecute("coalesce(BIGINT '9', null)", BIGINT, 9L);
        assertExecute("coalesce(9, cast(null as bigint))", BIGINT, 9L);
        assertExecute("coalesce(null, 9, 1)", INTEGER, 9);
        assertExecute("coalesce(null, 9, null)", INTEGER, 9);
        assertExecute("coalesce(null, 9, BIGINT '1')", BIGINT, 9L);
        assertExecute("coalesce(null, 9, CAST (null AS BIGINT))", BIGINT, 9L);
        assertExecute("coalesce(null, 9, cast(null as bigint))", BIGINT, 9L);
        assertExecute("coalesce(cast(null as bigint), 9, 1)", BIGINT, 9L);
        assertExecute("coalesce(cast(null as bigint), 9, null)", BIGINT, 9L);
        assertExecute("coalesce(cast(null as bigint), 9, cast(null as bigint))", BIGINT, 9L);

        assertExecute("coalesce(9.0E0, 1.0E0)", DOUBLE, 9.0);
        assertExecute("coalesce(9.0E0, 1)", DOUBLE, 9.0);
        assertExecute("coalesce(9.0E0, null)", DOUBLE, 9.0);
        assertExecute("coalesce(9.0E0, cast(null as double))", DOUBLE, 9.0);
        assertExecute("coalesce(null, 9.0E0, 1)", DOUBLE, 9.0);
        assertExecute("coalesce(null, 9.0E0, null)", DOUBLE, 9.0);
        assertExecute("coalesce(null, 9.0E0, cast(null as double))", DOUBLE, 9.0);
        assertExecute("coalesce(null, 9.0E0, cast(null as bigint))", DOUBLE, 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0E0, 1)", DOUBLE, 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0E0, null)", DOUBLE, 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0E0, cast(null as bigint))", DOUBLE, 9.0);
        assertExecute("coalesce(cast(null as double), 9.0E0, cast(null as double))", DOUBLE, 9.0);

        assertExecute("coalesce('foo', 'banana')", createVarcharType(6), "foo");
        assertExecute("coalesce('foo', null)", createVarcharType(3), "foo");
        assertExecute("coalesce('foo', cast(null as varchar))", VARCHAR, "foo");
        assertExecute("coalesce(null, 'foo', 'banana')", createVarcharType(6), "foo");
        assertExecute("coalesce(null, 'foo', null)", createVarcharType(3), "foo");
        assertExecute("coalesce(null, 'foo', cast(null as varchar))", VARCHAR, "foo");
        assertExecute("coalesce(cast(null as varchar), 'foo', 'bar')", VARCHAR, "foo");
        assertExecute("coalesce(cast(null as varchar), 'foo', null)", VARCHAR, "foo");
        assertExecute("coalesce(cast(null as varchar), 'foo', cast(null as varchar))", VARCHAR, "foo");

        assertExecute("coalesce(cast(null as bigint), null, cast(null as bigint))", BIGINT, null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testNullif()
            throws Exception
    {
        assertExecute("nullif(NULL, NULL)", UNKNOWN, null);
        assertExecute("nullif(NULL, 2)", UNKNOWN, null);
        assertExecute("nullif(2, NULL)", INTEGER, 2);
        assertExecute("nullif(BIGINT '2', NULL)", BIGINT, 2L);

        // Test coercion in which the CAST function takes ConnectorSession (e.g. MapToMapCast)
        assertExecute("nullif(" +
                        "map(array[1], array[smallint '1']), " +
                        "map(array[1], array[integer '1']))",
                mapType(INTEGER, SMALLINT),
                null);

        Futures.allAsList(futures).get();
    }

    private List<String> generateExpression(String expressionPattern, Boolean value)
    {
        return formatExpression(expressionPattern, value, "boolean");
    }

    private List<String> generateExpression(String expressionPattern, Long value)
    {
        return formatExpression(expressionPattern, value, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Integer value)
    {
        return formatExpression(expressionPattern, value, "integer");
    }

    private List<String> generateExpression(String expressionPattern, Double value)
    {
        return formatExpression(expressionPattern, value, "double");
    }

    private List<String> generateExpression(String expressionPattern, String value)
    {
        return formatExpression(expressionPattern, value, "varchar");
    }

    private List<String> generateExpression(String expressionPattern, BigDecimal value)
    {
        return formatExpression(expressionPattern, value, getDecimalType(value).toString());
    }

    private List<String> generateExpression(String expressionPattern, Boolean left, Boolean right)
    {
        return formatExpression(expressionPattern, left, "boolean", right, "boolean");
    }

    private List<String> generateExpression(String expressionPattern, Long left, Long right)
    {
        return formatExpression(expressionPattern, left, "bigint", right, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Long left, Integer right)
    {
        return formatExpression(expressionPattern, left, "bigint", right, "integer");
    }

    private List<String> generateExpression(String expressionPattern, Integer left, Integer right)
    {
        return formatExpression(expressionPattern, left, "integer", right, "integer");
    }

    private List<String> generateExpression(String expressionPattern, Long left, Double right)
    {
        return formatExpression(expressionPattern, left, "bigint", right, "double");
    }

    private List<String> generateExpression(String expressionPattern, Integer left, Double right)
    {
        return formatExpression(expressionPattern, left, "integer", right, "double");
    }

    private List<String> generateExpression(String expressionPattern, Double left, Long right)
    {
        return formatExpression(expressionPattern, left, "double", right, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Double left, Integer right)
    {
        return formatExpression(expressionPattern, left, "double", right, "integer");
    }

    private List<String> generateExpression(String expressionPattern, Double left, Double right)
    {
        return formatExpression(expressionPattern, left, "double", right, "double");
    }

    private List<String> generateExpression(String expressionPattern, Long left, BigDecimal right)
    {
        return formatExpression(expressionPattern, left, "bigint", right, getDecimalType(right).toString());
    }

    private List<String> generateExpression(String expressionPattern, BigDecimal left, Long right)
    {
        return formatExpression(expressionPattern, left, getDecimalType(left).toString(), right, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Integer left, BigDecimal right)
    {
        return formatExpression(expressionPattern, left, "integer", right, getDecimalType(right).toString());
    }

    private List<String> generateExpression(String expressionPattern, BigDecimal left, Integer right)
    {
        return formatExpression(expressionPattern, left, getDecimalType(left).toString(), right, "integer");
    }

    private List<String> generateExpression(String expressionPattern, Double left, BigDecimal right)
    {
        return formatExpression(expressionPattern, left, "double", right, getDecimalType(right).toString());
    }

    private List<String> generateExpression(String expressionPattern, BigDecimal left, Double right)
    {
        return formatExpression(expressionPattern, left, getDecimalType(left).toString(), right, "double");
    }

    private List<String> generateExpression(String expressionPattern, String left, String right)
    {
        return formatExpression(expressionPattern, left, "varchar", right, "varchar");
    }

    private List<String> generateExpression(String expressionPattern, Long first, Long second, Long third)
    {
        return formatExpression(expressionPattern, first, "bigint", second, "bigint", third, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Integer first, Integer second, Integer third)
    {
        return formatExpression(expressionPattern, first, "integer", second, "integer", third, "integer");
    }

    private List<String> generateExpression(String expressionPattern, BigDecimal first, BigDecimal second, BigDecimal third)
    {
        return formatExpression(expressionPattern, first, getDecimalType(first).toString(), second, getDecimalType(second).toString(), third, getDecimalType(third).toString());
    }

    private List<String> generateExpression(String expressionPattern, Long first, Double second, Long third)
    {
        return formatExpression(expressionPattern, first, "bigint", second, "double", third, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Integer first, Double second, Integer third)
    {
        return formatExpression(expressionPattern, first, "integer", second, "double", third, "integer");
    }

    private List<String> generateExpression(String expressionPattern, Double first, Double second, Double third)
    {
        return formatExpression(expressionPattern, first, "double", second, "double", third, "double");
    }

    private List<String> generateExpression(String expressionPattern, Double first, Double second, Long third)
    {
        return formatExpression(expressionPattern, first, "double", second, "double", third, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Double first, Double second, Integer third)
    {
        return formatExpression(expressionPattern, first, "double", second, "double", third, "integer");
    }

    private List<String> generateExpression(String expressionPattern, Double first, Long second, Double third)
    {
        return formatExpression(expressionPattern, first, "double", second, "bigint", third, "double");
    }

    private List<String> generateExpression(String expressionPattern, String first, String second, String third)
    {
        return formatExpression(expressionPattern, first, "varchar", second, "varchar", third, "varchar");
    }

    private List<String> generateExpression(String expressionPattern, Boolean first, String second, String third)
    {
        return formatExpression(expressionPattern, first, "boolean", second, "varchar", third, "varchar");
    }

    private List<String> generateExpression(String expressionPattern, String first, Long second, Long third)
    {
        return formatExpression(expressionPattern, first, "varchar", second, "bigint", third, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, String first, Integer second, Integer third)
    {
        return formatExpression(expressionPattern, first, "varchar", second, "integer", third, "integer");
    }

    private List<String> generateExpression(String expressionPattern, Long first, BigDecimal second, Long third)
    {
        return formatExpression(expressionPattern, first, "bigint", second, "decimal(3,1)", third, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, BigDecimal first, Double second, BigDecimal third)
    {
        return formatExpression(expressionPattern, first, getDecimalType(first).toString(), second, "double", third, getDecimalType(third).toString());
    }

    private static List<String> formatExpression(String expressionPattern, Object value, String type)
    {
        return formatExpression(expressionPattern,
                Arrays.asList(value),
                ImmutableList.of(type));
    }

    private static List<String> formatExpression(String expressionPattern, Object left, final String leftType, Object right, final String rightType)
    {
        return formatExpression(expressionPattern,
                Arrays.asList(left, right),
                ImmutableList.of(leftType, rightType));
    }

    private static List<String> formatExpression(String expressionPattern,
            Object first, String firstType,
            Object second, String secondType,
            Object third, String thirdType)
    {
        return formatExpression(expressionPattern,
                Arrays.asList(first, second, third),
                ImmutableList.of(firstType, secondType, thirdType));
    }

    private static List<String> formatExpression(String expressionPattern, List<Object> values, List<String> types)
    {
        Preconditions.checkArgument(values.size() == types.size());

        List<Set<String>> unrolledValues = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            String type = types.get(i);
            if (value != null) {
                if (type.equals("varchar")) {
                    value = "'" + value + "'";
                }
                else if (type.equals("bigint")) {
                    value = "CAST( " + value + " AS BIGINT)";
                }
                else if (type.equals("double")) {
                    value = "CAST( " + value + " AS DOUBLE)";
                }
                unrolledValues.add(ImmutableSet.of(String.valueOf(value)));
            }
            else {
                // todo enable when null output type is supported
                // unrolledValues.add(ImmutableSet.of("null", "cast(null as " + type + ")"));
                unrolledValues.add(ImmutableSet.of("cast(null as " + type + ")"));
            }
        }

        ImmutableList.Builder<String> expressions = ImmutableList.builder();
        Set<List<String>> valueLists = Sets.cartesianProduct(unrolledValues);
        for (List<String> valueList : valueLists) {
            expressions.add(format(expressionPattern, valueList.toArray(new Object[valueList.size()])));
        }
        return expressions.build();
    }

    private String formatDoubleToScientificNotation(Double value)
    {
        DecimalFormat formatter = ((DecimalFormat) NumberFormat.getNumberInstance(Locale.US));
        formatter.applyPattern("0.##############E0");
        return formatter.format(value);
    }

    private void assertExecute(String expression, Type expectedType, Object expected)
    {
        addCallable(new AssertExecuteTask(functionAssertions, expression, expectedType, expected));
    }

    private void addCallable(Runnable runnable)
    {
        if (PARALLEL) {
            futures.add(executor.submit(runnable));
        }
        else {
            runnable.run();
        }
    }

    private void assertExecute(List<String> expressions, Type expectedType, Object expected)
    {
        if (expected instanceof Slice) {
            expected = ((Slice) expected).toStringUtf8();
        }
        for (String expression : expressions) {
            assertExecute(expression, expectedType, expected);
        }
    }

    private void assertExecute(List<String> expressions, BigDecimal decimal)
    {
        Type type = getDecimalType(decimal);
        SqlDecimal value = decimal == null ? null : new SqlDecimal(decimal.unscaledValue(), decimal.precision(), decimal.scale());
        for (String expression : expressions) {
            assertExecute(expression, type, value);
        }
    }

    private static Type getDecimalType(BigDecimal decimal)
    {
        if (decimal == null) {
            return createDecimalType(1, 0);
        }
        return createDecimalType(decimal.precision(), decimal.scale());
    }

    private static class AssertExecuteTask
            implements Runnable
    {
        private final FunctionAssertions functionAssertions;
        private final String expression;
        private final Type expectedType;
        private final Object expected;

        public AssertExecuteTask(FunctionAssertions functionAssertions, String expression, Type expectedType, Object expected)
        {
            this.functionAssertions = functionAssertions;
            this.expectedType = expectedType;
            this.expression = expression;
            this.expected = expected;
        }

        @Override
        public void run()
        {
            try {
                functionAssertions.assertFunction(expression, expectedType, expected);
            }
            catch (Throwable e) {
                throw new RuntimeException("Error processing " + expression, e);
            }
        }
    }

    private void assertFilterWithNoInputColumns(String filter, boolean expected)
    {
        addCallable(new AssertFilterTask(functionAssertions, filter, expected, true));
    }

    private void assertFilter(String filter, boolean expected)
    {
        addCallable(new AssertFilterTask(functionAssertions, filter, expected, false));
    }

    private static class AssertFilterTask
            implements Runnable
    {
        private final FunctionAssertions functionAssertions;
        private final String filter;
        private final boolean expected;
        private final boolean withNoInputColumns;

        public AssertFilterTask(FunctionAssertions functionAssertions, String filter, boolean expected, boolean withNoInputColumns)
        {
            this.functionAssertions = functionAssertions;
            this.filter = filter;
            this.expected = expected;
            this.withNoInputColumns = withNoInputColumns;
        }

        @Override
        public void run()
        {
            try {
                functionAssertions.assertFilter(filter, expected, withNoInputColumns);
            }
            catch (Throwable e) {
                throw new RuntimeException("Error processing " + filter, e);
            }
        }
    }
}
