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

import com.facebook.presto.operator.scalar.DateTimeFunctions;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.JsonPath;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.RegexpFunctions;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.sql.tree.Extract.Field;
import com.facebook.presto.type.LikeFunctions;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.joni.Regex;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.cos;
import static java.lang.Runtime.getRuntime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExpressionCompiler
{
    private static final Boolean[] booleanValues = {true, false, null};
    private static final Long[] longLefts = {9L, 10L, 11L, -9L, -10L, -11L, 10151082135029368L, /*Long.MIN_VALUE,*/ Long.MAX_VALUE, null};
    private static final Long[] longRights = {3L, -3L, 10151082135029369L, null};
    private static final Long[] longMiddle = {9L, -3L, 88L, null};
    private static final Double[] doubleLefts = {9.0, 10.0, 11.0, -9.0, -10.0, -11.0, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1,
                                                 Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_NORMAL, null};
    private static final Double[] doubleRights = {3.0, -3.0, 3.1, -3.1, null};
    private static final Double[] doubleMiddle = {9.0, -3.1, 88.0, null};
    private static final String[] stringLefts = {"hello", "foo", "mellow", "fellow", "", null};
    private static final String[] stringRights = {"hello", "foo", "bar", "baz", "", null};

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
    private static final boolean PARALLEL = true;

    private long start;
    private ListeningExecutorService executor;
    private FunctionAssertions functionAssertions;
    private List<ListenableFuture<Void>> futures;

    @BeforeSuite
    public void setupClass()
    {
        Logging.initialize();
        if (PARALLEL) {
            executor = listeningDecorator(newFixedThreadPool(getRuntime().availableProcessors() * 2, daemonThreadsNamed("completer-%s")));
        }
        else {
            executor = listeningDecorator(sameThreadExecutor());
        }
        functionAssertions = new FunctionAssertions();
    }

    @AfterSuite
    public void tearDownClass()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @BeforeMethod
    public void setUp()
    {
        start = System.nanoTime();
        futures = new ArrayList<>();
    }

    @AfterMethod
    public void tearDown(Method method)
            throws Exception
    {
        assertTrue(Futures.allAsList(futures).isDone(), "Expression test futures are not complete");
        log.info("FINISHED %s in %s verified %s expressions", method.getName(), Duration.nanosSince(start), futures.size());
    }

    @Test
    public void smokedTest()
            throws Exception
    {
        assertExecute("cast(true as boolean)", true);
        assertExecute("true", true);
        assertExecute("false", false);
        assertExecute("42", 42L);
        assertExecute("'foo'", "foo");
        assertExecute("4.2", 4.2);
        assertExecute("1 + 1", 2L);
        assertExecute("bound_long", 1234L);
        assertExecute("bound_string", "hello");
        assertExecute("bound_double", 12.34);
        assertExecute("bound_boolean", true);
        assertExecute("bound_timestamp", new DateTime(2001, 8, 22, 3, 4, 5, 321, UTC).getMillis());
        assertExecute("bound_pattern", "%el%");
        assertExecute("bound_null_string", null);

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
        assertFilter("bound_long = 1234", true);
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
        assertExecute("cast(null as boolean) is null", true);

        for (Boolean value : booleanValues) {
            assertExecute(generateExpression("%s", value), value == null ? null : (value ? true : false));
            assertExecute(generateExpression("%s is null", value), (value == null ? true : false));
            assertExecute(generateExpression("%s is not null", value), (value != null ? true : false));
        }

        for (Long value : longLefts) {
            assertExecute(generateExpression("%s", value), value == null ? null : value);
            assertExecute(generateExpression("- (%s)", value), value == null ? null : -value);
            assertExecute(generateExpression("%s is null", value), (value == null ? true : false));
            assertExecute(generateExpression("%s is not null", value), (value != null ? true : false));
        }

        for (Double value : doubleLefts) {
            assertExecute(generateExpression("%s", value), value == null ? null : value);
            assertExecute(generateExpression("- (%s)", value), value == null ? null : -value);
            assertExecute(generateExpression("%s is null", value), (value == null ? true : false));
            assertExecute(generateExpression("%s is not null", value), (value != null ? true : false));
        }

        for (String value : stringLefts) {
            assertExecute(generateExpression("%s", value), value == null ? null : value);
            assertExecute(generateExpression("%s is null", value), (value == null ? true : false));
            assertExecute(generateExpression("%s is not null", value), (value != null ? true : false));
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
        assertExecute("nullif(cast(null as boolean), true)", null);
        for (Boolean left : booleanValues) {
            for (Boolean right : booleanValues) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : left == right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : left != right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right));
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsLongLong()
            throws Exception
    {
        for (Long left : longLefts) {
            for (Long right : longRights) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : (long) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : (long) left != right);
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : (long) left > right);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : (long) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : (long) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : (long) left <= right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right));

                assertExecute(generateExpression("%s + %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), left == null || right == null ? null : left % right);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsLongDouble()
            throws Exception
    {
        for (Long left : longLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : (double) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : (double) left != right);
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : (double) left > right);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : (double) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : (double) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : (double) left <= right);

                Object expectedNullIf = nullIf(left, right);
                for (String expression : generateExpression("nullif(%s, %s)", left, right)) {
                    functionAssertions.assertFunction(expression, expectedNullIf);
                }

                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left == null ? null : left.doubleValue(), right));

                assertExecute(generateExpression("%s + %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), left == null || right == null ? null : left % right);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testBinaryOperatorsDoubleLong()
            throws Exception
    {
        for (Double left : doubleLefts) {
            for (Long right : longRights) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : left == (double) right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : left != (double) right);
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : left > (double) right);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : left < (double) right);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : left >= (double) right);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : left <= (double) right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right == null ? null : right.doubleValue()));

                assertExecute(generateExpression("%s + %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), left == null || right == null ? null : left % right);
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
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : (double) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : (double) left != right);
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : (double) left > right);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : (double) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : (double) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : (double) left <= right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right));

                assertExecute(generateExpression("%s + %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), left == null || right == null ? null : left % right);
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
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : left.equals(right));
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : !left.equals(right));
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : left.compareTo(right) > 0);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : left.compareTo(right) < 0);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : left.compareTo(right) >= 0);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : left.compareTo(right) <= 0);

                assertExecute(generateExpression("%s || %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right));

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(left, right));
            }
        }

        Futures.allAsList(futures).get();
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
        for (Long first : longLefts) {
            for (Long second : longLefts) {
                for (Long third : longRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
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
        for (Long first : longLefts) {
            for (Double second : doubleLefts) {
                for (Long third : longRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
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
                for (Long third : longRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
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
                            first == null || second == null || third == null ? null : second.compareTo(first) <= 0 && first.compareTo(third) <= 0);
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
            assertExecute(generateExpression("cast(%s as boolean)", value), value == null ? null : (value ? true : false));
            assertExecute(generateExpression("cast(%s as bigint)", value), value == null ? null : (value ? 1L : 0L));
            assertExecute(generateExpression("cast(%s as double)", value), value == null ? null : (value ? 1.0 : 0.0));
            assertExecute(generateExpression("cast(%s as varchar)", value), value == null ? null : (value ? "true" : "false"));
        }

        for (Long value : longLefts) {
            assertExecute(generateExpression("cast(%s as boolean)", value), value == null ? null : (value != 0L ? true : false));
            assertExecute(generateExpression("cast(%s as bigint)", value), value == null ? null : value);
            assertExecute(generateExpression("cast(%s as double)", value), value == null ? null : value.doubleValue());
            assertExecute(generateExpression("cast(%s as varchar)", value), value == null ? null : String.valueOf(value));
        }

        for (Double value : doubleLefts) {
            assertExecute(generateExpression("cast(%s as boolean)", value), value == null ? null : (value != 0.0 ? true : false));
            assertExecute(generateExpression("cast(%s as bigint)", value), value == null ? null : value.longValue());
            assertExecute(generateExpression("cast(%s as double)", value), value == null ? null : value);
            assertExecute(generateExpression("cast(%s as varchar)", value), value == null ? null : String.valueOf(value));
        }

        assertExecute("cast('true' as boolean)", true);
        assertExecute("cast('true' as BOOLEAN)", true);
        assertExecute("cast('tRuE' as BOOLEAN)", true);
        assertExecute("cast('false' as BOOLEAN)", false);
        assertExecute("cast('fAlSe' as BOOLEAN)", false);
        assertExecute("cast('t' as BOOLEAN)", true);
        assertExecute("cast('T' as BOOLEAN)", true);
        assertExecute("cast('f' as BOOLEAN)", false);
        assertExecute("cast('F' as BOOLEAN)", false);
        assertExecute("cast('1' as BOOLEAN)", true);
        assertExecute("cast('0' as BOOLEAN)", false);

        for (Long value : longLefts) {
            if (value != null) {
                assertExecute(generateExpression("cast(%s as bigint)", String.valueOf(value)), value == null ? null : value);
            }
        }
        for (Double value : doubleLefts) {
            if (value != null) {
                assertExecute(generateExpression("cast(%s as double)", String.valueOf(value)), value == null ? null : value);
            }
        }
        for (String value : stringLefts) {
            assertExecute(generateExpression("cast(%s as varchar)", value), value == null ? null : value);
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testTryCast()
            throws Exception
    {
        assertExecute("try_cast(null as bigint)", null);
        assertExecute("try_cast('123' as bigint)", 123L);
        assertExecute("try_cast('foo' as varchar)", "foo");
        assertExecute("try_cast('foo' as bigint)", null);
        assertExecute("try_cast(bound_string as bigint)", null);
        assertExecute("try_cast(cast(null as varchar) as bigint)", null);
        assertExecute("try_cast(bound_long / 13  as bigint)", 94);
        assertExecute("coalesce(try_cast('123' as bigint), 456)", 123L);
        assertExecute("coalesce(try_cast('foo' as bigint), 456)", 456L);
        assertExecute("concat('foo', cast('bar' as varchar))", "foobar");
        assertExecute("try_cast(try_cast(123 as varchar) as bigint)", 123L);
        assertExecute("try_cast('foo' as varchar) || try_cast('bar' as varchar)", "foobar");

        Futures.allAsList(futures).get();
    }

    @Test
    public void testAnd()
            throws Exception
    {
        assertExecute("true and true", true);
        assertExecute("true and false", false);
        assertExecute("false and true", false);
        assertExecute("false and false", false);

        assertExecute("true and cast(null as boolean)", null);
        assertExecute("false and cast(null as boolean)", false);
        assertExecute("cast(null as boolean) and true", null);
        assertExecute("cast(null as boolean) and false", false);
        assertExecute("cast(null as boolean) and cast(null as boolean)", null);

        assertExecute("true and null", null);
        assertExecute("false and null", false);
        assertExecute("null and true", null);
        assertExecute("null and false", false);
        assertExecute("null and null", null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testOr()
            throws Exception
    {
        assertExecute("true or true", true);
        assertExecute("true or false", true);
        assertExecute("false or true", true);
        assertExecute("false or false", false);

        assertExecute("true or cast(null as boolean)", true);
        assertExecute("false or cast(null as boolean)", null);
        assertExecute("cast(null as boolean) or true", true);
        assertExecute("cast(null as boolean) or false", null);
        assertExecute("cast(null as boolean) or cast(null as boolean)", null);

        assertExecute("true or null", true);
        assertExecute("false or null", null);
        assertExecute("null or true", true);
        assertExecute("null or false", null);
        assertExecute("null or null", null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testNot()
            throws Exception
    {
        assertExecute("not true", false);
        assertExecute("not false", true);

        assertExecute("not cast(null as boolean)", null);

        assertExecute("not null", null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testIf()
            throws Exception
    {
        // todo enable when null output type is supported
        //assertExecute("if(null and true, 1, 0)", 0L);
        for (Boolean condition : booleanValues) {
            for (String trueValue : stringLefts) {
                for (String falseValue : stringRights) {
                    assertExecute(generateExpression("if(%s, %s, %s)", condition, trueValue, falseValue), condition != null && condition ? trueValue : falseValue);
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
                    assertExecute(generateExpression("case %s when %s then 'first' when %s then 'second' else 'else' end", value, firstTest, secondTest), expected);
                }
            }
        }
        for (Long value : longLefts) {
            for (Long firstTest : longMiddle) {
                for (Long secondTest : longRights) {
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
                    assertExecute(generateExpression("case %s when %s then 'first' when %s then 'second' end", value, firstTest, secondTest), expected);
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
            for (Long firstTest : longLefts) {
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
                            Arrays.<Object>asList(value, firstTest, value, secondTest),
                            ImmutableList.of("double", "bigint", "double", "double"));
                    assertExecute(expressions, expected);
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
            for (Long firstTest : longLefts) {
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
                            Arrays.<Object>asList(value, firstTest, value, secondTest),
                            ImmutableList.of("double", "bigint", "double", "double"));
                    assertExecute(expressions, expected);
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
            assertExecute(generateExpression("%s in (true)", value), value == null ? null : value == Boolean.TRUE);
            assertExecute(generateExpression("%s in (null, true)", value), value == null ? null : value == Boolean.TRUE ? true : null);
            assertExecute(generateExpression("%s in (true, null)", value), value == null ? null : value == Boolean.TRUE ? true : null);
            assertExecute(generateExpression("%s in (false)", value), value == null ? null : value == Boolean.FALSE);
            assertExecute(generateExpression("%s in (null, false)", value), value == null ? null : value == Boolean.FALSE ? true : null);
            assertExecute(generateExpression("%s in (null)", value), null);
        }

        for (Long value : longLefts) {
            List<Long> testValues = Arrays.asList(33L, 9L, -9L, -33L);
            assertExecute(generateExpression("%s in (33, 9, -9, -33)", value),
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 33, 9, -9, -33)", value),
                    value == null ? null : testValues.contains(value) ? true : null);

            assertExecute(generateExpression("%s in (33, null, 9, -9, -33)", value),
                    value == null ? null : testValues.contains(value) ? true : null);

            // compare a long to in containing doubles
            assertExecute(generateExpression("%s in (33, 9.0, -9, -33)", value),
                     value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 33, 9.0, -9, -33)", value),
                     value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (33.0, null, 9.0, -9, -33)", value),
                     value == null ? null : testValues.contains(value) ? true : null);

        }

        for (Double value : doubleLefts) {
            List<Double> testValues = Arrays.asList(33.0, 9.0, -9.0, -33.0);
            assertExecute(generateExpression("%s in (33.0, 9.0, -9.0, -33.0)", value),
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 33.0, 9.0, -9.0, -33.0)", value),
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (33.0, null, 9.0, -9.0, -33.0)", value),
                    value == null ? null : testValues.contains(value) ? true : null);

            // compare a double to in containing longs
            assertExecute(generateExpression("%s in (33.0, 9, -9, -33.0)", value),
                     value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 33.0, 9, -9, -33.0)", value),
                     value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in (33.0, null, 9, -9, -33.0)", value),
                     value == null ? null : testValues.contains(value) ? true : null);

            // compare to dynamically computed values
            testValues = Arrays.asList(33.0, cos(9.0), cos(-9.0), -33.0);
            assertExecute(generateExpression("cos(%s) in (33.0, cos(9.0), cos(-9.0), -33.0)", value),
                    value == null ? null : testValues.contains(cos(value)));
            assertExecute(generateExpression("cos(%s) in (null, 33.0, cos(9.0), cos(-9.0), -33.0)", value),
                    value == null ? null : testValues.contains(cos(value)) ? true : null);
        }

        for (String value : stringLefts) {
            List<String> testValues = Arrays.asList("what?", "foo", "mellow", "end");
            assertExecute(generateExpression("%s in ('what?', 'foo', 'mellow', 'end')", value),
                    value == null ? null : testValues.contains(value));
            assertExecute(generateExpression("%s in (null, 'what?', 'foo', 'mellow', 'end')", value),
                    value == null ? null : testValues.contains(value) ? true : null);
            assertExecute(generateExpression("%s in ('what?', null, 'foo', 'mellow', 'end')", value),
                    value == null ? null : testValues.contains(value) ? true : null);
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testHugeIn()
            throws Exception
    {
        ContiguousSet<Integer> longValues = ContiguousSet.create(Range.openClosed(2000, 7000), DiscreteDomain.integers());
        assertExecute("bound_long in (1234, " + Joiner.on(", ").join(longValues) + ")", true);
        assertExecute("bound_long in (" + Joiner.on(", ").join(longValues) + ")", false);

        Iterable<Object> doubleValues = transform(ContiguousSet.create(Range.openClosed(2000, 7000), DiscreteDomain.integers()), i -> (double) i);
        assertExecute("bound_double in (12.34, " + Joiner.on(", ").join(doubleValues) + ")", true);
        assertExecute("bound_double in (" + Joiner.on(", ").join(doubleValues) + ")", false);

        Iterable<Object> stringValues = transform(ContiguousSet.create(Range.openClosed(2000, 7000), DiscreteDomain.integers()), i -> "'" + i + "'");
        assertExecute("bound_string in ('hello', " + Joiner.on(", ").join(stringValues) + ")", true);
        assertExecute("bound_string in (" + Joiner.on(", ").join(stringValues) + ")", false);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testFunctionCall()
            throws Exception
    {
        for (Long left : longLefts) {
            for (Long right : longRights) {
                assertExecute(generateExpression("log(%s, %s)", left, right), left == null || right == null ? null : MathFunctions.log(left, right));
            }
        }

        for (Long left : longLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("log(%s, %s)", left, right), left == null || right == null ? null : MathFunctions.log(left, right));
            }
        }

        for (Double left : doubleLefts) {
            for (Long right : longRights) {
                assertExecute(generateExpression("log(%s, %s)", left, right), left == null || right == null ? null : MathFunctions.log(left, right));
            }
        }

        for (Double left : doubleLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("log(%s, %s)", left, right), left == null || right == null ? null : MathFunctions.log(left, right));
            }
        }

        for (String value : stringLefts) {
            for (Long start : longLefts) {
                for (Long length : longRights) {
                    String expected;
                    if (value == null || start == null || length == null) {
                        expected = null;
                    }
                    else {
                        expected = StringFunctions.substr(Slices.copiedBuffer(value, UTF_8), start, length).toString(UTF_8);
                    }
                    assertExecute(generateExpression("substr(%s, %s, %s)", value, start, length), expected);
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
                        value == null || pattern == null ? null : RegexpFunctions.regexpLike(Slices.utf8Slice(value), RegexpFunctions.castToRegexp(Slices.utf8Slice(pattern))));
                assertExecute(generateExpression("regexp_replace(%s, %s)", value, pattern),
                        value == null || pattern == null ? null : RegexpFunctions.regexpReplace(Slices.utf8Slice(value), RegexpFunctions.castToRegexp(Slices.utf8Slice(pattern))));
                assertExecute(generateExpression("regexp_extract(%s, %s)", value, pattern),
                        value == null || pattern == null ? null : RegexpFunctions.regexpExtract(Slices.utf8Slice(value), RegexpFunctions.castToRegexp(Slices.utf8Slice(pattern))));
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
                        value == null || pattern == null ? null : JsonFunctions.jsonExtract(Slices.copiedBuffer(value, UTF_8), new JsonPath(pattern)));
                assertExecute(generateExpression("json_extract_scalar(%s, %s)", value, pattern),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtractScalar(Slices.copiedBuffer(value, UTF_8), new JsonPath(pattern)));

                assertExecute(generateExpression("json_extract(%s, %s || '')", value, pattern),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtract(Slices.copiedBuffer(value, UTF_8), new JsonPath(pattern)));
                assertExecute(generateExpression("json_extract_scalar(%s, %s || '')", value, pattern),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtractScalar(Slices.copiedBuffer(value, UTF_8), new JsonPath(pattern)));
            }
        }

        assertExecute("json_array_contains('[1, 2, 3]', 2)", true);
        assertExecute("json_array_contains('[2.5]', 2.5)", true);
        assertExecute("json_array_contains('[false, true]', true)", true);
        assertExecute("json_array_contains('[5]', 3)", false);
        assertExecute("json_array_contains('[', 9)", null);
        assertExecute("json_array_length('[')", null);

        Futures.allAsList(futures).get();
    }

    @Test
    public void testFunctionWithSessionCall()
            throws Exception
    {
        assertExecute("now()", new SqlTimestampWithTimeZone(TEST_SESSION.getStartTime(), TEST_SESSION.getTimeZoneKey()));
        assertExecute("current_timestamp", new SqlTimestampWithTimeZone(TEST_SESSION.getStartTime(), TEST_SESSION.getTimeZoneKey()));

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
                assertExecute(generateExpression("extract(" + field.toString() + " from from_unixtime(%s / 1000.0, 0, 0))", millis), expected);
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
                    expected = LikeFunctions.like(Slices.copiedBuffer(value, UTF_8), regex);
                }
                assertExecute(generateExpression("%s like %s", value, pattern), expected);
            }
        }

        Futures.allAsList(futures).get();
    }

    @Test
    public void testCoalesce()
            throws Exception
    {
        assertExecute("coalesce(9, 1)", 9L);
        assertExecute("coalesce(9, null)", 9L);
        assertExecute("coalesce(9, cast(null as bigint))", 9L);
        assertExecute("coalesce(null, 9, 1)", 9L);
        assertExecute("coalesce(null, 9, null)", 9L);
        assertExecute("coalesce(null, 9, cast(null as bigint))", 9L);
        assertExecute("coalesce(cast(null as bigint), 9, 1)", 9L);
        assertExecute("coalesce(cast(null as bigint), 9, null)", 9L);
        assertExecute("coalesce(cast(null as bigint), 9, cast(null as bigint))", 9L);

        assertExecute("coalesce(9.0, 1.0)", 9.0);
        assertExecute("coalesce(9.0, 1)", 9.0);
        assertExecute("coalesce(9.0, null)", 9.0);
        assertExecute("coalesce(9.0, cast(null as double))", 9.0);
        assertExecute("coalesce(null, 9.0, 1)", 9.0);
        assertExecute("coalesce(null, 9.0, null)", 9.0);
        assertExecute("coalesce(null, 9.0, cast(null as double))", 9.0);
        assertExecute("coalesce(null, 9.0, cast(null as bigint))", 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0, 1)", 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0, null)", 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0, cast(null as bigint))", 9.0);
        assertExecute("coalesce(cast(null as double), 9.0, cast(null as double))", 9.0);

        assertExecute("coalesce('foo', 'bar')", "foo");
        assertExecute("coalesce('foo', null)", "foo");
        assertExecute("coalesce('foo', cast(null as varchar))", "foo");
        assertExecute("coalesce(null, 'foo', 'bar')", "foo");
        assertExecute("coalesce(null, 'foo', null)", "foo");
        assertExecute("coalesce(null, 'foo', cast(null as varchar))", "foo");
        assertExecute("coalesce(cast(null as varchar), 'foo', 'bar')", "foo");
        assertExecute("coalesce(cast(null as varchar), 'foo', null)", "foo");
        assertExecute("coalesce(cast(null as varchar), 'foo', cast(null as varchar))", "foo");

        assertExecute("coalesce(cast(null as bigint), null, cast(null as bigint))", null);

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

    private List<String> generateExpression(String expressionPattern, Double value)
    {
        return formatExpression(expressionPattern, value, "double");
    }

    private List<String> generateExpression(String expressionPattern, String value)
    {
        return formatExpression(expressionPattern, value, "varchar");
    }

    private List<String> generateExpression(String expressionPattern, Boolean left, Boolean right)
    {
        return formatExpression(expressionPattern, left, "boolean", right, "boolean");
    }

    private List<String> generateExpression(String expressionPattern, Long left, Long right)
    {
        return formatExpression(expressionPattern, left, "bigint", right, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Long left, Double right)
    {
        return formatExpression(expressionPattern, left, "bigint", right, "double");
    }

    private List<String> generateExpression(String expressionPattern, Double left, Long right)
    {
        return formatExpression(expressionPattern, left, "double", right, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Double left, Double right)
    {
        return formatExpression(expressionPattern, left, "double", right, "double");
    }

    private List<String> generateExpression(String expressionPattern, String left, String right)
    {
        return formatExpression(expressionPattern, left, "varchar", right, "varchar");
    }

    private List<String> generateExpression(String expressionPattern, Long first, Long second, Long third)
    {
        return formatExpression(expressionPattern, first, "bigint", second, "bigint", third, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Long first, Double second, Long third)
    {
        return formatExpression(expressionPattern, first, "bigint", second, "double", third, "bigint");
    }

    private List<String> generateExpression(String expressionPattern, Double first, Double second, Double third)
    {
        return formatExpression(expressionPattern, first, "double", second, "double", third, "double");
    }

    private List<String> generateExpression(String expressionPattern, Double first, Double second, Long third)
    {
        return formatExpression(expressionPattern, first, "double", second, "double", third, "bigint");
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

    private List<String> formatExpression(String expressionPattern, Object value, String type)
    {
        return formatExpression(expressionPattern,
                Arrays.<Object>asList(value),
                ImmutableList.of(type));
    }

    private List<String> formatExpression(String expressionPattern, Object left, final String leftType, Object right, final String rightType)
    {
        return formatExpression(expressionPattern,
                Arrays.<Object>asList(left, right),
                ImmutableList.of(leftType, rightType));
    }

    private List<String> formatExpression(String expressionPattern,
            Object first, String firstType,
            Object second, String secondType,
            Object third, String thirdType)
    {
        return formatExpression(expressionPattern,
                Arrays.<Object>asList(first, second, third),
                ImmutableList.of(firstType, secondType, thirdType));
    }

    private List<String> formatExpression(String expressionPattern, List<Object> values, List<String> types)
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
            expressions.add(String.format(expressionPattern, valueList.toArray(new Object[valueList.size()])));
        }
        return expressions.build();
    }

    private void assertExecute(String expression, Object expected)
    {
        addCallable(new AssertExecuteTask(functionAssertions, expression, expected));
    }

    private void addCallable(Callable<Void> callable)
    {
        if (PARALLEL) {
            futures.add(executor.submit(callable));
        }
        else {
            try {
                callable.call();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private void assertExecute(List<String> expressions, Object expected)
    {
        if (expected instanceof Slice) {
            expected = ((Slice) expected).toString(UTF_8);
        }
        for (String expression : expressions) {
            assertExecute(expression, expected);
        }
    }

    private static class AssertExecuteTask
            implements Callable<Void>
    {
        private final FunctionAssertions functionAssertions;
        private final String expression;
        private final Object expected;

        public AssertExecuteTask(FunctionAssertions functionAssertions, String expression, Object expected)
        {
            this.functionAssertions = functionAssertions;
            this.expression = expression;
            this.expected = expected;
        }

        @Override
        public Void call()
                throws Exception
        {
            try {
                functionAssertions.assertFunction(expression, expected);
            }
            catch (Throwable e) {
                throw new RuntimeException("Error processing " + expression, e);
            }
            return null;
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
            implements Callable<Void>
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
        public Void call()
                throws Exception
        {
            try {
                functionAssertions.assertFilter(filter, expected, withNoInputColumns);
            }
            catch (Throwable e) {
                throw new RuntimeException("Error processing " + filter, e);
            }
            return null;
        }
    }
}
