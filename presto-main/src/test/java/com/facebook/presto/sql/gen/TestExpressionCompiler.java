package com.facebook.presto.sql.gen;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.RegexpFunctions;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.scalar.UnixTimeFunctions;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.LikeUtils;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract.Field;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joni.Regex;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.sql.parser.SqlParser.createExpression;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static com.google.common.base.Charsets.UTF_8;
import static java.lang.Math.cos;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestExpressionCompiler
{
    private static final ExpressionCompiler compiler = new ExpressionCompiler(new MetadataManager());

    private static final Boolean[] booleanValues = {true, false, null};
    private static final Long[] longLefts = {9L, 10L, 11L, -9L, -10L, -11L, 10151082135029368L, /*Long.MIN_VALUE,*/ Long.MAX_VALUE, null};
    private static final Long[] longRights = {3L, -3L, 10151082135029369L, null};
    private static final Double[] doubleLefts = {9.0, 10.0, 11.0, -9.0, -10.0, -11.0, 9.1, 10.1, 11.1, -9.1, -10.1, -11.1,
            Double.MIN_VALUE, Double.MAX_VALUE, Double.MIN_NORMAL, null};
    private static final Double[] doubleRights = {3.0, -3.0, 3.1, -3.1, null};
    private static final String[] stringLefts = {"hello", "foo", "mellow", "fellow", "", null};
    private static final String[] stringRights = {"hello", "foo", "bar", "baz", "", null};

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

    @Test
    public void smokeTest()
            throws Exception
    {
        assertExecute("true", true);
        assertExecute("false", false);
        assertExecute("42", 42L);
        assertExecute("'foo'", "foo");
        assertExecute("4.2", 4.2);
        assertExecute("1 + 1", 2L);
        assertExecute("bound_long", 1234L);
        assertExecute("bound_string", "hello");
        assertExecute("bound_double", 12.34);
        assertExecute("bound_timestamp", MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis()));
        assertExecute("bound_pattern", "%el%");

        assertExecute("null", null);
    }

    @Test
    public void filterFunction()
            throws Exception
    {
        assertFilter("true", true);
        assertFilter("false", false);
        assertFilter("bound_long = 1234", true);
        assertFilter("bound_long = 5678", false);

        assertFilter("null", false);
        assertFilter("cast(null as boolean)", false);
        assertFilter("nullif(true, true)", false);

        assertFilter("true AND cast(null as boolean) AND true", false);
    }

    @Test
    public void testUnaryOperators()
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
    }

    @Test
    public void testBinaryOperators()
    {
        for (Boolean left : booleanValues) {
            for (Boolean right : booleanValues) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : left == right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : left != right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(boolean.class, left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right));
            }
        }

        for (Long left : longLefts) {
            for (Long right : longRights) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : (long) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : (long) left != right);
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : (long) left > right);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : (long) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : (long) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : (long) left <= right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(long.class, left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right));

                assertExecute(generateExpression("%s + %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), left == null || right == null ? null : left % right);
            }
        }

        for (Long left : longLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : (double) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : (double) left != right);
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : (double) left > right);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : (double) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : (double) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : (double) left <= right);

                Object expectedNullIf = nullIf(double.class, left, right);
                for (String expression : generateExpression("nullif(%s, %s)", left, right)) {
                    try {
                        Object actual = execute(expression);
                        if (!Objects.equals(actual, expectedNullIf)) {
                            if (left != null && right == null) {
                                expectedNullIf = ((Number) expectedNullIf).doubleValue();
                                actual = ((Number) expectedNullIf).doubleValue();
                            }
                            assertEquals(actual, expectedNullIf, expression);
                        }
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Error processing " + expression, e);
                    }
                }

                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left == null ? null : left.doubleValue(), right));

                assertExecute(generateExpression("%s + %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), left == null || right == null ? null : left % right);
            }
        }

        for (Double left : doubleLefts) {
            for (Long right : longRights) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : left == (double) right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : left != (double) right);
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : left > (double) right);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : left < (double) right);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : left >= (double) right);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : left <= (double) right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(double.class, left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right == null ? null : right.doubleValue()));

                assertExecute(generateExpression("%s + %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), left == null || right == null ? null : left % right);
            }
        }

        for (Double left : doubleLefts) {
            for (Double right : doubleRights) {
                assertExecute(generateExpression("%s = %s", left, right), left == null || right == null ? null : (double) left == right);
                assertExecute(generateExpression("%s <> %s", left, right), left == null || right == null ? null : (double) left != right);
                assertExecute(generateExpression("%s > %s", left, right), left == null || right == null ? null : (double) left > right);
                assertExecute(generateExpression("%s < %s", left, right), left == null || right == null ? null : (double) left < right);
                assertExecute(generateExpression("%s >= %s", left, right), left == null || right == null ? null : (double) left >= right);
                assertExecute(generateExpression("%s <= %s", left, right), left == null || right == null ? null : (double) left <= right);

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(double.class, left, right));
                assertExecute(generateExpression("%s is distinct from %s", left, right), !Objects.equals(left, right));

                assertExecute(generateExpression("%s + %s", left, right), left == null || right == null ? null : left + right);
                assertExecute(generateExpression("%s - %s", left, right), left == null || right == null ? null : left - right);
                assertExecute(generateExpression("%s * %s", left, right), left == null || right == null ? null : left * right);
                assertExecute(generateExpression("%s / %s", left, right), left == null || right == null ? null : left / right);
                assertExecute(generateExpression("%s %% %s", left, right), left == null || right == null ? null : left % right);
            }
        }

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

                assertExecute(generateExpression("nullif(%s, %s)", left, right), nullIf(String.class, left, right));
            }
        }
    }

    private static Object nullIf(Class<?> expectedType, Object left, Object right)
    {
        if (left != null && right != null) {
            if (left instanceof Double || right instanceof Double) {
                left = ((Number) left).doubleValue();
                right = ((Number) right).doubleValue();
            }
            if (left.equals(right)) {
                return null;
            }
        }

        if (expectedType == double.class && left != null) {
            left = ((Number) left).doubleValue();
        }
        return left;
    }

    @Test
    public void testTernaryOperators()
    {
        for (Long first : longLefts) {
            for (Long second : longLefts) {
                for (Long third : longRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            first == null || second == null || third == null ? null : second <= first && first <= third);
                }
            }
        }

        for (Long first : longLefts) {
            for (Double second : doubleLefts) {
                for (Long third : longRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            first == null || second == null || third == null ? null : second <= first && first <= third);
                }
            }
        }

        for (Double first : doubleLefts) {
            for (Double second : doubleLefts) {
                for (Long third : longRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            first == null || second == null || third == null ? null : second <= first && first <= third);
                }
            }
        }

        for (String first : stringLefts) {
            for (String second : stringLefts) {
                for (String third : stringRights) {
                    assertExecute(generateExpression("%s between %s and %s", first, second, third),
                            first == null || second == null || third == null ? null : second.compareTo(first) <= 0 && first.compareTo(third) <= 0);
                }
            }
        }
    }

    @Test
    public void testCast()
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
    }

    @Test
    public void testNot()
            throws Exception
    {
        assertExecute("not true", false);
        assertExecute("not false", true);

        assertExecute("not cast(null as boolean)", null);

        assertExecute("not null", null);
    }

    @Test
    public void testIf()
            throws Exception
    {
        for (Boolean condition : booleanValues) {
            for (String trueValue : stringLefts) {
                for (String falseValue : stringRights) {
                    assertExecute(generateExpression("if(%s, %s, %s)", condition, trueValue, falseValue), condition != null && condition ? trueValue : falseValue);
                }
            }
        }
    }

    @Test
    public void testSimpleCase()
            throws Exception
    {
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
                    assertExecute(generateExpression("case %s when %s then 'first' when %s then 'second' else 'else' end", value, firstTest, secondTest), expected);
                }
            }
        }
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
                    assertExecute(generateExpression("case %s when %s then 'first' when %s then 'second' end", value, firstTest, secondTest), expected);
                }
            }
        }
    }

    @Test
    public void testSearchCase()
            throws Exception
    {
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
    }

    @Test
    public void testHugeIn()
            throws Exception
    {
        ContiguousSet<Integer> longValues = Range.openClosed(2000, 7000).asSet(DiscreteDomain.integers());
        assertExecute("bound_long in (1234, " + Joiner.on(", ").join(longValues) + ")", true);
        assertExecute("bound_long in (" + Joiner.on(", ").join(longValues) + ")", false);

        Iterable<Object> doubleValues = Iterables.transform(Range.openClosed(2000, 7000).asSet(DiscreteDomain.integers()), new Function<Integer, Object>()
        {
            @Override
            public Object apply(Integer i)
            {
                if (i % 2 == 0) {
                    return i;
                }
                else {
                    return (double) i;
                }
            }
        });
        assertExecute("bound_double in (12.34, " + Joiner.on(", ").join(doubleValues) + ")", true);
        assertExecute("bound_double in (" + Joiner.on(", ").join(doubleValues) + ")", false);


        Iterable<Object> stringValues = Iterables.transform(Range.openClosed(2000, 7000).asSet(DiscreteDomain.integers()), new Function<Integer, Object>()
        {
            @Override
            public Object apply(Integer i)
            {
                return "'" + i + "'";
            }
        });
        assertExecute("bound_string in ('hello', " + Joiner.on(", ").join(stringValues) + ")", true);
        assertExecute("bound_string in (" + Joiner.on(", ").join(stringValues) + ")", false);


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

        for (String value : stringLefts) {
            for (String pattern : stringRights) {
                assertExecute(generateExpression("regexp_like(%s, %s)", value, pattern),
                        value == null || pattern == null ? null : RegexpFunctions.regexpLike(Slices.copiedBuffer(value, UTF_8), Slices.copiedBuffer(pattern, UTF_8)));
                assertExecute(generateExpression("regexp_replace(%s, %s)", value, pattern),
                        value == null || pattern == null ? null : RegexpFunctions.regexpReplace(Slices.copiedBuffer(value, UTF_8), Slices.copiedBuffer(pattern, UTF_8)));
                assertExecute(generateExpression("regexp_extract(%s, %s)", value, pattern),
                        value == null || pattern == null ? null : RegexpFunctions.regexpExtract(Slices.copiedBuffer(value, UTF_8), Slices.copiedBuffer(pattern, UTF_8)));
            }
        }

        for (String value : jsonValues) {
            for (String pattern : jsonPatterns) {
                assertExecute(generateExpression("json_extract(%s, %s)", value, pattern),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtract(Slices.copiedBuffer(value, UTF_8), Slices.copiedBuffer(pattern, UTF_8)));
                assertExecute(generateExpression("json_extract_scalar(%s, %s)", value, pattern),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtractScalar(Slices.copiedBuffer(value, UTF_8), Slices.copiedBuffer(pattern, UTF_8)));

                assertExecute(generateExpression("json_extract(%s, %s || '')", value, pattern),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtract(Slices.copiedBuffer(value, UTF_8), Slices.copiedBuffer(pattern, UTF_8)));
                assertExecute(generateExpression("json_extract_scalar(%s, %s || '')", value, pattern),
                        value == null || pattern == null ? null : JsonFunctions.jsonExtractScalar(Slices.copiedBuffer(value, UTF_8), Slices.copiedBuffer(pattern, UTF_8)));
            }
        }

        assertExecute("json_array_contains('[1, 2, 3]', 2)", true);
        assertExecute("json_array_contains('[2.5]', 2.5)", true);
        assertExecute("json_array_contains('[false, true]', true)", true);
        assertExecute("json_array_contains('[5]', 3)", false);
        assertExecute("json_array_contains('[', 9)", null);
        assertExecute("json_array_length('[')", null);
    }

    @Test
    public void tesExtract()
            throws Exception
    {
        for (Long left : longLefts) {
            for (Field field : Field.values()) {
                Long expected = null;
                if (left != null) {
                    switch (field) {
                        case CENTURY:
                            expected = UnixTimeFunctions.century(left);
                            break;
                        case YEAR:
                            expected = UnixTimeFunctions.year(left);
                            break;
                        case QUARTER:
                            expected = UnixTimeFunctions.quarter(left);
                            break;
                        case MONTH:
                            expected = UnixTimeFunctions.month(left);
                            break;
                        case WEEK:
                            expected = UnixTimeFunctions.week(left);
                            break;
                        case DAY:
                            expected = UnixTimeFunctions.day(left);
                            break;
                        case DOW:
                            expected = UnixTimeFunctions.dayOfWeek(left);
                            break;
                        case DOY:
                            expected = UnixTimeFunctions.dayOfYear(left);
                            break;
                        case HOUR:
                            expected = UnixTimeFunctions.hour(left);
                            break;
                        case MINUTE:
                            expected = UnixTimeFunctions.minute(left);
                            break;
                        case SECOND:
                            expected = UnixTimeFunctions.second(left);
                            break;
                        case TIMEZONE_HOUR:
                        case TIMEZONE_MINUTE:
                            // TODO: we assume all times are UTC for now
                            expected = 0L;
                            break;
                    }
                }
                assertExecute(generateExpression("extract(" + field.toString() + " from %s)", left), expected);
            }
        }
    }

    @Test
    public void testLike()
            throws Exception
    {
        for (String value : stringLefts) {
            for (String pattern : stringLefts) {
                Boolean expected = null;
                if (value != null && pattern != null) {
                    Regex regex = LikeUtils.likeToPattern(pattern, '\\');
                    expected = LikeUtils.regexMatches(regex, Slices.copiedBuffer(value, UTF_8));
                }
                assertExecute(generateExpression("%s like %s", value, pattern), expected);
            }
        }
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

        assertExecute("coalesce(9.0, 1)", 9.0);
        assertExecute("coalesce(9.0, null)", 9.0);
        assertExecute("coalesce(9.0, cast(null as bigint))", 9.0);
        assertExecute("coalesce(null, 9.0, 1)", 9.0);
        assertExecute("coalesce(null, 9.0, null)", 9.0);
        assertExecute("coalesce(null, 9.0, cast(null as bigint))", 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0, 1)", 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0, null)", 9.0);
        assertExecute("coalesce(cast(null as bigint), 9.0, cast(null as bigint))", 9.0);

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
                unrolledValues.add(ImmutableSet.of("null", "cast(null as " + type + ")"));
            }
        }

        ImmutableList.Builder<String> expressions = ImmutableList.builder();
        Set<List<String>> valueLists = Sets.cartesianProduct(unrolledValues);
        for (List<String> valueList : valueLists) {
            expressions.add(String.format(expressionPattern, valueList.toArray(new String[valueList.size()])));
        }
        return expressions.build();
    }

    private static void assertExecute(String actual, Object expected)
    {
        if (expected instanceof Boolean) {
            expected = ((Boolean) expected) ? true : false;
        }

        if (expected instanceof Slice) {
            expected = ((Slice) expected).toString(UTF_8);
        }

        assertEquals(execute(actual), expected);
    }

    private static void assertExecute(List<String> expressions, Object expected)
    {
        if (expected instanceof Boolean) {
            expected = ((Boolean) expected) ? true : false;
        }

        if (expected instanceof Slice) {
            expected = ((Slice) expected).toString(UTF_8);
        }

        for (String expression : expressions) {
            try {
                assertEquals(execute(expression), expected, expression);
            }
            catch (Exception e) {
                throw new RuntimeException("Error processing " + expression, e);
            }
        }
    }

    private static Object execute(String expression)
    {
        Expression parsedExpression = parseExpression(expression);

        Function<Operator,Operator> operatorFactory;
        try {
            operatorFactory = compiler.compileFilterAndProjectOperator(BooleanLiteral.TRUE_LITERAL,
                    ImmutableList.of(parsedExpression),
                    ImmutableMap.<Input, Type>of(
                            new Input(0, 0), Type.LONG,
                            new Input(1, 0), Type.STRING,
                            new Input(2, 0), Type.DOUBLE,
                            new Input(3, 0), Type.LONG,
                            new Input(4, 0), Type.STRING));
        }
        catch (Throwable e) {
            throw new RuntimeException("Error compiling " + expression, e);
        }

        Operator source = createOperator(new Page(
                createLongsBlock(1234L),
                createStringsBlock("hello"),
                createDoublesBlock(12.34),
                createLongsBlock(MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis())),
                createStringsBlock("%el%")));

        Operator operator = operatorFactory.apply(source);
        PageIterator pageIterator = operator.iterator(new OperatorStats());
        assertTrue(pageIterator.hasNext());
        Page page = pageIterator.next();
        assertFalse(pageIterator.hasNext());

        assertEquals(page.getPositionCount(), 1);
        assertEquals(page.getChannelCount(), 1);

        Block block = page.getBlock(0);
        assertEquals(block.getPositionCount(), 1);
        assertEquals(block.getTupleInfo().getFieldCount(), 1);

        BlockCursor cursor = block.cursor();
        assertTrue(cursor.advanceNextPosition());
        if (cursor.isNull(0)) {
            return null;
        }
        else {
            return cursor.getTuple().toValues().get(0);
        }
    }

    private static void assertFilter(String expression, boolean expected)
    {
        Expression parsedExpression = parseExpression(expression);

        FilterFunction filterFunction;
        try {
            filterFunction = compiler.compileFilterFunction(parsedExpression,
                    ImmutableMap.<Input, Type>of(
                            new Input(0, 0), Type.LONG,
                            new Input(1, 0), Type.STRING,
                            new Input(2, 0), Type.DOUBLE,
                            new Input(3, 0), Type.LONG,
                            new Input(4, 0), Type.STRING));
        }
        catch (Throwable e) {
            throw new RuntimeException("Error compiling " + expression, e);
        }


        boolean value = filterFunction.filter(createTuple(1234L),
                createTuple("hello"),
                createTuple(12.34),
                createTuple(MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis())),
                createTuple("%el%"));
        assertEquals(value, expected);
    }

    private static Expression parseExpression(String expression)
    {
        Expression parsedExpression = createExpression(expression);

        parsedExpression = TreeRewriter.rewriteWith(new SymbolToInputRewriter(ImmutableMap.of(
                new Symbol("bound_long"), new Input(0, 0),
                new Symbol("bound_string"), new Input(1, 0),
                new Symbol("bound_double"), new Input(2, 0),
                new Symbol("bound_timestamp"), new Input(3, 0),
                new Symbol("bound_pattern"), new Input(4, 0)
        )), parsedExpression);
        return parsedExpression;
    }
}
