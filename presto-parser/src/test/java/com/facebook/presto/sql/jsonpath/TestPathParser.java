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
package com.facebook.presto.sql.jsonpath;

import com.facebook.presto.sql.jsonpath.tree.AbsMethod;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary;
import com.facebook.presto.sql.jsonpath.tree.ArithmeticUnary;
import com.facebook.presto.sql.jsonpath.tree.ArrayAccessor;
import com.facebook.presto.sql.jsonpath.tree.ArrayAccessor.Subscript;
import com.facebook.presto.sql.jsonpath.tree.CeilingMethod;
import com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate;
import com.facebook.presto.sql.jsonpath.tree.ConjunctionPredicate;
import com.facebook.presto.sql.jsonpath.tree.ContextVariable;
import com.facebook.presto.sql.jsonpath.tree.DatetimeMethod;
import com.facebook.presto.sql.jsonpath.tree.DisjunctionPredicate;
import com.facebook.presto.sql.jsonpath.tree.DoubleMethod;
import com.facebook.presto.sql.jsonpath.tree.ExistsPredicate;
import com.facebook.presto.sql.jsonpath.tree.Filter;
import com.facebook.presto.sql.jsonpath.tree.FloorMethod;
import com.facebook.presto.sql.jsonpath.tree.IsUnknownPredicate;
import com.facebook.presto.sql.jsonpath.tree.JsonPath;
import com.facebook.presto.sql.jsonpath.tree.KeyValueMethod;
import com.facebook.presto.sql.jsonpath.tree.LastIndexVariable;
import com.facebook.presto.sql.jsonpath.tree.LikeRegexPredicate;
import com.facebook.presto.sql.jsonpath.tree.MemberAccessor;
import com.facebook.presto.sql.jsonpath.tree.NamedVariable;
import com.facebook.presto.sql.jsonpath.tree.NegationPredicate;
import com.facebook.presto.sql.jsonpath.tree.PredicateCurrentItemVariable;
import com.facebook.presto.sql.jsonpath.tree.SizeMethod;
import com.facebook.presto.sql.jsonpath.tree.SqlValueLiteral;
import com.facebook.presto.sql.jsonpath.tree.StartsWithPredicate;
import com.facebook.presto.sql.jsonpath.tree.TypeMethod;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.RecursiveComparisonAssert;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary.Operator.ADD;
import static com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary.Operator.DIVIDE;
import static com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary.Operator.MODULUS;
import static com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary.Operator.MULTIPLY;
import static com.facebook.presto.sql.jsonpath.tree.ArithmeticBinary.Operator.SUBTRACT;
import static com.facebook.presto.sql.jsonpath.tree.ArithmeticUnary.Sign.MINUS;
import static com.facebook.presto.sql.jsonpath.tree.ArithmeticUnary.Sign.PLUS;
import static com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate.Operator.EQUAL;
import static com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate.Operator.GREATER_THAN;
import static com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate.Operator.LESS_THAN;
import static com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate.Operator.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.jsonpath.tree.ComparisonPredicate.Operator.NOT_EQUAL;
import static com.facebook.presto.sql.jsonpath.tree.JsonNullLiteral.JSON_NULL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPathParser
{
    private static final PathParser PATH_PARSER = new PathParser(new PathParser.Location(1, 0));
    private static final RecursiveComparisonConfiguration COMPARISON_CONFIGURATION = RecursiveComparisonConfiguration.builder().withStrictTypeChecking(true).build();

    @Test
    public void testPathMode()
    {
        assertThat(path("lax null"))
                .isEqualTo(new JsonPath(true, JSON_NULL));

        assertThat(path("strict null"))
                .isEqualTo(new JsonPath(false, JSON_NULL));
    }

    @Test
    public void testNumericLiteral()
    {
        assertThat(path("lax 1"))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new LongLiteral("1"))));

        assertThat(path("lax -2"))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new LongLiteral("-2"))));

        assertThat(path("lax 1.2e3"))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new DoubleLiteral("1.2e3"))));

        assertThat(path("lax -1.2e3"))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new DoubleLiteral("-1.2e3"))));

        assertThat(path("lax 1.0"))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new DecimalLiteral("1.0"))));

        assertThat(path("lax -.5"))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new DecimalLiteral("-.5"))));
    }

    @Test
    public void testStringLiteral()
    {
        assertThat(path("lax \"aBcD\""))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new StringLiteral("aBcD"))));

        assertThat(path("lax \"x     x\""))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new StringLiteral("x     x"))));

        assertThat(path("lax \"x\"\"x\""))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new StringLiteral("x\"x"))));
    }

    @Test
    public void testBooleanLiteral()
    {
        assertThat(path("lax true"))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new BooleanLiteral("true"))));

        assertThat(path("lax false"))
                .isEqualTo(new JsonPath(true, new SqlValueLiteral(new BooleanLiteral("false"))));
    }

    @Test
    public void testVariable()
    {
        assertThat(path("lax $"))
                .isEqualTo(new JsonPath(true, new ContextVariable()));

        assertThat(path("lax $Some_Name"))
                .isEqualTo(new JsonPath(true, new NamedVariable("Some_Name")));

        assertThat(path("lax last"))
                .isEqualTo(new JsonPath(true, new LastIndexVariable()));
    }

    @Test
    public void testMethod()
    {
        assertThat(path("lax $.abs()"))
                .isEqualTo(new JsonPath(true, new AbsMethod(new ContextVariable())));

        assertThat(path("lax $.ceiling()"))
                .isEqualTo(new JsonPath(true, new CeilingMethod(new ContextVariable())));

        assertThat(path("lax $.datetime()"))
                .isEqualTo(new JsonPath(true, new DatetimeMethod(new ContextVariable(), Optional.empty())));

        assertThat(path("lax $.datetime(\"some datetime template\")"))
                .isEqualTo(new JsonPath(true, new DatetimeMethod(new ContextVariable(), Optional.of("some datetime template"))));

        assertThat(path("lax $.double()"))
                .isEqualTo(new JsonPath(true, new DoubleMethod(new ContextVariable())));

        assertThat(path("lax $.floor()"))
                .isEqualTo(new JsonPath(true, new FloorMethod(new ContextVariable())));

        assertThat(path("lax $.keyvalue()"))
                .isEqualTo(new JsonPath(true, new KeyValueMethod(new ContextVariable())));

        assertThat(path("lax $.size()"))
                .isEqualTo(new JsonPath(true, new SizeMethod(new ContextVariable())));

        assertThat(path("lax $.type()"))
                .isEqualTo(new JsonPath(true, new TypeMethod(new ContextVariable())));
    }

    @Test
    public void testArithmeticBinary()
    {
        assertThat(path("lax $ + 2"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(ADD, new ContextVariable(), new SqlValueLiteral(new LongLiteral("2")))));

        assertThat(path("lax $ - 2"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(SUBTRACT, new ContextVariable(), new SqlValueLiteral(new LongLiteral("2")))));

        assertThat(path("lax $ * 2"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(MULTIPLY, new ContextVariable(), new SqlValueLiteral(new LongLiteral("2")))));

        assertThat(path("lax $ / 2"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(DIVIDE, new ContextVariable(), new SqlValueLiteral(new LongLiteral("2")))));

        assertThat(path("lax $ % 2"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(MODULUS, new ContextVariable(), new SqlValueLiteral(new LongLiteral("2")))));
    }

    @Test
    public void testArithmeticUnary()
    {
        assertThat(path("lax -$"))
                .isEqualTo(new JsonPath(true, new ArithmeticUnary(MINUS, new ContextVariable())));

        assertThat(path("lax +$"))
                .isEqualTo(new JsonPath(true, new ArithmeticUnary(PLUS, new ContextVariable())));
    }

    @Test
    public void testArrayAccessor()
    {
        assertThat(path("lax $[*]"))
                .isEqualTo(new JsonPath(
                        true,
                        new ArrayAccessor(new ContextVariable(), ImmutableList.of())));

        assertThat(path("lax $[5]"))
                .isEqualTo(new JsonPath(
                        true,
                        new ArrayAccessor(
                                new ContextVariable(),
                                ImmutableList.of(new Subscript(new SqlValueLiteral(new LongLiteral("5")))))));

        assertThat(path("lax $[5 to 10]"))
                .isEqualTo(new JsonPath(
                        true,
                        new ArrayAccessor(
                                new ContextVariable(),
                                ImmutableList.of(new Subscript(new SqlValueLiteral(new LongLiteral("5")), new SqlValueLiteral(new LongLiteral("10")))))));

        assertThat(path("lax $[3 to 5, 2, 0 to 1]"))
                .isEqualTo(new JsonPath(
                        true,
                        new ArrayAccessor(
                                new ContextVariable(),
                                ImmutableList.of(
                                        new Subscript(new SqlValueLiteral(new LongLiteral("3")), new SqlValueLiteral(new LongLiteral("5"))),
                                        new Subscript(new SqlValueLiteral(new LongLiteral("2"))),
                                        new Subscript(new SqlValueLiteral(new LongLiteral("0")), new SqlValueLiteral(new LongLiteral("1")))))));
    }

    @Test
    public void testMemberAccessor()
    {
        assertThat(path("lax $.*"))
                .isEqualTo(new JsonPath(
                        true,
                        new MemberAccessor(new ContextVariable(), Optional.empty())));

        assertThat(path("lax $.Key_Identifier"))
                .isEqualTo(new JsonPath(
                        true,
                        new MemberAccessor(new ContextVariable(), Optional.of("Key_Identifier"))));

        assertThat(path("lax $.\"Key Name\""))
                .isEqualTo(new JsonPath(
                        true,
                        new MemberAccessor(new ContextVariable(), Optional.of("Key Name"))));
    }

    @Test
    public void testPrecedenceAndGrouping()
    {
        assertThat(path("lax 1 + 2 + 3"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(
                        ADD,
                        new ArithmeticBinary(ADD, new SqlValueLiteral(new LongLiteral("1")), new SqlValueLiteral(new LongLiteral("2"))),
                        new SqlValueLiteral(new LongLiteral("3")))));

        assertThat(path("lax 1 * 2 + 3"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(
                        ADD,
                        new ArithmeticBinary(MULTIPLY, new SqlValueLiteral(new LongLiteral("1")), new SqlValueLiteral(new LongLiteral("2"))),
                        new SqlValueLiteral(new LongLiteral("3")))));

        assertThat(path("lax 1 + 2 * 3"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(
                        ADD,
                        new SqlValueLiteral(new LongLiteral("1")),
                        new ArithmeticBinary(MULTIPLY, new SqlValueLiteral(new LongLiteral("2")), new SqlValueLiteral(new LongLiteral("3"))))));

        assertThat(path("lax (1 + 2) * 3"))
                .isEqualTo(new JsonPath(true, new ArithmeticBinary(
                        MULTIPLY,
                        new ArithmeticBinary(ADD, new SqlValueLiteral(new LongLiteral("1")), new SqlValueLiteral(new LongLiteral("2"))),
                        new SqlValueLiteral(new LongLiteral("3")))));
    }

    @Test
    public void testFilter()
    {
        assertThat(path("lax $ ? (exists($))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ExistsPredicate(new ContextVariable()))));

        assertThat(path("lax $ ? ($x == $y)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ComparisonPredicate(EQUAL, new NamedVariable("x"), new NamedVariable("y")))));

        assertThat(path("lax $ ? ($x <> $y)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ComparisonPredicate(NOT_EQUAL, new NamedVariable("x"), new NamedVariable("y")))));

        assertThat(path("lax $ ? ($x != $y)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ComparisonPredicate(NOT_EQUAL, new NamedVariable("x"), new NamedVariable("y")))));

        assertThat(path("lax $ ? ($x < $y)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ComparisonPredicate(LESS_THAN, new NamedVariable("x"), new NamedVariable("y")))));

        assertThat(path("lax $ ? ($x > $y)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ComparisonPredicate(GREATER_THAN, new NamedVariable("x"), new NamedVariable("y")))));

        assertThat(path("lax $ ? ($x <= $y)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ComparisonPredicate(LESS_THAN_OR_EQUAL, new NamedVariable("x"), new NamedVariable("y")))));

        assertThat(path("lax $ ? ($x >= $y)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ComparisonPredicate(GREATER_THAN_OR_EQUAL, new NamedVariable("x"), new NamedVariable("y")))));

        assertThat(path("lax $ ? ($ like_regex \"something*\")"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new LikeRegexPredicate(new ContextVariable(), "something*", Optional.empty()))));

        assertThat(path("lax $ ? ($ like_regex \"something*\" flag \"some_flag\")"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new LikeRegexPredicate(new ContextVariable(), "something*", Optional.of("some_flag")))));

        assertThat(path("lax $ ? ($ starts with $some_variable)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new StartsWithPredicate(new ContextVariable(), new NamedVariable("some_variable")))));

        assertThat(path("lax $ ? ($ starts with \"some_text\")"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new StartsWithPredicate(new ContextVariable(), new SqlValueLiteral(new StringLiteral("some_text"))))));

        assertThat(path("lax $ ? ((exists($)) is unknown)"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new IsUnknownPredicate(new ExistsPredicate(new ContextVariable())))));

        assertThat(path("lax $ ? (! exists($))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new NegationPredicate(new ExistsPredicate(new ContextVariable())))));

        assertThat(path("lax $ ? (exists($x) && exists($y))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ConjunctionPredicate(new ExistsPredicate(new NamedVariable("x")), new ExistsPredicate(new NamedVariable("y"))))));

        assertThat(path("lax $ ? (exists($x) || exists($y))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new DisjunctionPredicate(new ExistsPredicate(new NamedVariable("x")), new ExistsPredicate(new NamedVariable("y"))))));
    }

    @Test
    public void testPrecedenceAndGroupingInFilter()
    {
        assertThat(path("lax $ ? (exists($x) && exists($y) && exists($z))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(
                                new ContextVariable(),
                                new ConjunctionPredicate(
                                        new ConjunctionPredicate(new ExistsPredicate(new NamedVariable("x")), new ExistsPredicate(new NamedVariable("y"))),
                                        new ExistsPredicate(new NamedVariable("z"))))));

        assertThat(path("lax $ ? (exists($x) || exists($y) || exists($z))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(
                                new ContextVariable(),
                                new DisjunctionPredicate(
                                        new DisjunctionPredicate(new ExistsPredicate(new NamedVariable("x")), new ExistsPredicate(new NamedVariable("y"))),
                                        new ExistsPredicate(new NamedVariable("z"))))));

        assertThat(path("lax $ ? (exists($x) || (exists($y) || exists($z)))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(
                                new ContextVariable(),
                                new DisjunctionPredicate(
                                        new ExistsPredicate(new NamedVariable("x")),
                                        new DisjunctionPredicate(new ExistsPredicate(new NamedVariable("y")), new ExistsPredicate(new NamedVariable("z")))))));

        assertThat(path("lax $ ? (exists($x) || exists($y) && exists($z))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(
                                new ContextVariable(),
                                new DisjunctionPredicate(
                                        new ExistsPredicate(new NamedVariable("x")),
                                        new ConjunctionPredicate(new ExistsPredicate(new NamedVariable("y")), new ExistsPredicate(new NamedVariable("z")))))));
    }

    @Test
    public void testPredicateCurrentItemVariable()
    {
        assertThat(path("lax $ ? (exists(@))"))
                .isEqualTo(new JsonPath(
                        true,
                        new Filter(new ContextVariable(), new ExistsPredicate(new PredicateCurrentItemVariable()))));
    }

    @Test
    public void testCaseSensitiveKeywords()
    {
        assertThatThrownBy(() -> PATH_PARSER.parseJsonPath("LAX $"))
                .hasMessage("line 1:2: mismatched input 'LAX' expecting {'lax', 'strict'}");

        assertThatThrownBy(() -> PATH_PARSER.parseJsonPath("lax $[1 To 2]"))
                .hasMessage("line 1:10: mismatched input 'To' expecting {',', ']'}");
    }

    @Test
    public void testNonReservedKeywords()
    {
        // keyword "lax" as key in member accessor
        assertThat(path("lax $.lax"))
                .isEqualTo(new JsonPath(
                        true,
                        new MemberAccessor(
                                new ContextVariable(),
                                Optional.of("lax"))));

        // keyword "ceiling" as key in member accessor
        assertThat(path("lax $.ceiling"))
                .isEqualTo(new JsonPath(
                        true,
                        new MemberAccessor(
                                new ContextVariable(),
                                Optional.of("ceiling"))));

        // keyword "lax" as variable name
        assertThat(path("lax $lax"))
                .isEqualTo(new JsonPath(
                        true,
                        new NamedVariable("lax")));

        // keyword "lax" as variable name in array subscript
        assertThat(path("lax $[$lax]"))
                .isEqualTo(new JsonPath(
                        true,
                        new ArrayAccessor(new ContextVariable(), ImmutableList.of(new Subscript(new NamedVariable("lax"))))));
    }

    @Test
    public void testNestedStructure()
    {
        assertThat(path("lax $multiplier[0].floor().abs() * ($.array.size() + $component ? (exists(@)))"))
                .isEqualTo(new JsonPath(
                        true,
                        new ArithmeticBinary(
                                MULTIPLY,
                                new AbsMethod(new FloorMethod(new ArrayAccessor(new NamedVariable("multiplier"), ImmutableList.of(new Subscript(new SqlValueLiteral(new LongLiteral("0"))))))),
                                new ArithmeticBinary(
                                        ADD,
                                        new SizeMethod(new MemberAccessor(new ContextVariable(), Optional.of("array"))),
                                        new Filter(new NamedVariable("component"), new ExistsPredicate(new PredicateCurrentItemVariable()))))));
    }

    private static AssertProvider<? extends RecursiveComparisonAssert<?>> path(String path)
    {
        return () -> new RecursiveComparisonAssert<>(PATH_PARSER.parseJsonPath(path), COMPARISON_CONFIGURATION);
    }
}
