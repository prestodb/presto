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
package com.facebook.presto.sql.relational;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractPredicates;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static org.testng.Assert.assertEquals;

public class TestLogicalRowExpressions
{
    private FunctionAndTypeManager functionAndTypeManager;
    private LogicalRowExpressions logicalRowExpressions;
    private static final RowExpression a = name("a");
    private static final RowExpression b = name("b");
    private static final RowExpression c = name("c");
    private static final RowExpression d = name("d");
    private static final RowExpression e = name("e");
    private static final RowExpression f = name("f");
    private static final RowExpression g = name("g");
    private static final RowExpression h = name("h");
    private static final VariableReferenceExpression V_0 = variable("v0");
    private static final VariableReferenceExpression V_1 = variable("v1");
    private static final VariableReferenceExpression V_2 = variable("v2");

    @BeforeClass
    public void setup()
    {
        functionAndTypeManager = createTestFunctionAndTypeManager();
        logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(functionAndTypeManager), new FunctionResolution(functionAndTypeManager), functionAndTypeManager);
    }

    @Test
    public void testAnd()
    {
        assertEquals(
                LogicalRowExpressions.and(a, b, c, d, e),
                and(and(and(a, b), and(c, d)), e));

        assertEquals(
                LogicalRowExpressions.and(),
                constant(true, BOOLEAN));

        assertEquals(
                logicalRowExpressions.combinePredicates(AND, a, b, a, c, d, c, e),
                and(and(and(a, b), and(c, d)), e));

        assertEquals(
                logicalRowExpressions.combineConjuncts(a, b, constant(false, BOOLEAN)),
                constant(false, BOOLEAN));

        assertEquals(
                extractPredicates(and(and(and(a, b), and(c, d)), e)),
                ImmutableList.of(a, b, c, d, e));
    }

    @Test
    public void testAndWithSubclassOfRowExpression()
    {
        assertEquals(
                LogicalRowExpressions.and(V_0, V_1, V_2),
                and(and(V_0, V_1), V_2));

        assertEquals(
                LogicalRowExpressions.and(ImmutableList.of(V_0, V_1, V_2)),
                and(and(V_0, V_1), V_2));
    }

    @Test
    public void testOr()
    {
        assertEquals(
                LogicalRowExpressions.or(a, b, c, d, e),
                or(or(or(a, b), or(c, d)), e));

        assertEquals(
                LogicalRowExpressions.or(),
                constant(false, BOOLEAN));

        assertEquals(
                logicalRowExpressions.combinePredicates(OR, a, b, constant(true, BOOLEAN)),
                constant(true, BOOLEAN));

        assertEquals(
                extractPredicates(or(or(or(a, b), or(c, d)), e)),
                ImmutableList.of(a, b, c, d, e));
    }

    @Test
    public void testOrWithSubclassOfRowExpression()
    {
        assertEquals(
                LogicalRowExpressions.or(V_0, V_1, V_2),
                or(or(V_0, V_1), V_2));

        assertEquals(
                LogicalRowExpressions.or(ImmutableList.of(V_0, V_1, V_2)),
                or(or(V_0, V_1), V_2));
    }

    @Test
    public void testDeterminism()
    {
        RowExpression nondeterministic = call("random", functionAndTypeManager.lookupFunction("random", fromTypes()), DOUBLE);
        RowExpression deterministic = call("length", functionAndTypeManager.lookupFunction("length", fromTypes(VARCHAR)), INTEGER);

        RowExpression expression = and(and(a, or(b, nondeterministic)), deterministic);

        assertEquals(logicalRowExpressions.filterDeterministicConjuncts(expression), and(a, deterministic));
        assertEquals(logicalRowExpressions.filterNonDeterministicConjuncts(expression), or(b, nondeterministic));
    }

    @Test
    public void testPushNegationToLeaves()
    {
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(and(a, b))), or(not(a), not(b)));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(or(a, b))), and(not(a), not(b)));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(or(not(a), not(b)))), and(a, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(or(not(a), not(not(not(b)))))), and(a, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(or(not(and(a, b)), or(b, not(and(c, d)))))), and(and(a, b), and(not(b), and(c, d))));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(or(not(and(a, b)), not(b)))), and(and(a, b), b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(or(not(and(a, b)), not(and(b, c))))), and(and(a, b), and(b, c)));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(or(not(and(a, b)), not(or(b, c))))), and(and(a, b), or(b, c)));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(or(not(and(a, b)), not(or(b, not(and(c, d))))))), and(and(a, b), or(b, or(not(c), not(d)))));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(or(not(and(a, b)), not(or(b, not(and(c, d)))))), or(or(not(a), not(b)), and(not(b), and(c, d))));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(and(and(a, b), c)), and(and(a, b), c));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(compare(a, EQUAL, b))), compare(a, NOT_EQUAL, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(compare(a, NOT_EQUAL, b))), compare(a, EQUAL, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(compare(a, GREATER_THAN, b))), compare(a, LESS_THAN_OR_EQUAL, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(compare(a, GREATER_THAN_OR_EQUAL, b))), compare(a, LESS_THAN, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(compare(a, GREATER_THAN_OR_EQUAL, not(not(b))))), compare(a, LESS_THAN, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(compare(a, LESS_THAN, b))), compare(a, GREATER_THAN_OR_EQUAL, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(compare(a, LESS_THAN_OR_EQUAL, b))), compare(a, GREATER_THAN, b));
        assertEquals(logicalRowExpressions.pushNegationToLeaves(not(compare(a, LESS_THAN_OR_EQUAL, not(not(b))))), compare(a, GREATER_THAN, b));
    }

    @Test
    public void testEliminateConstant()
    {
        // Testing eliminate constant
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(TRUE_CONSTANT, a), and(FALSE_CONSTANT, b))),
                a);
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(or(and(TRUE_CONSTANT, a), and(FALSE_CONSTANT, b))),
                a);

        // Testing eliminate constant in nested tree
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(a, and(b, or(c, and(FALSE_CONSTANT, d))))),
                and(and(a, b), c));
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(a, and(b, or(c, and(e, or(f, and(FALSE_CONSTANT, d))))))),
                and(and(a, b), or(c, and(e, f))));
    }

    @Test
    public void testDuplicateIsNullExpressions()
    {
        SpecialFormExpression isNullExpression = new SpecialFormExpression(IS_NULL, BOOLEAN, a);
        List<RowExpression> arguments = Arrays.asList(new SpecialFormExpression[]{isNullExpression, isNullExpression});
        SpecialFormExpression duplicateIsNullExpression = new SpecialFormExpression(OR, BOOLEAN, arguments);
        logicalRowExpressions.minimalNormalForm(duplicateIsNullExpression);
    }

    @Test
    public void testEliminateDuplicate()
    {
        RowExpression nd = call("random", functionAndTypeManager.lookupFunction("random", fromTypes()), DOUBLE);

        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(TRUE_CONSTANT, a), and(b, b))),
                or(a, b));

        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(a, b), and(a, b))),
                and(a, b));
        // we will prefer most simplified expression than correct conjunctive/disjunctive form
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(or(and(a, b), and(a, b))),
                and(a, b));

        // eliminate duplicated items with different order, prefers the ones appears first.
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(b, a), and(a, b))),
                and(b, a));
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(or(and(b, a), and(a, b))),
                and(b, a));

        // (b && a) || a
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(b, a), a)),
                a);
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(or(and(b, a), a)),
                a);
        // (b || a) && a
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(a, or(b, a))),
                a);
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(a, or(b, a))),
                a);

        // (b && a) || (a && b && c) -> b && a (should keep b && a instead of a && b as it appears first)
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(b, a), and(and(a, b), c))),
                and(b, a));
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(or(and(b, a), and(and(a, b), c))),
                and(b, a));

        // (b || a) && (a || b) && (a || b || c || d) || (a || b || c) -> b || a (should keep b || a instead of a || b as it appears first)
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(or(b, a), and(or(a, b), and(or(or(c, d), or(a, b)), or(a, or(b, c)))))),
                or(b, a));

        // (b || a)  && (a || b || c) && (a || b) && (a || b || nd) && e
        // we cannot eliminate nd because it is non-deterministic
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(and(or(b, a), and(or(a, or(b, c)), and(or(a, b), or(or(a, b), nd)))), e)),
                and(and(or(b, a), or(or(a, b), nd)), e));
        // we cannot convert to disjunctive form because nd is non-deterministic
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(and(or(b, a), and(or(a, or(b, c)), and(or(a, b), or(or(a, b), nd)))), e)),
                and(and(or(b, a), or(or(a, b), nd)), e));

        // (b || a)  && (a || b || c) && (a || b) && (a || b || d) && e
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(and(or(b, a), and(or(a, or(b, c)), and(or(a, b), or(or(a, b), d)))), e)),
                and(or(b, a), e));
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(and(or(b, a), and(or(a, or(b, c)), and(or(a, b), or(or(a, b), d)))), e)),
                or(and(b, e), and(a, e)));

        // (b || a || c) && (a || b || d) && (a || b || e) && (a || b || f)
        // already conjunctive form
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(or(or(b, a), c), and(or(d, or(a, b)), and(or(or(a, b), e), or(or(a, b), f))))),
                and(and(or(or(b, a), c), or(or(b, a), d)), and(or(or(b, a), e), or(or(b, a), f))));
        // (b || a || c) && (a || b || d) && (a || b || f) -> b || a || (c && d && e && f)
        // can be simplified by extract common predicates
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(or(or(b, a), c), and(or(d, or(a, b)), and(or(or(a, b), e), or(or(a, b), f))))),
                or(or(b, a), and(and(c, d), and(e, f))));

        // de-duplicate nested expression
        // ((a && b) || (a && c) || (a && d)) && ((b && c) || (c && a) || (c && d)) -> a && c
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(or(and(a, b), and(a, c), and(a, d)), or(and(c, b), and(c, a), and(c, d)))),
                and(a, c));
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(or(and(a, b), and(a, c), and(a, d)), or(and(c, b), and(c, a), and(c, d)))),
                and(a, c));
    }

    @Test
    public void testConvertToCNF()
    {
        // When considering where we want to swap forms (i.e. an AND expression for DNF or OR expression for CNF), there are 3 cases for each argument
        // 1) it's a 1-term clause (literal) (neither AND nor OR)
        // 2) it's a multi-term single clause (ex. for CNF: (A OR B), DNF: (A AND B))
        // 3) it's a multi-clause branch (ex. for CNF: (A OR B) AND (C OR D), DNF: (A AND B) OR (C AND D))

        // Consider the following unique argument pairs and specify what the expected transformation should be.
        // left = 1, right = 1 --> (2) termJoiner(left, right)
        // left = 1, right = 2 --> (2) termJoiner(left, right)
        // left = 1, right = 3 --> (3) distribute left into right
        // left = 2, right = 2 --> (2) distribute left into right
        // left = 2, right = 3 --> (3) right but for each arg of right's arguments : arg = OR (arg, left);
        // left = 3, right = 3 --> (3) expand and distribute both

        // Now let us test each of these permutations
        // 1, 1
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(a, b)),
                and(a, b),
                "Failed 1,1");

        // Should not change shape
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(a, b)),
                or(a, b),
                "Failed to keep same form if cannot convert");

        // 1, 1 with not pushdown
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(not(and(a, b))),
                or(not(a), not(b)),
                "Failed 1,1 with NOT pushdown");

        // 1, 2
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(or(a, b), c)),
                or(or(a, b), c),
                "Failed 1,2");

        // 1, 3
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(a, b), c)),
                and(or(a, c), or(b, c)),
                "Failed 1,3");

        // 1, 3 with multiple clauses
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(a, and(b, or(e, g))), c)),
                and(and(or(a, c), or(b, c)), or(or(e, g), c)),
                "Failed 1,3 with multiple clauses");

        // 2, 2
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(or(a, b), or(c, d))),
                or(or(a, b), or(c, d)),
                "Failed 2,2");

        // 2, 3
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(or(a, b), and(or(e, f), or(g, h)))),
                and(or(or(a, b), or(e, f)), or(or(a, b), or(g, h))),
                "Failed 2,3");

        // 3, 3
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(or(a, b), or(c, d)), and(or(e, f), or(g, h)))),
                and(and(or(or(a, b), or(e, f)), or(or(a, b), or(g, h))), and(or(or(c, d), or(e, f)), or(or(c, d), or(g, h)))),
                "Failed 3,3");

        // Testing symmetry
        // 2, 1
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(c, or(a, b))),
                or(or(c, a), b),
                "Failed 2,1");

        // 3, 1
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(c, and(a, b))),
                and(or(c, a), or(c, b)),
                "Failed 3,1");

        // 3, 2
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(or(e, f), or(g, h)), or(a, b))),
                and(or(or(e, f), or(a, b)), or(or(g, h), or(a, b))),
                "Failed 3,2");

        // 3, 3 large with NOT push down
        // (a && b && (e || g)) || !(a && b) || !(b || !(c && d)) ==> (a || !a || !b) && (b || !a || !b) && ((e || g) || !a || !b)
        // notice that a || !a cannot be easily optimized away in SQL since we have to handle if a is unknown (null).
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(a, and(b, or(e, g))), or(not(and(a, b)), not(or(b, not(and(c, d))))))),
                and(and(or(or(a, not(a)), not(b)), or(or(b, not(a)), not(b))), or(or(e, g), or(not(a), not(b)))),
                "Failed 3,3 large with NOT pushdown");

        // a || b || c || d || (d && e) || (e && f) ==> (a || b || c || d || e) && (a || b || c || d || f)
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(
                or(a, or(b, or(c, or(d, or(and(d, e), and(e, f))))))),
                and(or(or(or(a, b), or(c, d)), e), or(or(or(a, b), or(c, d)), f)));

        // (a && b && c) || (d && e) can increase size significantly so do not expand.
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(a, and(b, c)), and(and(d, e), f))),
                or(and(and(a, b), c), and(and(d, e), f)));
    }

    @Test
    public void testBigExpressions()
    {
        // Do not expand big list (a0 && b0) || (a1 && b1) || ....
        RowExpression bigExpression = or(IntStream.range(0, 1000)
                .boxed()
                .map(i -> and(name("a" + i), name("b" + i)))
                .toArray(RowExpression[]::new));
        assertEquals(logicalRowExpressions.convertToConjunctiveNormalForm(bigExpression), bigExpression);

        // extract common predicates on (a && b0) || (a && b1) || ....
        RowExpression bigExpressionWithCommonPredicate = or(IntStream.range(0, 10001)
                .boxed()
                .map(i -> and(name("a"), name("b" + i)))
                .toArray(RowExpression[]::new));
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(bigExpressionWithCommonPredicate),
                or(IntStream.range(0, 10001).boxed().map(i -> and(name("a"), name("b" + i))).toArray(RowExpression[]::new)));
        // a || (a && b0) || (a && b1) || ... can be simplified to a but if conjunctive is very large, we will skip reduction.
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(a, bigExpressionWithCommonPredicate)),
                or(Stream.concat(Stream.of(name("a")), IntStream.range(0, 10001).boxed().map(i -> and(name("a"), name("b" + i)))).toArray(RowExpression[]::new)));
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(or(a, bigExpressionWithCommonPredicate)),
                or(Stream.concat(Stream.of(name("a")), IntStream.range(0, 10001).boxed().map(i -> and(name("a"), name("b" + i)))).toArray(RowExpression[]::new)));
    }

    @Test
    public void testConvertToDNF()
    {
        // 1, 1
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(or(a, b)),
                or(a, b),
                "Failed 1,1");

        // Should not change shape
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(a, b)),
                and(a, b),
                "Failed to keep same form if cannot convert");

        // 1, 1 with not pushdown
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(not(or(a, b))),
                and(not(a), not(b)),
                "Failed 1,1 with NOT pushdown");

        // 1, 2
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(and(a, b), c)),
                and(and(a, b), c),
                "Failed 1,2");

        // 1, 3
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(or(a, b), c)),
                or(and(a, c), and(b, c)),
                "Failed 1,3");

        // 1, 3 with multiple clauses
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(or(a, or(b, and(e, g))), c)),
                or(or(and(a, c), and(b, c)), and(and(e, g), c)),
                "Failed 1,3 with multiple clauses");

        // 2, 2
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(and(a, b), and(c, d))),
                and(and(a, b), and(c, d)),
                "Failed 2,2");

        // 2, 3
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(and(a, b), or(and(e, f), and(g, h)))),
                or(and(and(a, b), and(e, f)), and(and(a, b), and(g, h))),
                "Failed 2,3");

        // 3, 3
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(or(and(a, b), and(c, d)), or(and(e, f), and(g, h)))),
                or(or(and(and(a, b), and(e, f)), and(and(a, b), and(g, h))), or(and(and(c, d), and(e, f)), and(and(c, d), and(g, h)))),
                "Failed 3,3");

        // Testing symmetry
        // 2, 1
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(c, and(a, b))),
                and(and(c, a), b),
                "Failed 2,1");

        // 3, 1
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(c, or(a, b))),
                or(and(c, a), and(c, b)),
                "Failed 3,1");

        // 3, 2
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(or(and(e, f), and(g, h)), and(a, b))),
                or(and(and(e, f), and(a, b)), and(and(g, h), and(a, b))),
                "Failed 3,2");

        // 3, 3 large with NOT pushdown
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(or(a, or(b, and(e, g))), and(not(or(a, b)), not(and(b, not(or(c, d))))))),
                or(or(and(and(a, not(a)), not(b)), and(and(b, not(a)), not(b))), and(and(e, g), and(not(a), not(b)))),
                "Failed 3,3 large with NOT pushdown");

        // (a || b || c) && ( d || e) will expand to too big if we convert to disjunctive form.
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(and(or(a, or(b, c)), or(or(d, e), f))),
                and(or(or(a, b), c), or(or(d, e), f)));
    }

    private static RowExpression name(String name)
    {
        return new VariableReferenceExpression(name, BOOLEAN);
    }

    private static VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(name, BOOLEAN);
    }

    private RowExpression compare(RowExpression left, OperatorType operator, RowExpression right)
    {
        return call(
                operator.getOperator(),
                new FunctionResolution(functionAndTypeManager).comparisonFunction(operator, left.getType(), right.getType()),
                BOOLEAN,
                left,
                right);
    }

    private RowExpression and(RowExpression left, RowExpression right)
    {
        return new SpecialFormExpression(AND, BOOLEAN, left, right);
    }

    private RowExpression or(RowExpression... expressions)
    {
        return logicalRowExpressions.or(expressions);
    }

    private RowExpression or(RowExpression left, RowExpression right)
    {
        return new SpecialFormExpression(OR, BOOLEAN, left, right);
    }

    private RowExpression not(RowExpression expression)
    {
        return new CallExpression("not", new FunctionResolution(functionAndTypeManager).notFunction(), BOOLEAN, ImmutableList.of(expression));
    }
}
