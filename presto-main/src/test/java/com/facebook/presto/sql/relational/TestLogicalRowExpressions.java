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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.relation.LogicalRowExpressions.extractPredicates;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static org.testng.Assert.assertEquals;

public class TestLogicalRowExpressions
{
    private FunctionManager functionManager;
    private LogicalRowExpressions logicalRowExpressions;

    @BeforeClass
    public void setup()
    {
        TypeManager typeManager = new TypeRegistry();
        functionManager = new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(functionManager), new FunctionResolution(functionManager));
    }

    @Test
    public void testAnd()
    {
        RowExpression a = name("a");
        RowExpression b = name("b");
        RowExpression c = name("c");
        RowExpression d = name("d");
        RowExpression e = name("e");

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
    public void testOr()
    {
        RowExpression a = name("a");
        RowExpression b = name("b");
        RowExpression c = name("c");
        RowExpression d = name("d");
        RowExpression e = name("e");

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
    public void testDeterminism()
    {
        RowExpression a = name("a");
        RowExpression b = name("b");
        RowExpression nondeterministic = call("random", functionManager.lookupFunction("random", fromTypes()), DOUBLE);
        RowExpression deterministic = call("length", functionManager.lookupFunction("length", fromTypes(VARCHAR)), INTEGER);

        RowExpression expression = and(and(a, or(b, nondeterministic)), deterministic);

        assertEquals(logicalRowExpressions.filterDeterministicConjuncts(expression), and(a, deterministic));
        assertEquals(logicalRowExpressions.filterNonDeterministicConjuncts(expression), or(b, nondeterministic));
    }

    @Test
    public void testPushNegationToLeaves()
    {
        RowExpression a = name("a");
        RowExpression b = name("b");
        RowExpression c = name("c");
        RowExpression d = name("d");

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
        RowExpression a = name("a");
        RowExpression b = name("b");
        RowExpression c = name("c");
        RowExpression d = name("d");
        RowExpression e = name("e");
        RowExpression f = name("f");
        RowExpression g = name("g");
        RowExpression h = name("h");

        // 1, 1
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(and(a, b)),
                and(a, b),
                "Failed 1,1");

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
                or(c, or(a, b)),
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

        // 3, 3 large with NOT pushdown
        assertEquals(
                logicalRowExpressions.convertToConjunctiveNormalForm(or(and(a, and(b, or(e, g))), or(not(and(a, b)), not(or(b, not(and(c, d))))))),
                and(and(and(and(or(a, or(or(not(a), not(b)), not(b))), or(a, or(or(not(a), not(b)), c))), and(or(a, or(or(not(a), not(b)), d)), or(b, or(or(not(a), not(b)), not(b))))), and(and(or(b, or(or(not(a), not(b)), c)), or(b, or(or(not(a), not(b)), d))), and(or(or(e, g), or(or(not(a), not(b)), not(b))), or(or(e, g), or(or(not(a), not(b)), c))))), or(or(e, g), or(or(not(a), not(b)), d))),
                "Failed 3,3 large with NOT pushdown");
    }

    @Test
    public void testConvertToDNF()
    {
        RowExpression a = name("a");
        RowExpression b = name("b");
        RowExpression c = name("c");
        RowExpression d = name("d");
        RowExpression e = name("e");
        RowExpression f = name("f");
        RowExpression g = name("g");
        RowExpression h = name("h");

        // 1, 1
        assertEquals(
                logicalRowExpressions.convertToDisjunctiveNormalForm(or(a, b)),
                or(a, b),
                "Failed 1,1");

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
                and(c, and(a, b)),
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
                or(or(or(or(and(a, and(and(not(a), not(b)), not(b))), and(a, and(and(not(a), not(b)), c))), or(and(a, and(and(not(a), not(b)), d)), and(b, and(and(not(a), not(b)), not(b))))), or(or(and(b, and(and(not(a), not(b)), c)), and(b, and(and(not(a), not(b)), d))), or(and(and(e, g), and(and(not(a), not(b)), not(b))), and(and(e, g), and(and(not(a), not(b)), c))))), and(and(e, g), and(and(not(a), not(b)), d))),
                "Failed 3,3 large with NOT pushdown");
    }

    private static RowExpression name(String name)
    {
        return new VariableReferenceExpression(name, BOOLEAN);
    }

    private RowExpression and(RowExpression left, RowExpression right)
    {
        return new SpecialFormExpression(AND, BOOLEAN, left, right);
    }

    private RowExpression or(RowExpression left, RowExpression right)
    {
        return new SpecialFormExpression(OR, BOOLEAN, left, right);
    }

    private RowExpression not(RowExpression expression)
    {
        return new CallExpression("not", new FunctionResolution(functionManager).notFunction(), BOOLEAN, ImmutableList.of(expression));
    }
}
