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
