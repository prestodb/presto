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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.google.common.base.Predicates.not;

public class TestEqualityInference
{
    @Test
    public void testTransitivity()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        addEquality("a1", "b1", builder);
        addEquality("b1", "c1", builder);
        addEquality("d1", "c1", builder);

        addEquality("a2", "b2", builder);
        addEquality("b2", "a2", builder);
        addEquality("b2", "c2", builder);
        addEquality("d2", "b2", builder);
        addEquality("c2", "d2", builder);

        EqualityInference inference = builder.build();

        Assert.assertEquals(
                inference.rewriteExpression(someExpression("a1", "a2"), matchesSymbols("d1", "d2")),
                someExpression("d1", "d2"));

        Assert.assertEquals(
                inference.rewriteExpression(someExpression("a1", "c1"), matchesSymbols("b1")),
                someExpression("b1", "b1"));

        Assert.assertEquals(
                inference.rewriteExpression(someExpression("a1", "a2"), matchesSymbols("b1", "d2", "c3")),
                someExpression("b1", "d2"));

        // Both starting expressions should canonicalize to the same expression
        Assert.assertEquals(
                inference.getScopedCanonical(nameReference("a2"), matchesSymbols("c2", "d2")),
                inference.getScopedCanonical(nameReference("b2"), matchesSymbols("c2", "d2")));
        Expression canonical = inference.getScopedCanonical(nameReference("a2"), matchesSymbols("c2", "d2"));

        // Given multiple translatable candidates, should choose the canonical
        Assert.assertEquals(
                inference.rewriteExpression(someExpression("a2", "b2"), matchesSymbols("c2", "d2")),
                someExpression(canonical, canonical));
    }

    @Test
    public void testTriviallyRewritable()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        Expression expression = builder.build()
                .rewriteExpression(someExpression("a1", "a2"), matchesSymbols("a1", "a2"));

        Assert.assertEquals(expression, someExpression("a1", "a2"));
    }

    @Test
    public void testUnrewritable()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        addEquality("a1", "b1", builder);
        addEquality("a2", "b2", builder);
        EqualityInference inference = builder.build();

        Assert.assertNull(inference.rewriteExpression(someExpression("a1", "a2"), matchesSymbols("b1", "c1")));
        Assert.assertNull(inference.rewriteExpression(someExpression("c1", "c2"), matchesSymbols("a1", "a2")));
    }

    @Test
    public void testParseEqualityExpression()
            throws Exception
    {
        EqualityInference inference = new EqualityInference.Builder()
                .addEquality(equals("a1", "b1"))
                .addEquality(equals("a1", "c1"))
                .addEquality(equals("c1", "a1"))
                .build();

        Expression expression = inference.rewriteExpression(someExpression("a1", "b1"), matchesSymbols("c1"));
        Assert.assertEquals(expression, someExpression("c1", "c1"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidEqualityExpression1()
            throws Exception
    {
        new EqualityInference.Builder()
                .addEquality(equals("a1", "a1"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidEqualityExpression2()
            throws Exception
    {
        new EqualityInference.Builder()
                .addEquality(someExpression("a1", "b1"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidEqualityExpression3()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        addEquality("a1", "a1", builder);
    }

    @Test
    public void testExtractInferrableEqualities()
            throws Exception
    {
        EqualityInference inference = new EqualityInference.Builder()
                .extractInferenceCandidates(ExpressionUtils.and(equals("a1", "b1"), equals("b1", "c1"), someExpression("c1", "d1")))
                .build();

        // Able to rewrite to c1 due to equalities
        Assert.assertEquals(nameReference("c1"), inference.rewriteExpression(nameReference("a1"), matchesSymbols("c1")));

        // But not be able to rewrite to d1 which is not connected via equality
        Assert.assertNull(inference.rewriteExpression(nameReference("a1"), matchesSymbols("d1")));
    }

    @Test
    public void testEqualityPartitionGeneration()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        builder.addEquality(nameReference("a1"), nameReference("b1"));
        builder.addEquality(add("a1", "a1"), multiply(nameReference("a1"), number(2)));
        builder.addEquality(nameReference("b1"), nameReference("c1"));
        builder.addEquality(add("a1", "a1"), nameReference("c1"));
        builder.addEquality(add("a1", "b1"), nameReference("c1"));

        EqualityInference inference = builder.build();

        EqualityInference.EqualityPartition emptyScopePartition = inference.generateEqualitiesPartitionedBy(Predicates.<Symbol>alwaysFalse());
        // Cannot generate any scope equalities with no matching symbols
        Assert.assertTrue(emptyScopePartition.getScopeEqualities().isEmpty());
        // All equalities should be represented in the inverse scope
        Assert.assertFalse(emptyScopePartition.getScopeComplementEqualities().isEmpty());
        // There should be no equalities straddling the scope
        Assert.assertTrue(emptyScopePartition.getScopeStraddlingEqualities().isEmpty());

        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(matchesSymbols("c1"));

        // There should be equalities in the scope, that only use c1 and are all inferrable equalities
        Assert.assertFalse(equalityPartition.getScopeEqualities().isEmpty());
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), matchesSymbolScope(matchesSymbols("c1"))));
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be equalities in the inverse scope, that never use c1 and are all inferrable equalities
        Assert.assertFalse(equalityPartition.getScopeComplementEqualities().isEmpty());
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesSymbolScope(not(matchesSymbols("c1")))));
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        Assert.assertFalse(equalityPartition.getScopeStraddlingEqualities().isEmpty());
        Assert.assertTrue(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(matchesSymbols("c1"))));
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = new EqualityInference.Builder()
                .addAllEqualities(equalityPartition.getScopeEqualities())
                .addAllEqualities(equalityPartition.getScopeComplementEqualities())
                .addAllEqualities(equalityPartition.getScopeStraddlingEqualities())
                .build();

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(matchesSymbols("c1"));

        Assert.assertEquals(setCopy(equalityPartition.getScopeEqualities()), setCopy(newEqualityPartition.getScopeEqualities()));
        Assert.assertEquals(setCopy(equalityPartition.getScopeComplementEqualities()), setCopy(newEqualityPartition.getScopeComplementEqualities()));
        Assert.assertEquals(setCopy(equalityPartition.getScopeStraddlingEqualities()), setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
    }

    @Test
    public void testMultipleEqualitySetsPredicateGeneration()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        addEquality("a1", "b1", builder);
        addEquality("b1", "c1", builder);
        addEquality("c1", "d1", builder);

        addEquality("a2", "b2", builder);
        addEquality("b2", "c2", builder);
        addEquality("c2", "d2", builder);

        EqualityInference inference = builder.build();

        // Generating equalities for disjoint groups
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(symbolBeginsWith("a", "b"));

        // There should be equalities in the scope, that only use a* and b* symbols and are all inferrable equalities
        Assert.assertFalse(equalityPartition.getScopeEqualities().isEmpty());
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), matchesSymbolScope(symbolBeginsWith("a", "b"))));
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be equalities in the inverse scope, that never use a* and b* symbols and are all inferrable equalities
        Assert.assertFalse(equalityPartition.getScopeComplementEqualities().isEmpty());
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesSymbolScope(not(symbolBeginsWith("a", "b")))));
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        Assert.assertFalse(equalityPartition.getScopeStraddlingEqualities().isEmpty());
        Assert.assertTrue(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(symbolBeginsWith("a", "b"))));
        Assert.assertTrue(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), EqualityInference.isInferenceCandidate()));

        // Again, there should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = new EqualityInference.Builder()
                .addAllEqualities(equalityPartition.getScopeEqualities())
                .addAllEqualities(equalityPartition.getScopeComplementEqualities())
                .addAllEqualities(equalityPartition.getScopeStraddlingEqualities())
                .build();

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(symbolBeginsWith("a", "b"));

        Assert.assertEquals(setCopy(equalityPartition.getScopeEqualities()), setCopy(newEqualityPartition.getScopeEqualities()));
        Assert.assertEquals(setCopy(equalityPartition.getScopeComplementEqualities()), setCopy(newEqualityPartition.getScopeComplementEqualities()));
        Assert.assertEquals(setCopy(equalityPartition.getScopeStraddlingEqualities()), setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
    }

    @Test
    public void testSubExpressionRewrites()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        builder.addEquality(nameReference("a1"), add("b", "c")); // a1 = b + c
        builder.addEquality(nameReference("a2"), multiply(nameReference("b"), add("b", "c"))); // a2 = b * (b + c)
        builder.addEquality(nameReference("a3"), multiply(nameReference("a1"), add("b", "c"))); // a3 = a1 * (b + c)
        EqualityInference inference = builder.build();

        // Expression (b + c) should get entirely rewritten as a1
        Assert.assertEquals(inference.rewriteExpression(add("b", "c"), symbolBeginsWith("a")), nameReference("a1"));

        // Only the sub-expression (b + c) should get rewritten in terms of a*
        Assert.assertEquals(inference.rewriteExpression(multiply(nameReference("ax"), add("b", "c")), symbolBeginsWith("a")), multiply(nameReference("ax"), nameReference("a1")));

        // To be compliant, could rewrite either the whole expression, or just the sub-expression. Rewriting larger expressions are preferred
        Assert.assertEquals(inference.rewriteExpression(multiply(nameReference("a1"), add("b", "c")), symbolBeginsWith("a")), nameReference("a3"));
    }

    @Test
    public void testConstantEqualities()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        addEquality("a1", "b1", builder);
        addEquality("b1", "c1", builder);
        builder.addEquality(nameReference("c1"), number(1));
        EqualityInference inference = builder.build();

        // Should always prefer a constant if available (constant is part of all scopes)
        Assert.assertEquals(inference.rewriteExpression(nameReference("a1"), matchesSymbols("a1", "b1")), number(1));

        // All scope equalities should utilize the constant if possible
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(matchesSymbols("a1", "b1"));
        Assert.assertEquals(equalitiesAsSets(equalityPartition.getScopeEqualities()),
                set(set(nameReference("a1"), number(1)), set(nameReference("b1"), number(1))));
        Assert.assertEquals(equalitiesAsSets(equalityPartition.getScopeComplementEqualities()),
                set(set(nameReference("c1"), number(1))));

        // There should be no scope straddling equalities as the full set of equalities should be already represented by the scope and inverse scope
        Assert.assertTrue(equalityPartition.getScopeStraddlingEqualities().isEmpty());
    }

    private static Predicate<Expression> matchesSymbolScope(final Predicate<Symbol> symbolScope)
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(Expression expression)
            {
                return Iterables.all(DependencyExtractor.extractUnique(expression), symbolScope);
            }
        };
    }

    private static Predicate<Expression> matchesStraddlingScope(final Predicate<Symbol> symbolScope)
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(Expression expression)
            {
                Set<Symbol> symbols = DependencyExtractor.extractUnique(expression);
                return Iterables.any(symbols, symbolScope) && Iterables.any(symbols, not(symbolScope));
            }
        };
    }

    private static void addEquality(String symbol1, String symbol2, EqualityInference.Builder builder)
    {
        builder.addEquality(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression someExpression(String symbol1, String symbol2)
    {
        return someExpression(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression someExpression(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(GREATER_THAN, expression1, expression2);
    }

    private static Expression add(String symbol1, String symbol2)
    {
        return add(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression add(Expression expression1, Expression expression2)
    {
        return new ArithmeticExpression(ArithmeticExpression.Type.ADD, expression1, expression2);
    }

    private static Expression multiply(String symbol1, String symbol2)
    {
        return multiply(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression multiply(Expression expression1, Expression expression2)
    {
        return new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, expression1, expression2);
    }

    private static Expression equals(String symbol1, String symbol2)
    {
        return equals(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression equals(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(EQUAL, expression1, expression2);
    }

    private static QualifiedNameReference nameReference(String symbol)
    {
        return new QualifiedNameReference(new Symbol(symbol).toQualifiedName());
    }

    private static LongLiteral number(long number)
    {
        return new LongLiteral(String.valueOf(number));
    }

    private static Predicate<Symbol> matchesSymbols(String... symbols)
    {
        return matchesSymbols(Arrays.asList(symbols));
    }

    private static Predicate<Symbol> matchesSymbols(Iterable<String> symbols)
    {
        final Set<Symbol> symbolSet = IterableTransformer.<String>on(symbols)
                .transform(new Function<String, Symbol>()
                {
                    @Override
                    public Symbol apply(String symbol)
                    {
                        return new Symbol(symbol);
                    }
                }).set();
        return Predicates.in(symbolSet);
    }

    private static Predicate<Symbol> symbolBeginsWith(String... prefixes)
    {
        return symbolBeginsWith(Arrays.asList(prefixes));
    }

    private static Predicate<Symbol> symbolBeginsWith(final Iterable<String> prefixes)
    {
        return new Predicate<Symbol>()
        {
            @Override
            public boolean apply(Symbol symbol)
            {
                for (String prefix : prefixes) {
                    if (symbol.getName().startsWith(prefix)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    private static Set<Set<Expression>> equalitiesAsSets(Iterable<Expression> expressions)
    {
        ImmutableSet.Builder<Set<Expression>> builder = ImmutableSet.builder();
        for (Expression expression : expressions) {
            builder.add(equalityAsSet(expression));
        }
        return builder.build();
    }

    private static Set<Expression> equalityAsSet(Expression expression)
    {
        Preconditions.checkArgument(expression instanceof ComparisonExpression);
        ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
        Preconditions.checkArgument(comparisonExpression.getType() == EQUAL);
        return ImmutableSet.of(comparisonExpression.getLeft(), comparisonExpression.getRight());
    }

    private static <E> Set<E> set(E... elements)
    {
        return setCopy(Arrays.asList(elements));
    }

    private static <E> Set<E> setCopy(Iterable<E> elements)
    {
        return ImmutableSet.copyOf(elements);
    }
}
