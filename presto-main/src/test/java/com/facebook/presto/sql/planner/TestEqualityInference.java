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
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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

        assertEquals(
                inference.rewriteExpression(someExpression("a1", "a2"), matchesSymbols("d1", "d2")),
                someExpression("d1", "d2"));

        assertEquals(
                inference.rewriteExpression(someExpression("a1", "c1"), matchesSymbols("b1")),
                someExpression("b1", "b1"));

        assertEquals(
                inference.rewriteExpression(someExpression("a1", "a2"), matchesSymbols("b1", "d2", "c3")),
                someExpression("b1", "d2"));

        // Both starting expressions should canonicalize to the same expression
        assertEquals(
                inference.getScopedCanonical(nameReference("a2"), matchesSymbols("c2", "d2")),
                inference.getScopedCanonical(nameReference("b2"), matchesSymbols("c2", "d2")));
        Expression canonical = inference.getScopedCanonical(nameReference("a2"), matchesSymbols("c2", "d2"));

        // Given multiple translatable candidates, should choose the canonical
        assertEquals(
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

        assertEquals(expression, someExpression("a1", "a2"));
    }

    @Test
    public void testUnrewritable()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        addEquality("a1", "b1", builder);
        addEquality("a2", "b2", builder);
        EqualityInference inference = builder.build();

        assertNull(inference.rewriteExpression(someExpression("a1", "a2"), matchesSymbols("b1", "c1")));
        assertNull(inference.rewriteExpression(someExpression("c1", "c2"), matchesSymbols("a1", "a2")));
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
        assertEquals(expression, someExpression("c1", "c1"));
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
        assertEquals(nameReference("c1"), inference.rewriteExpression(nameReference("a1"), matchesSymbols("c1")));

        // But not be able to rewrite to d1 which is not connected via equality
        assertNull(inference.rewriteExpression(nameReference("a1"), matchesSymbols("d1")));
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

        EqualityInference.EqualityPartition emptyScopePartition = inference.generateEqualitiesPartitionedBy(Predicates.alwaysFalse());
        // Cannot generate any scope equalities with no matching symbols
        assertTrue(emptyScopePartition.getScopeEqualities().isEmpty());
        // All equalities should be represented in the inverse scope
        assertFalse(emptyScopePartition.getScopeComplementEqualities().isEmpty());
        // There should be no equalities straddling the scope
        assertTrue(emptyScopePartition.getScopeStraddlingEqualities().isEmpty());

        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(matchesSymbols("c1"));

        // There should be equalities in the scope, that only use c1 and are all inferrable equalities
        assertFalse(equalityPartition.getScopeEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), matchesSymbolScope(matchesSymbols("c1"))));
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be equalities in the inverse scope, that never use c1 and are all inferrable equalities
        assertFalse(equalityPartition.getScopeComplementEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesSymbolScope(not(matchesSymbols("c1")))));
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        assertFalse(equalityPartition.getScopeStraddlingEqualities().isEmpty());
        assertTrue(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(matchesSymbols("c1"))));
        assertTrue(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = new EqualityInference.Builder()
                .addAllEqualities(equalityPartition.getScopeEqualities())
                .addAllEqualities(equalityPartition.getScopeComplementEqualities())
                .addAllEqualities(equalityPartition.getScopeStraddlingEqualities())
                .build();

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(matchesSymbols("c1"));

        assertEquals(setCopy(equalityPartition.getScopeEqualities()), setCopy(newEqualityPartition.getScopeEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeComplementEqualities()), setCopy(newEqualityPartition.getScopeComplementEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeStraddlingEqualities()), setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
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
        assertFalse(equalityPartition.getScopeEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), matchesSymbolScope(symbolBeginsWith("a", "b"))));
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be equalities in the inverse scope, that never use a* and b* symbols and are all inferrable equalities
        assertFalse(equalityPartition.getScopeComplementEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesSymbolScope(not(symbolBeginsWith("a", "b")))));
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), EqualityInference.isInferenceCandidate()));

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        assertFalse(equalityPartition.getScopeStraddlingEqualities().isEmpty());
        assertTrue(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(symbolBeginsWith("a", "b"))));
        assertTrue(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), EqualityInference.isInferenceCandidate()));

        // Again, there should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = new EqualityInference.Builder()
                .addAllEqualities(equalityPartition.getScopeEqualities())
                .addAllEqualities(equalityPartition.getScopeComplementEqualities())
                .addAllEqualities(equalityPartition.getScopeStraddlingEqualities())
                .build();

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(symbolBeginsWith("a", "b"));

        assertEquals(setCopy(equalityPartition.getScopeEqualities()), setCopy(newEqualityPartition.getScopeEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeComplementEqualities()), setCopy(newEqualityPartition.getScopeComplementEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeStraddlingEqualities()), setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
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
        assertEquals(inference.rewriteExpression(add("b", "c"), symbolBeginsWith("a")), nameReference("a1"));

        // Only the sub-expression (b + c) should get rewritten in terms of a*
        assertEquals(inference.rewriteExpression(multiply(nameReference("ax"), add("b", "c")), symbolBeginsWith("a")), multiply(nameReference("ax"), nameReference("a1")));

        // To be compliant, could rewrite either the whole expression, or just the sub-expression. Rewriting larger expressions are preferred
        assertEquals(inference.rewriteExpression(multiply(nameReference("a1"), add("b", "c")), symbolBeginsWith("a")), nameReference("a3"));
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
        assertEquals(inference.rewriteExpression(nameReference("a1"), matchesSymbols("a1", "b1")), number(1));

        // All scope equalities should utilize the constant if possible
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(matchesSymbols("a1", "b1"));
        assertEquals(equalitiesAsSets(equalityPartition.getScopeEqualities()),
                set(set(nameReference("a1"), number(1)), set(nameReference("b1"), number(1))));
        assertEquals(equalitiesAsSets(equalityPartition.getScopeComplementEqualities()),
                set(set(nameReference("c1"), number(1))));

        // There should be no scope straddling equalities as the full set of equalities should be already represented by the scope and inverse scope
        assertTrue(equalityPartition.getScopeStraddlingEqualities().isEmpty());
    }

    @Test
    public void testEqualityGeneration()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        builder.addEquality(nameReference("a1"), add("b", "c")); // a1 = b + c
        builder.addEquality(nameReference("e1"), add("b", "d")); // e1 = b + d
        addEquality("c", "d", builder);
        EqualityInference inference = builder.build();

        Expression scopedCanonical = inference.getScopedCanonical(nameReference("e1"), symbolBeginsWith("a"));
        assertEquals(scopedCanonical, nameReference("a1"));
    }

    @Test
    public void testExpressionsThatMayReturnNullOnNonNullInput()
            throws Exception
    {
        List<Expression> candidates = ImmutableList.of(
                new Cast(nameReference("b"), "BIGINT", true), // try_cast
                new FunctionCall(QualifiedName.of("try"), ImmutableList.of(nameReference("b"))),
                new NullIfExpression(nameReference("b"), number(1)),
                new IfExpression(nameReference("b"), number(1), new NullLiteral()),
                new DereferenceExpression(nameReference("b"), "x"),
                new InPredicate(nameReference("b"), new InListExpression(ImmutableList.of(new NullLiteral()))),
                new SearchedCaseExpression(ImmutableList.of(new WhenClause(new IsNotNullPredicate(nameReference("b")), new NullLiteral())), Optional.empty()),
                new SimpleCaseExpression(nameReference("b"), ImmutableList.of(new WhenClause(number(1), new NullLiteral())), Optional.empty()),
                new SubscriptExpression(new ArrayConstructor(ImmutableList.of(new NullLiteral())), nameReference("b")));

        for (Expression candidate : candidates) {
            EqualityInference.Builder builder = new EqualityInference.Builder();
            builder.extractInferenceCandidates(equals(nameReference("b"), nameReference("x")));
            builder.extractInferenceCandidates(equals(nameReference("a"), candidate));

            EqualityInference inference = builder.build();
            List<Expression> equalities = inference.generateEqualitiesPartitionedBy(matchesSymbols("b")).getScopeStraddlingEqualities();
            assertEquals(equalities.size(), 1);
            assertTrue(equalities.get(0).equals(equals(nameReference("x"), nameReference("b"))) || equalities.get(0).equals(equals(nameReference("b"), nameReference("x"))));
        }
    }

    private static Predicate<Expression> matchesSymbolScope(final Predicate<Symbol> symbolScope)
    {
        return expression -> Iterables.all(DependencyExtractor.extractUnique(expression), symbolScope);
    }

    private static Predicate<Expression> matchesStraddlingScope(final Predicate<Symbol> symbolScope)
    {
        return expression -> {
            Set<Symbol> symbols = DependencyExtractor.extractUnique(expression);
            return Iterables.any(symbols, symbolScope) && Iterables.any(symbols, not(symbolScope));
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
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.ADD, expression1, expression2);
    }

    private static Expression multiply(String symbol1, String symbol2)
    {
        return multiply(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression multiply(Expression expression1, Expression expression2)
    {
        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.MULTIPLY, expression1, expression2);
    }

    private static Expression equals(String symbol1, String symbol2)
    {
        return equals(nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression equals(Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(EQUAL, expression1, expression2);
    }

    private static SymbolReference nameReference(String symbol)
    {
        return new SymbolReference(symbol);
    }

    private static LongLiteral number(long number)
    {
        return new LongLiteral(String.valueOf(number));
    }

    private static Predicate<Symbol> matchesSymbols(String... symbols)
    {
        return matchesSymbols(Arrays.asList(symbols));
    }

    private static Predicate<Symbol> matchesSymbols(Collection<String> symbols)
    {
        final Set<Symbol> symbolSet = symbols.stream()
                .map(Symbol::new)
                .collect(toImmutableSet());

        return Predicates.in(symbolSet);
    }

    private static Predicate<Symbol> symbolBeginsWith(String... prefixes)
    {
        return symbolBeginsWith(Arrays.asList(prefixes));
    }

    private static Predicate<Symbol> symbolBeginsWith(final Iterable<String> prefixes)
    {
        return symbol -> {
            for (String prefix : prefixes) {
                if (symbol.getName().startsWith(prefix)) {
                    return true;
                }
            }
            return false;
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
