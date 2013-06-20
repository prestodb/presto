package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.planner.optimizations.PredicatePushDown.EqualityInference;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;

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
                inference.rewritePredicate(someExpression("a1", "a2"), matchesSymbols("d1", "d2")),
                someExpression("d1", "d2"));

        Assert.assertEquals(
                inference.rewritePredicate(someExpression("a1", "c1"), matchesSymbols("b1")),
                someExpression("b1", "b1"));

        Assert.assertEquals(
                inference.rewritePredicate(someExpression("a1", "a2"), matchesSymbols("b1", "d2", "c3")),
                someExpression("b1", "d2"));

        // Both starting symbols should canonicalize to the same symbol
        Assert.assertEquals(
                inference.canonicalScopedSymbol(new Symbol("a2"), matchesSymbols("c2", "d2")),
                inference.canonicalScopedSymbol(new Symbol("b2"), matchesSymbols("c2", "d2")));
        Symbol canonical = inference.canonicalScopedSymbol(new Symbol("a2"), matchesSymbols("c2", "d2"));

        // Given multiple translatable candidates, should choose the canonical
        Assert.assertEquals(
                inference.rewritePredicate(someExpression("a2", "b2"), matchesSymbols("c2", "d2")),
                someExpression(canonical.getName(), canonical.getName()));
    }

    @Test
    public void testTriviallyRewritable()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        Expression expression = builder.build()
                .rewritePredicate(someExpression("a1", "a2"), matchesSymbols("a1", "a2"));

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

        Assert.assertNull(inference.rewritePredicate(someExpression("a1", "a2"), matchesSymbols("b1", "c1")));
        Assert.assertNull(inference.rewritePredicate(someExpression("c1", "c2"), matchesSymbols("a1", "a2")));
    }

    @Test
    public void testParseEqualityExpression()
            throws Exception
    {
        EqualityInference inference = new EqualityInference.Builder()
                .addEquality(equalExpression("a1", "b1"))
                .addEquality(equalExpression("a1", "c1"))
                .addEquality(equalExpression("c1", "a1"))
                .build();

        Expression expression = inference.rewritePredicate(someExpression("a1", "b1"), matchesSymbols("c1"));
        Assert.assertEquals(expression, someExpression("c1", "c1"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidEqualityExpression1()
            throws Exception
    {
        new EqualityInference.Builder()
                .addEquality(equalExpression("a1", "a1"));
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
    public void testEqualityPredicateGeneration()
            throws Exception
    {
        EqualityInference.Builder builder = new EqualityInference.Builder();
        addEquality("a1", "b1", builder);
        addEquality("b1", "c1", builder);
        addEquality("d1", "c1", builder);

        addEquality("a2", "b2", builder);
        addEquality("b2", "c2", builder);
        addEquality("d2", "b2", builder);
        addEquality("c2", "d2", builder);

        EqualityInference inference = builder.build();

        // Cannot generate any equalities with no symbols
        Assert.assertTrue(inference.scopedEqualityPredicates(Predicates.<Symbol>alwaysFalse()).isEmpty());
        // There are no partitions to bridge
        Assert.assertTrue(inference.scopeBridgePredicates(Predicates.<Symbol>alwaysFalse()).isEmpty());

        // Cannot generate any equalities with only one symbol in the set
        Assert.assertTrue(inference.scopedEqualityPredicates(matchesSymbols("a1")).isEmpty());
        // The corresponding bridge predicate should link the single symbol with the canonical on the other side of the predicate
        Assert.assertEquals(ImmutableSet.copyOf(inference.scopeBridgePredicates(matchesSymbols("a1"))),
                ImmutableSet.of(equalExpression("a1", "b1")));

        List<Expression> expressions = inference.scopedEqualityPredicates(matchesSymbols("a1", "c1"));
        Assert.assertEquals(expressions.size(), 1);
        Assert.assertEquals(ImmutableSet.copyOf(expressions),
                ImmutableSet.of(equalExpression("a1", "c1")));
        // Choose the two canonical symbols from each side of the partition for the bridge
        List<Expression> bridges = inference.scopeBridgePredicates(matchesSymbols("a1", "c1"));
        Assert.assertEquals(ImmutableSet.copyOf(bridges),
                ImmutableSet.of(equalExpression("a1", "b1")));

        expressions = inference.scopedEqualityPredicates(matchesSymbols("a2", "c2", "d2"));
        Assert.assertEquals(expressions.size(), 2);
        Assert.assertEquals(ImmutableSet.copyOf(expressions),
                ImmutableSet.of(equalExpression("a2", "c2"), equalExpression("a2", "d2")));
        bridges = inference.scopeBridgePredicates(matchesSymbols("a2", "c2", "d2"));
        Assert.assertEquals(ImmutableSet.copyOf(bridges),
                ImmutableSet.of(equalExpression("a2", "b2")));

        // Generating equalities for disjoint groups
        expressions = inference.scopedEqualityPredicates(symbolBeginsWith("a", "b"));
        Assert.assertEquals(expressions.size(), 2);
        Assert.assertEquals(ImmutableSet.copyOf(expressions),
                ImmutableSet.of(equalExpression("a1", "b1"), equalExpression("a2", "b2")));
        // Two bridges necessary to connect the two partitions
        bridges = inference.scopeBridgePredicates(symbolBeginsWith("a", "b"));
        Assert.assertEquals(ImmutableSet.copyOf(bridges),
                ImmutableSet.of(equalExpression("a1", "c1"), equalExpression("a2", "c2")));
    }

    private static void addEquality(String symbol1, String symbol2, EqualityInference.Builder builder)
    {
        builder.addEquality(new Symbol(symbol1), new Symbol(symbol2));
    }

    private static Expression someExpression(String symbol1, String symbol2)
    {
        return new ComparisonExpression(GREATER_THAN, nameReference(symbol1), nameReference(symbol2));
    }

    private static Expression equalExpression(String symbol1, String symbol2)
    {
        return new ComparisonExpression(EQUAL, nameReference(symbol1), nameReference(symbol2));
    }

    private static QualifiedNameReference nameReference(String symbol)
    {
        return new QualifiedNameReference(new Symbol(symbol).toQualifiedName());
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
        return new Predicate<Symbol>()
        {
            @Override
            public boolean apply(Symbol symbol)
            {
                return symbolSet.contains(symbol);
            }
        };
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
}
