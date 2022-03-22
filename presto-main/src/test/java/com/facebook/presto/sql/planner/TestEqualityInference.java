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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.relational.Expressions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.RowType.field;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.EqualityInference.Builder.isInferenceCandidate;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEqualityInference
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TestingRowExpressionTranslator ROW_EXPRESSION_TRANSLATOR = new TestingRowExpressionTranslator(METADATA);

    @Test
    public void testTransitivity()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
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
                inference.rewriteExpression(someExpression("a1", "a2"), matchesVariables("d1", "d2")),
                someExpression("d1", "d2"));

        assertEquals(
                inference.rewriteExpression(someExpression("a1", "c1"), matchesVariables("b1")),
                someExpression("b1", "b1"));

        assertEquals(
                inference.rewriteExpression(someExpression("a1", "a2"), matchesVariables("b1", "d2", "c3")),
                someExpression("b1", "d2"));

        // Both starting expressions should canonicalize to the same expression
        assertEquals(
                inference.getScopedCanonical(variable("a2"), matchesVariables("c2", "d2")),
                inference.getScopedCanonical(variable("b2"), matchesVariables("c2", "d2")));
        RowExpression canonical = inference.getScopedCanonical(variable("a2"), matchesVariables("c2", "d2"));

        // Given multiple translatable candidates, should choose the canonical
        assertEquals(
                inference.rewriteExpression(someExpression("a2", "b2"), matchesVariables("c2", "d2")),
                someExpression(canonical, canonical));
    }

    @Test
    public void testTriviallyRewritable()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        RowExpression expression = builder.build()
                .rewriteExpression(someExpression("a1", "a2"), matchesVariables("a1", "a2"));

        assertEquals(expression, someExpression("a1", "a2"));
    }

    @Test
    public void testUnrewritable()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        addEquality("a1", "b1", builder);
        addEquality("a2", "b2", builder);
        EqualityInference inference = builder.build();

        assertNull(inference.rewriteExpression(someExpression("a1", "a2"), matchesVariables("b1", "c1")));
        assertNull(inference.rewriteExpression(someExpression("c1", "c2"), matchesVariables("a1", "a2")));
    }

    @Test
    public void testParseEqualityExpression()
    {
        EqualityInference inference = new EqualityInference.Builder(METADATA)
                .addEquality(equals("a1", "b1"))
                .addEquality(equals("a1", "c1"))
                .addEquality(equals("c1", "a1"))
                .build();

        RowExpression expression = inference.rewriteExpression(someExpression("a1", "b1"), matchesVariables("c1"));
        assertEquals(expression, someExpression("c1", "c1"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidEqualityExpression1()
    {
        new EqualityInference.Builder(METADATA)
                .addEquality(equals("a1", "a1"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidEqualityExpression2()
    {
        new EqualityInference.Builder(METADATA)
                .addEquality(someExpression("a1", "b1"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidEqualityExpression3()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        addEquality("a1", "a1", builder);
    }

    @Test
    public void testExtractInferrableEqualities()
    {
        EqualityInference inference = new EqualityInference.Builder(METADATA)
                .extractInferenceCandidates(and(equals("a1", "b1"), equals("b1", "c1"), someExpression("c1", "d1")))
                .build();

        // Able to rewrite to c1 due to equalities
        assertEquals(variable("c1"), inference.rewriteExpression(variable("a1"), matchesVariables("c1")));

        // But not be able to rewrite to d1 which is not connected via equality
        assertNull(inference.rewriteExpression(variable("a1"), matchesVariables("d1")));
    }

    @Test
    public void testEqualityPartitionGeneration()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        builder.addEquality(variable("a1"), variable("b1"));
        builder.addEquality(add("a1", "a1"), multiply(variable("a1"), number(2)));
        builder.addEquality(variable("b1"), variable("c1"));
        builder.addEquality(add("a1", "a1"), variable("c1"));
        builder.addEquality(add("a1", "b1"), variable("c1"));

        EqualityInference inference = builder.build();

        EqualityInference.EqualityPartition emptyScopePartition = inference.generateEqualitiesPartitionedBy(Predicates.alwaysFalse());
        // Cannot generate any scope equalities with no matching symbols
        assertTrue(emptyScopePartition.getScopeEqualities().isEmpty());
        // All equalities should be represented in the inverse scope
        assertFalse(emptyScopePartition.getScopeComplementEqualities().isEmpty());
        // There should be no equalities straddling the scope
        assertTrue(emptyScopePartition.getScopeStraddlingEqualities().isEmpty());

        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(matchesVariables("c1"));

        // There should be equalities in the scope, that only use c1 and are all inferrable equalities
        assertFalse(equalityPartition.getScopeEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), matchesVariableScope(matchesVariables("c1"))));
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), isInferenceCandidate(METADATA)));

        // There should be equalities in the inverse scope, that never use c1 and are all inferrable equalities
        assertFalse(equalityPartition.getScopeComplementEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesVariableScope(not(matchesVariables("c1")))));
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), isInferenceCandidate(METADATA)));

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        assertFalse(equalityPartition.getScopeStraddlingEqualities().isEmpty());
        assertTrue(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(matchesVariables("c1"))));
        assertTrue(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), isInferenceCandidate(METADATA)));

        // There should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = new EqualityInference.Builder(METADATA)
                .addAllEqualities(equalityPartition.getScopeEqualities())
                .addAllEqualities(equalityPartition.getScopeComplementEqualities())
                .addAllEqualities(equalityPartition.getScopeStraddlingEqualities())
                .build();

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(matchesVariables("c1"));

        assertEquals(setCopy(equalityPartition.getScopeEqualities()), setCopy(newEqualityPartition.getScopeEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeComplementEqualities()), setCopy(newEqualityPartition.getScopeComplementEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeStraddlingEqualities()), setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
    }

    @Test
    public void testMultipleEqualitySetsPredicateGeneration()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        addEquality("a1", "b1", builder);
        addEquality("b1", "c1", builder);
        addEquality("c1", "d1", builder);

        addEquality("a2", "b2", builder);
        addEquality("b2", "c2", builder);
        addEquality("c2", "d2", builder);

        EqualityInference inference = builder.build();

        // Generating equalities for disjoint groups
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(variableBeginsWith("a", "b"));

        // There should be equalities in the scope, that only use a* and b* symbols and are all inferrable equalities
        assertFalse(equalityPartition.getScopeEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), matchesVariableScope(variableBeginsWith("a", "b"))));
        assertTrue(Iterables.all(equalityPartition.getScopeEqualities(), isInferenceCandidate(METADATA)));

        // There should be equalities in the inverse scope, that never use a* and b* symbols and are all inferrable equalities
        assertFalse(equalityPartition.getScopeComplementEqualities().isEmpty());
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), matchesVariableScope(not(variableBeginsWith("a", "b")))));
        assertTrue(Iterables.all(equalityPartition.getScopeComplementEqualities(), isInferenceCandidate(METADATA)));

        // There should be equalities in the straddling scope, that should use both c1 and not c1 symbols
        assertFalse(equalityPartition.getScopeStraddlingEqualities().isEmpty());
        assertTrue(Iterables.any(equalityPartition.getScopeStraddlingEqualities(), matchesStraddlingScope(variableBeginsWith("a", "b"))));
        assertTrue(Iterables.all(equalityPartition.getScopeStraddlingEqualities(), isInferenceCandidate(METADATA)));

        // Again, there should be a "full cover" of all of the equalities used
        // THUS, we should be able to plug the generated equalities back in and get an equivalent set of equalities back the next time around
        EqualityInference newInference = new EqualityInference.Builder(METADATA)
                .addAllEqualities(equalityPartition.getScopeEqualities())
                .addAllEqualities(equalityPartition.getScopeComplementEqualities())
                .addAllEqualities(equalityPartition.getScopeStraddlingEqualities())
                .build();

        EqualityInference.EqualityPartition newEqualityPartition = newInference.generateEqualitiesPartitionedBy(variableBeginsWith("a", "b"));

        assertEquals(setCopy(equalityPartition.getScopeEqualities()), setCopy(newEqualityPartition.getScopeEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeComplementEqualities()), setCopy(newEqualityPartition.getScopeComplementEqualities()));
        assertEquals(setCopy(equalityPartition.getScopeStraddlingEqualities()), setCopy(newEqualityPartition.getScopeStraddlingEqualities()));
    }

    @Test
    public void testSubExpressionRewrites()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        builder.addEquality(variable("a1"), add("b", "c")); // a1 = b + c
        builder.addEquality(variable("a2"), multiply(variable("b"), add("b", "c"))); // a2 = b * (b + c)
        builder.addEquality(variable("a3"), multiply(variable("a1"), add("b", "c"))); // a3 = a1 * (b + c)
        EqualityInference inference = builder.build();

        // Expression (b + c) should get entirely rewritten as a1
        assertEquals(inference.rewriteExpression(add("b", "c"), variableBeginsWith("a")), variable("a1"));

        // Only the sub-expression (b + c) should get rewritten in terms of a*
        assertEquals(inference.rewriteExpression(multiply(variable("ax"), add("b", "c")), variableBeginsWith("a")), multiply(variable("ax"), variable("a1")));

        // To be compliant, could rewrite either the whole expression, or just the sub-expression. Rewriting larger expressions are preferred
        assertEquals(inference.rewriteExpression(multiply(variable("a1"), add("b", "c")), variableBeginsWith("a")), variable("a3"));
    }

    @Test
    public void testConstantEqualities()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        addEquality("a1", "b1", builder);
        addEquality("b1", "c1", builder);
        builder.addEquality(variable("c1"), number(1));
        EqualityInference inference = builder.build();

        // Should always prefer a constant if available (constant is part of all scopes)
        assertEquals(inference.rewriteExpression(variable("a1"), matchesVariables("a1", "b1")), number(1));

        // All scope equalities should utilize the constant if possible
        EqualityInference.EqualityPartition equalityPartition = inference.generateEqualitiesPartitionedBy(matchesVariables("a1", "b1"));
        assertEquals(equalitiesAsSets(equalityPartition.getScopeEqualities()),
                set(set(variable("a1"), number(1)), set(variable("b1"), number(1))));
        assertEquals(equalitiesAsSets(equalityPartition.getScopeComplementEqualities()),
                set(set(variable("c1"), number(1))));

        // There should be no scope straddling equalities as the full set of equalities should be already represented by the scope and inverse scope
        assertTrue(equalityPartition.getScopeStraddlingEqualities().isEmpty());
    }

    @Test
    public void testEqualityGeneration()
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        builder.addEquality(variable("a1"), add("b", "c")); // a1 = b + c
        builder.addEquality(variable("e1"), add("b", "d")); // e1 = b + d
        addEquality("c", "d", builder);
        EqualityInference inference = builder.build();

        RowExpression scopedCanonical = inference.getScopedCanonical(variable("e1"), variableBeginsWith("a"));
        assertEquals(scopedCanonical, variable("a1"));
    }

    @Test(dataProvider = "testRowExpressions")
    public void testExpressionsThatMayReturnNullOnNonNullInput(RowExpression candidate)
    {
        EqualityInference.Builder builder = new EqualityInference.Builder(METADATA);
        builder.extractInferenceCandidates(equals(variable("b"), variable("x")));
        builder.extractInferenceCandidates(equals(variable("a"), candidate));

        EqualityInference inference = builder.build();
        List<RowExpression> equalities = inference.generateEqualitiesPartitionedBy(matchesVariables("b")).getScopeStraddlingEqualities();
        assertEquals(equalities.size(), 1);
        assertTrue(equalities.get(0).equals(equals(variable("x"), variable("b"))) || equalities.get(0).equals(equals(variable("b"), variable("x"))));
    }

    @DataProvider(name = "testRowExpressions")
    public Object[][] toRowExpressionProvider()
    {
        return new Object[][] {
                {ROW_EXPRESSION_TRANSLATOR.translate("try_cast(b AS BIGINT)", ImmutableMap.of("b", VARCHAR))},
                {ROW_EXPRESSION_TRANSLATOR.translate("\"$internal$try\"(() -> b)", ImmutableMap.of("b", BIGINT))},
                {ROW_EXPRESSION_TRANSLATOR.translate("nullif(b, 1)", ImmutableMap.of("b", BIGINT))},
                {ROW_EXPRESSION_TRANSLATOR.translate("if(b = 1, 1)", ImmutableMap.of("b", BIGINT))},
                {ROW_EXPRESSION_TRANSLATOR.translate("b.x", ImmutableMap.of("b", RowType.from(ImmutableList.of(field("x", BIGINT)))))},
                {ROW_EXPRESSION_TRANSLATOR.translate("IF(b in (NULL), b)", ImmutableMap.of("b", BIGINT))},
                {ROW_EXPRESSION_TRANSLATOR.translate("case b when 1 then 1 END", ImmutableMap.of("b", BIGINT))}, //simple case
                {ROW_EXPRESSION_TRANSLATOR.translate("case when b is not NULL then 1 END", ImmutableMap.of("b", BIGINT))}, //search case
                {ROW_EXPRESSION_TRANSLATOR.translate("ARRAY [NULL][b]", ImmutableMap.of("b", BIGINT))}};
    }

    private static Predicate<RowExpression> matchesVariableScope(final Predicate<VariableReferenceExpression> variableScope)
    {
        return expression -> Iterables.all(VariablesExtractor.extractUnique(expression), variableScope);
    }

    private static Predicate<RowExpression> matchesStraddlingScope(final Predicate<VariableReferenceExpression> variableScope)
    {
        return expression -> {
            Set<VariableReferenceExpression> variables = VariablesExtractor.extractUnique(expression);
            return Iterables.any(variables, variableScope) && Iterables.any(variables, not(variableScope));
        };
    }

    private static void addEquality(String symbol1, String symbol2, EqualityInference.Builder builder)
    {
        builder.addEquality(variable(symbol1), variable(symbol2));
    }

    private static RowExpression someExpression(String symbol1, String symbol2)
    {
        return someExpression(variable(symbol1), variable(symbol2));
    }

    private static RowExpression someExpression(RowExpression expression1, RowExpression expression2)
    {
        return compare(OperatorType.GREATER_THAN, expression1, expression2);
    }

    private static RowExpression add(String symbol1, String symbol2)
    {
        return arithmeticOperation(ADD, variable(symbol1), variable(symbol2));
    }

    private static RowExpression add(RowExpression expression1, RowExpression expression2)
    {
        return arithmeticOperation(ADD, expression1, expression2);
    }

    private static RowExpression multiply(String symbol1, String symbol2)
    {
        return arithmeticOperation(MULTIPLY, variable(symbol1), variable(symbol2));
    }

    private static RowExpression multiply(RowExpression expression1, RowExpression expression2)
    {
        return arithmeticOperation(MULTIPLY, expression1, expression2);
    }

    private static RowExpression equals(String symbol1, String symbol2)
    {
        return compare(OperatorType.EQUAL, variable(symbol1), variable(symbol2));
    }

    private static RowExpression equals(RowExpression expression1, RowExpression expression2)
    {
        return compare(OperatorType.EQUAL, expression1, expression2);
    }

    private static VariableReferenceExpression variable(String name)
    {
        return Expressions.variable(name, BIGINT);
    }

    private static RowExpression number(long number)
    {
        return constant(number, BIGINT);
    }

    private static Predicate<VariableReferenceExpression> matchesVariables(String... variables)
    {
        return matchesVariables(Arrays.asList(variables));
    }

    private static Predicate<VariableReferenceExpression> matchesVariables(Collection<String> variables)
    {
        final Set<VariableReferenceExpression> symbolSet = variables.stream()
                .map(name -> new VariableReferenceExpression(name, BIGINT))
                .collect(toImmutableSet());

        return Predicates.in(symbolSet);
    }

    private static Predicate<VariableReferenceExpression> variableBeginsWith(String... prefixes)
    {
        return variableBeginsWith(Arrays.asList(prefixes));
    }

    private static Predicate<VariableReferenceExpression> variableBeginsWith(final Iterable<String> prefixes)
    {
        return variable -> {
            for (String prefix : prefixes) {
                if (variable.getName().startsWith(prefix)) {
                    return true;
                }
            }
            return false;
        };
    }

    private static Set<Set<RowExpression>> equalitiesAsSets(Iterable<RowExpression> expressions)
    {
        ImmutableSet.Builder<Set<RowExpression>> builder = ImmutableSet.builder();
        for (RowExpression expression : expressions) {
            builder.add(equalityAsSet(expression));
        }
        return builder.build();
    }

    private static Set<RowExpression> equalityAsSet(RowExpression expression)
    {
        Preconditions.checkArgument(isOperation(expression, OperatorType.EQUAL));
        return ImmutableSet.of(getLeft(expression), getRight(expression));
    }

    private static <E> Set<E> set(E... elements)
    {
        return setCopy(Arrays.asList(elements));
    }

    private static <E> Set<E> setCopy(Iterable<E> elements)
    {
        return ImmutableSet.copyOf(elements);
    }

    private static CallExpression compare(OperatorType type, RowExpression left, RowExpression right)
    {
        return call(
                type.getFunctionName().getObjectName(),
                METADATA.getFunctionAndTypeManager().resolveOperator(type, fromTypes(left.getType(), right.getType())),
                BOOLEAN,
                left,
                right);
    }

    private static RowExpression getLeft(RowExpression expression)
    {
        checkArgument(expression instanceof CallExpression && ((CallExpression) expression).getArguments().size() == 2, "must be binary call expression");
        return ((CallExpression) expression).getArguments().get(0);
    }

    private static RowExpression getRight(RowExpression expression)
    {
        checkArgument(expression instanceof CallExpression && ((CallExpression) expression).getArguments().size() == 2, "must be binary call expression");
        return ((CallExpression) expression).getArguments().get(1);
    }

    private static CallExpression arithmeticOperation(OperatorType type, RowExpression left, RowExpression right)
    {
        return call(
                type.getFunctionName().getObjectName(),
                METADATA.getFunctionAndTypeManager().resolveOperator(type, fromTypes(left.getType(), right.getType())),
                left.getType(),
                left,
                right);
    }

    private static boolean isOperation(RowExpression expression, OperatorType type)
    {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            Optional<OperatorType> expressionOperatorType = METADATA.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle()).getOperatorType();
            if (expressionOperatorType.isPresent()) {
                return expressionOperatorType.get() == type;
            }
        }
        return false;
    }
}
