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
import com.facebook.presto.expressions.RowExpressionNodeInliner;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.util.DisjointSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.uniqueSubExpressions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static java.util.Objects.requireNonNull;

public class EqualityInference
{
    // Ordering used to determine Expression preference when determining canonicals
    private static final Ordering<RowExpression> CANONICAL_ORDERING = Ordering.from((expression1, expression2) -> {
        // Current cost heuristic:
        // 1) Prefer fewer input symbols
        // 2) Prefer smaller expression trees
        // 3) Sort the expressions alphabetically - creates a stable consistent ordering (extremely useful for unit testing)
        // TODO: be more precise in determining the cost of an RowExpression
        return ComparisonChain.start()
                .compare(VariablesExtractor.extractAll(expression1).size(), VariablesExtractor.extractAll(expression2).size())
                .compare(uniqueSubExpressions(expression1).size(), uniqueSubExpressions(expression2).size())
                .compare(expression1.toString(), expression2.toString())
                .result();
    });

    private final SetMultimap<RowExpression, RowExpression> equalitySets; // Indexed by canonical RowExpression
    private final Map<RowExpression, RowExpression> canonicalMap; // Map each known RowExpression to canonical RowExpression
    private final Set<RowExpression> derivedExpressions;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    private final FunctionAndTypeManager functionAndTypeManager;

    private EqualityInference(
            Iterable<Set<RowExpression>> equalityGroups,
            Set<RowExpression> derivedExpressions,
            RowExpressionDeterminismEvaluator determinismEvaluator,
            FunctionAndTypeManager functionAndTypeManager)
    {
        this.determinismEvaluator = determinismEvaluator;
        this.functionAndTypeManager = functionAndTypeManager;
        ImmutableSetMultimap.Builder<RowExpression, RowExpression> setBuilder = ImmutableSetMultimap.builder();
        for (Set<RowExpression> equalityGroup : equalityGroups) {
            if (!equalityGroup.isEmpty()) {
                setBuilder.putAll(CANONICAL_ORDERING.min(equalityGroup), equalityGroup);
            }
        }
        equalitySets = setBuilder.build();

        ImmutableMap.Builder<RowExpression, RowExpression> mapBuilder = ImmutableMap.builder();
        for (Map.Entry<RowExpression, RowExpression> entry : equalitySets.entries()) {
            RowExpression canonical = entry.getKey();
            RowExpression expression = entry.getValue();
            mapBuilder.put(expression, canonical);
        }
        canonicalMap = mapBuilder.build();

        this.derivedExpressions = ImmutableSet.copyOf(derivedExpressions);
    }

    public static EqualityInference createEqualityInference(Metadata metadata, RowExpression... equalityInferences)
    {
        return new Builder(metadata)
                .addEqualityInference(equalityInferences)
                .build();
    }

    /**
     * Attempts to rewrite an RowExpression in terms of the symbols allowed by the symbol scope
     * given the known equalities. Returns null if unsuccessful.
     * This method checks if rewritten expression is non-deterministic.
     */
    public RowExpression rewriteExpression(RowExpression expression, Predicate<VariableReferenceExpression> variableScope)
    {
        checkArgument(determinismEvaluator.isDeterministic(expression), "Only deterministic expressions may be considered for rewrite");
        return rewriteExpression(expression, variableScope, true);
    }

    /**
     * Attempts to rewrite an Expression in terms of the symbols allowed by the symbol scope
     * given the known equalities. Returns null if unsuccessful.
     * This method allows rewriting non-deterministic expressions.
     */
    public RowExpression rewriteExpressionAllowNonDeterministic(RowExpression expression, Predicate<VariableReferenceExpression> variableScope)
    {
        return rewriteExpression(expression, variableScope, true);
    }

    private RowExpression rewriteExpression(RowExpression expression, Predicate<VariableReferenceExpression> variableScope, boolean allowFullReplacement)
    {
        Iterable<RowExpression> subExpressions = uniqueSubExpressions(expression);
        if (!allowFullReplacement) {
            subExpressions = filter(subExpressions, not(equalTo(expression)));
        }

        ImmutableMap.Builder<RowExpression, RowExpression> expressionRemap = ImmutableMap.builder();
        for (RowExpression subExpression : subExpressions) {
            RowExpression canonical = getScopedCanonical(subExpression, variableScope);
            if (canonical != null) {
                expressionRemap.put(subExpression, canonical);
            }
        }

        // Perform a naive single-pass traversal to try to rewrite non-compliant portions of the tree. Prefers to replace
        // larger subtrees over smaller subtrees
        // TODO: this rewrite can probably be made more sophisticated
        RowExpression rewritten = RowExpressionTreeRewriter.rewriteWith(new RowExpressionNodeInliner(expressionRemap.build()), expression);
        if (!variableToExpressionPredicate(variableScope).apply(rewritten)) {
            // If the rewritten is still not compliant with the symbol scope, just give up
            return null;
        }
        return rewritten;
    }

    /**
     * Dumps the inference equalities as equality expressions that are partitioned by the variableScope.
     * All stored equalities are returned in a compact set and will be classified into three groups as determined by the symbol scope:
     * <ol>
     * <li>equalities that fit entirely within the symbol scope</li>
     * <li>equalities that fit entirely outside of the symbol scope</li>
     * <li>equalities that straddle the symbol scope</li>
     * </ol>
     * <pre>
     * Example:
     *   Stored Equalities:
     *     a = b = c
     *     d = e = f = g
     *
     *   Symbol Scope:
     *     a, b, d, e
     *
     *   Output EqualityPartition:
     *     Scope Equalities:
     *       a = b
     *       d = e
     *     Complement Scope Equalities
     *       f = g
     *     Scope Straddling Equalities
     *       a = c
     *       d = f
     * </pre>
     */
    public EqualityPartition generateEqualitiesPartitionedBy(Predicate<VariableReferenceExpression> variableScope)
    {
        ImmutableSet.Builder<RowExpression> scopeEqualities = ImmutableSet.builder();
        ImmutableSet.Builder<RowExpression> scopeComplementEqualities = ImmutableSet.builder();
        ImmutableSet.Builder<RowExpression> scopeStraddlingEqualities = ImmutableSet.builder();

        for (Collection<RowExpression> equalitySet : equalitySets.asMap().values()) {
            Set<RowExpression> scopeExpressions = new LinkedHashSet<>();
            Set<RowExpression> scopeComplementExpressions = new LinkedHashSet<>();
            Set<RowExpression> scopeStraddlingExpressions = new LinkedHashSet<>();

            // Try to push each non-derived expression into one side of the scope
            for (RowExpression expression : filter(equalitySet, not(derivedExpressions::contains))) {
                RowExpression scopeRewritten = rewriteExpression(expression, variableScope, false);
                if (scopeRewritten != null) {
                    scopeExpressions.add(scopeRewritten);
                }
                RowExpression scopeComplementRewritten = rewriteExpression(expression, not(variableScope), false);
                if (scopeComplementRewritten != null) {
                    scopeComplementExpressions.add(scopeComplementRewritten);
                }
                if (scopeRewritten == null && scopeComplementRewritten == null) {
                    scopeStraddlingExpressions.add(expression);
                }
            }
            // Compile the equality expressions on each side of the scope
            RowExpression matchingCanonical = getCanonical(scopeExpressions);
            if (scopeExpressions.size() >= 2) {
                for (RowExpression expression : filter(scopeExpressions, not(equalTo(matchingCanonical)))) {
                    scopeEqualities.add(buildEqualsExpression(functionAndTypeManager, matchingCanonical, expression));
                }
            }
            RowExpression complementCanonical = getCanonical(scopeComplementExpressions);
            if (scopeComplementExpressions.size() >= 2) {
                for (RowExpression expression : filter(scopeComplementExpressions, not(equalTo(complementCanonical)))) {
                    scopeComplementEqualities.add(buildEqualsExpression(functionAndTypeManager, complementCanonical, expression));
                }
            }

            // Compile the scope straddling equality expressions
            List<RowExpression> connectingExpressions = new ArrayList<>();
            connectingExpressions.add(matchingCanonical);
            connectingExpressions.add(complementCanonical);
            connectingExpressions.addAll(scopeStraddlingExpressions);
            connectingExpressions = ImmutableList.copyOf(filter(connectingExpressions, Predicates.notNull()));
            RowExpression connectingCanonical = getCanonical(connectingExpressions);
            if (connectingCanonical != null) {
                for (RowExpression expression : filter(connectingExpressions, not(equalTo(connectingCanonical)))) {
                    scopeStraddlingEqualities.add(buildEqualsExpression(functionAndTypeManager, connectingCanonical, expression));
                }
            }
        }

        return new EqualityPartition(scopeEqualities.build(), scopeComplementEqualities.build(), scopeStraddlingEqualities.build());
    }

    /**
     * Returns the most preferrable expression to be used as the canonical expression
     */
    private static RowExpression getCanonical(Iterable<RowExpression> expressions)
    {
        if (Iterables.isEmpty(expressions)) {
            return null;
        }
        return CANONICAL_ORDERING.min(expressions);
    }

    /**
     * Returns a canonical expression that is fully contained by the variableScope and that is equivalent
     * to the specified expression. Returns null if unable to to find a canonical.
     */
    @VisibleForTesting
    RowExpression getScopedCanonical(RowExpression expression, Predicate<VariableReferenceExpression> variableScope)
    {
        RowExpression canonicalIndex = canonicalMap.get(expression);
        if (canonicalIndex == null) {
            return null;
        }
        return getCanonical(filter(equalitySets.get(canonicalIndex), variableToExpressionPredicate(variableScope)));
    }

    private static Predicate<RowExpression> variableToExpressionPredicate(final Predicate<VariableReferenceExpression> variableScope)
    {
        return expression -> Iterables.all(VariablesExtractor.extractUnique(expression), variableScope);
    }

    public static class EqualityPartition
    {
        private final List<RowExpression> scopeEqualities;
        private final List<RowExpression> scopeComplementEqualities;
        private final List<RowExpression> scopeStraddlingEqualities;

        public EqualityPartition(Iterable<RowExpression> scopeEqualities, Iterable<RowExpression> scopeComplementEqualities, Iterable<RowExpression> scopeStraddlingEqualities)
        {
            this.scopeEqualities = ImmutableList.copyOf(requireNonNull(scopeEqualities, "scopeEqualities is null"));
            this.scopeComplementEqualities = ImmutableList.copyOf(requireNonNull(scopeComplementEqualities, "scopeComplementEqualities is null"));
            this.scopeStraddlingEqualities = ImmutableList.copyOf(requireNonNull(scopeStraddlingEqualities, "scopeStraddlingEqualities is null"));
        }

        public List<RowExpression> getScopeEqualities()
        {
            return scopeEqualities;
        }

        public List<RowExpression> getScopeComplementEqualities()
        {
            return scopeComplementEqualities;
        }

        public List<RowExpression> getScopeStraddlingEqualities()
        {
            return scopeStraddlingEqualities;
        }
    }

    public static class Builder
    {
        private final DisjointSet<RowExpression> equalities = new DisjointSet<>();
        private final Set<RowExpression> derivedExpressions = new LinkedHashSet<>();
        private final FunctionAndTypeManager functionAndTypeManager;
        private final NullabilityAnalyzer nullabilityAnalyzer;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;

        public Builder(FunctionAndTypeManager functionAndTypeManager)
        {
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
            this.functionAndTypeManager = functionAndTypeManager;
            this.nullabilityAnalyzer = new NullabilityAnalyzer(functionAndTypeManager);
        }

        public Builder(Metadata metadata)
        {
            this(metadata.getFunctionAndTypeManager());
        }

        /**
         * Determines whether an RowExpression may be successfully applied to the equality inference
         */
        public Predicate<RowExpression> isInferenceCandidate()
        {
            return expression -> {
                expression = normalizeInPredicateToEquality(expression);
                if (isOperation(expression, EQUAL) &&
                        determinismEvaluator.isDeterministic(expression) &&
                        !nullabilityAnalyzer.mayReturnNullOnNonNullInput(expression)) {
                    // We should only consider equalities that have distinct left and right components
                    return !getLeft(expression).equals(getRight(expression));
                }
                return false;
            };
        }

        public static Predicate<RowExpression> isInferenceCandidate(Metadata metadata)
        {
            return new Builder(metadata).isInferenceCandidate();
        }

        /**
         * Rewrite single value InPredicates as equality if possible
         */
        private RowExpression normalizeInPredicateToEquality(RowExpression expression)
        {
            if (isInPredicate(expression)) {
                int size = ((SpecialFormExpression) expression).getArguments().size() - 1;
                checkArgument(size >= 1, "InList cannot be empty");
                if (size == 1) {
                    RowExpression leftValue = ((SpecialFormExpression) expression).getArguments().get(0);
                    RowExpression rightValue = ((SpecialFormExpression) expression).getArguments().get(1);
                    return buildEqualsExpression(functionAndTypeManager, leftValue, rightValue);
                }
            }
            return expression;
        }

        /**
         * Provides a convenience Iterable of RowExpression conjuncts which have not been added to the inference
         */
        public Iterable<RowExpression> nonInferrableConjuncts(RowExpression expression)
        {
            return filter(extractConjuncts(expression), not(isInferenceCandidate()));
        }

        public static Iterable<RowExpression> nonInferrableConjuncts(Metadata metadata, RowExpression expression)
        {
            return new Builder(metadata).nonInferrableConjuncts(expression);
        }

        public Builder addEqualityInference(RowExpression... expressions)
        {
            for (RowExpression expression : expressions) {
                extractInferenceCandidates(expression);
            }
            return this;
        }

        public Builder extractInferenceCandidates(RowExpression expression)
        {
            return addAllEqualities(filter(extractConjuncts(expression), isInferenceCandidate()));
        }

        public EqualityInference.Builder addAllEqualities(Iterable<RowExpression> expressions)
        {
            for (RowExpression expression : expressions) {
                addEquality(expression);
            }
            return this;
        }

        public EqualityInference.Builder addEquality(RowExpression expression)
        {
            expression = normalizeInPredicateToEquality(expression);
            checkArgument(isInferenceCandidate().apply(expression), "RowExpression must be a simple equality: " + expression);
            addEquality(getLeft(expression), getRight(expression));
            return this;
        }

        public EqualityInference.Builder addEquality(RowExpression expression1, RowExpression expression2)
        {
            checkArgument(!expression1.equals(expression2), "Need to provide equality between different expressions");
            checkArgument(determinismEvaluator.isDeterministic(expression1), "RowExpression must be deterministic: " + expression1);
            checkArgument(determinismEvaluator.isDeterministic(expression2), "RowExpression must be deterministic: " + expression2);

            equalities.findAndUnion(expression1, expression2);
            return this;
        }

        /**
         * Performs one pass of generating more equivalences by rewriting sub-expressions in terms of known equivalences.
         */
        private void generateMoreEquivalences()
        {
            Collection<Set<RowExpression>> equivalentClasses = equalities.getEquivalentClasses();

            // Map every expression to the set of equivalent expressions
            ImmutableMap.Builder<RowExpression, Set<RowExpression>> mapBuilder = ImmutableMap.builder();
            for (Set<RowExpression> expressions : equivalentClasses) {
                expressions.forEach(expression -> mapBuilder.put(expression, expressions));
            }

            // For every non-derived expression, extract the sub-expressions and see if they can be rewritten as other expressions. If so,
            // use this new information to update the known equalities.
            Map<RowExpression, Set<RowExpression>> map = mapBuilder.build();
            for (RowExpression expression : map.keySet()) {
                if (!derivedExpressions.contains(expression)) {
                    for (RowExpression subExpression : filter(uniqueSubExpressions(expression), not(equalTo(expression)))) {
                        Set<RowExpression> equivalentSubExpressions = map.get(subExpression);
                        if (equivalentSubExpressions != null) {
                            for (RowExpression equivalentSubExpression : filter(equivalentSubExpressions, not(equalTo(subExpression)))) {
                                RowExpression rewritten = RowExpressionTreeRewriter.rewriteWith(new RowExpressionNodeInliner(ImmutableMap.of(subExpression, equivalentSubExpression)), expression);
                                equalities.findAndUnion(expression, rewritten);
                                derivedExpressions.add(rewritten);
                            }
                        }
                    }
                }
            }
        }

        public EqualityInference build()
        {
            generateMoreEquivalences();
            return new EqualityInference(equalities.getEquivalentClasses(), derivedExpressions, determinismEvaluator, functionAndTypeManager);
        }

        private boolean isOperation(RowExpression expression, OperatorType type)
        {
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                Optional<OperatorType> expressionOperatorType = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getOperatorType();
                if (expressionOperatorType.isPresent()) {
                    return expressionOperatorType.get() == type;
                }
            }
            return false;
        }
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

    private static boolean isInPredicate(RowExpression expression)
    {
        if (expression instanceof SpecialFormExpression) {
            return ((SpecialFormExpression) expression).getForm() == SpecialFormExpression.Form.IN;
        }
        return false;
    }

    private static CallExpression buildEqualsExpression(FunctionAndTypeManager functionAndTypeManager, RowExpression left, RowExpression right)
    {
        return binaryOperation(functionAndTypeManager, EQUAL, left, right);
    }

    private static CallExpression binaryOperation(FunctionAndTypeManager functionAndTypeManager, OperatorType type, RowExpression left, RowExpression right)
    {
        return call(
                type.getFunctionName().getObjectName(),
                functionAndTypeManager.resolveOperator(type, fromTypes(left.getType(), right.getType())),
                BOOLEAN,
                left,
                right);
    }
}
