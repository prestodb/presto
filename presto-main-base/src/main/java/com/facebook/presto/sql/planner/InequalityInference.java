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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.EqualityInference.getLeft;
import static com.facebook.presto.sql.planner.EqualityInference.getRight;
import static com.facebook.presto.sql.planner.EqualityInference.isOperation;
import static com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence.swapPair;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.filter;
import static java.util.Objects.requireNonNull;

public class InequalityInference
{
    // inequalityExpressions include the inequalities from the current join predicate
    // and those inherited from the join's parent
    private final Set<RowExpression> inequalityExpressions;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final ExpressionEquivalence expressionEquivalence;
    // 'outerVariables' is the variable set projected from the outer input for left or right join,
    // or EMPTY if the join is an inner join
    private final Optional<Collection<VariableReferenceExpression>> outerVariables;

    public InequalityInference(Set<RowExpression> inequalityExpressions, FunctionAndTypeManager functionAndTypeManager, ExpressionEquivalence expressionEquivalence, Optional<Collection<VariableReferenceExpression>> outerVariables)
    {
        if (inequalityExpressions.stream()
                .anyMatch(e -> !isOperation(e, LESS_THAN, functionAndTypeManager) && !isOperation(e, LESS_THAN_OR_EQUAL, functionAndTypeManager))) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "all inequality expressions for inference must be < or <=");
        }
        this.inequalityExpressions = requireNonNull(inequalityExpressions, "inequalityExpressions is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.expressionEquivalence = requireNonNull(expressionEquivalence, "expressionEquivalence is null");
        this.outerVariables = requireNonNull(outerVariables, "outerVariables is null");
    }

    // Basic idea is to pairwise compare each predicate with all other predicates.
    // Since any inferred predicates may trigger other inferences, keep comparing until no new predicates are generated
    public Set<RowExpression> inferInequalities()
    {
        if (inequalityExpressions.size() < 2) {
            return ImmutableSet.of();
        }

        Set<RowExpression> allInferredInequalities = new HashSet<>();
        Set<RowExpression> inequalitiesInferredInCurrentTraversal = new HashSet<>(inequalityExpressions);
        Set<Set<RowExpression>> exploredCombinations = new HashSet();

        while (!allInferredInequalities.containsAll(inequalitiesInferredInCurrentTraversal)) {
            allInferredInequalities.addAll(inequalitiesInferredInCurrentTraversal);
            Set<Set<RowExpression>> newCombinations = Sets.combinations(allInferredInequalities, 2).stream()
                    .filter(subset -> !exploredCombinations.contains(subset))
                    .collect(toImmutableSet());

            inequalitiesInferredInCurrentTraversal = newCombinations.stream()
                    .map(pair -> {
                        Iterator<RowExpression> it = pair.iterator();
                        return compareAndExtractInequalities(it.next(), it.next());
                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableSet());

            exploredCombinations.addAll(newCombinations);
        }

        // we want to exclude all the original inequalities we passed in from the set we return
        allInferredInequalities.removeAll(inequalityExpressions);
        return ImmutableSet.copyOf(allInferredInequalities);
    }

    // If the join is an outer join, we need to make sure any inferred predicate is on the inner side only
    // (i.e. does not reference any 'outerVariables').
    private Optional<RowExpression> compareAndExtractInequalities(RowExpression expression1, RowExpression expression2)
    {
        CallExpression firstConjunct = (CallExpression) expression1;
        OperatorType firstOperatorType = functionAndTypeManager.getFunctionMetadata(firstConjunct.getFunctionHandle()).getOperatorType().get();

        CallExpression secondConjunct = (CallExpression) expression2;
        OperatorType secondOperatorType = functionAndTypeManager.getFunctionMetadata(secondConjunct.getFunctionHandle()).getOperatorType().get();

        // Get the inference operator, or empty if no inference possible.
        Optional<OperatorType> inferenceOperatorType = getComparisonInferenceOperatorType(secondOperatorType, firstOperatorType);
        if (!inferenceOperatorType.isPresent()) {
            return Optional.empty();
        }

        // For two inequalities "a < b" and "b < 5" we want to infer "a < 5"
        // Compare the call expressions' arguments to determine equivalence
        // Make sure one input has no column/variable references since we only want to infer predicates with constants
        // LHS : Left hand side
        // RHS : Right hand side
        RowExpression firstConjunctLHS = firstConjunct.getArguments().get(0);
        RowExpression firstConjunctRHS = firstConjunct.getArguments().get(1);
        RowExpression secondConjunctLHS = secondConjunct.getArguments().get(0);
        RowExpression secondConjunctRHS = secondConjunct.getArguments().get(1);

        Optional<RowExpression> inferredFirstArgument = Optional.empty();
        Optional<RowExpression> inferredSecondArgument = Optional.empty();
        Set<VariableReferenceExpression> variablesReferencedInInferredPredicate = ImmutableSet.of();
        if (expressionEquivalence.areExpressionsEquivalent(firstConjunctRHS, secondConjunctLHS)) {
            variablesReferencedInInferredPredicate = getVariablesReferencedInInferredPredicate(firstConjunctLHS, secondConjunctRHS);
            if (!variablesReferencedInInferredPredicate.isEmpty()) {
                inferredFirstArgument = Optional.of(firstConjunctLHS);
                inferredSecondArgument = Optional.of(secondConjunctRHS);
            }
        }
        else if (expressionEquivalence.areExpressionsEquivalent(firstConjunctLHS, secondConjunctRHS)) {
            variablesReferencedInInferredPredicate = getVariablesReferencedInInferredPredicate(firstConjunctRHS, secondConjunctLHS);
            if (!variablesReferencedInInferredPredicate.isEmpty()) {
                inferredFirstArgument = Optional.of(secondConjunctLHS);
                inferredSecondArgument = Optional.of(firstConjunctRHS);
            }
        }

        if (!inferredFirstArgument.isPresent() ||
                !inferredSecondArgument.isPresent() ||
                (outerVariables.isPresent() && Iterables.any(variablesReferencedInInferredPredicate, in(outerVariables.get())))) {
            return Optional.empty();
        }

        // Build and return the new predicate
        FunctionHandle inferredComparatorFunctionHandle = functionAndTypeManager.resolveOperator(inferenceOperatorType.get(), fromTypes(inferredFirstArgument.get().getType(), inferredSecondArgument.get().getType()));
        return Optional.of(new CallExpression(inferenceOperatorType.toString(), inferredComparatorFunctionHandle, BOOLEAN, ImmutableList.of(inferredFirstArgument.get(), inferredSecondArgument.get())));
    }

    // Get comparison operator for inferring comparison predicates given operators from
    // two comparison predicates. E.g., '1<=a1 AND a1<a2' will return '<' as the operator.
    // NULL is returned if no inference is possible.
    private Optional<OperatorType> getComparisonInferenceOperatorType(OperatorType operator1, OperatorType operator2)
    {
        Optional<OperatorType> inferenceType = Optional.empty();
        if (operator1.equals(LESS_THAN)) {
            if (operator2.equals(LESS_THAN) || operator2.equals(LESS_THAN_OR_EQUAL)) {
                inferenceType = Optional.of(LESS_THAN);
            }
        }
        else if (operator1.equals(LESS_THAN_OR_EQUAL)) {
            if (operator2.equals(LESS_THAN)) {
                inferenceType = Optional.of(LESS_THAN);
            }
            else if (operator2.equals(LESS_THAN_OR_EQUAL)) {
                inferenceType = Optional.of(LESS_THAN_OR_EQUAL);
            }
        }

        return inferenceType;
    }

    private static Set<VariableReferenceExpression> getVariablesReferencedInInferredPredicate(RowExpression firstConjunct, RowExpression secondConjunct)
    {
        Set<VariableReferenceExpression> firstConjunctReferencedVariables = VariablesExtractor.extractUnique(firstConjunct);
        if (firstConjunctReferencedVariables.isEmpty()) {
            return VariablesExtractor.extractUnique(secondConjunct);
        }
        Set<VariableReferenceExpression> secondConjunctReferencedVariables = VariablesExtractor.extractUnique(secondConjunct);
        if (secondConjunctReferencedVariables.isEmpty()) {
            return firstConjunctReferencedVariables;
        }
        // inferred predicates cannot reference variables from both expressions
        // return empty to indicate that there is no valid inferred predicate
        return ImmutableSet.of();
    }

    public static class Builder
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final NullabilityAnalyzer nullabilityAnalyzer;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;
        private final ExpressionEquivalence expressionEquivalence;
        private final Set<RowExpression> inequalityExpressions = new HashSet<>();
        private final Optional<Collection<VariableReferenceExpression>> outerVariables;

        public Builder(FunctionAndTypeManager functionAndTypeManager, ExpressionEquivalence expressionEquivalence, Optional<Collection<VariableReferenceExpression>> outerVariables)
        {
            this.functionAndTypeManager = functionAndTypeManager;
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
            this.expressionEquivalence = expressionEquivalence;
            this.nullabilityAnalyzer = new NullabilityAnalyzer(functionAndTypeManager);
            this.outerVariables = outerVariables;
        }

        public InequalityInference build()
        {
            return new InequalityInference(inequalityExpressions, functionAndTypeManager, expressionEquivalence, outerVariables);
        }

        public Builder addInequalityInferences(RowExpression... expressions)
        {
            for (RowExpression expression : expressions) {
                extractInequalityInferenceCandidates(expression);
            }
            return this;
        }

        private Builder extractInequalityInferenceCandidates(RowExpression expression)
        {
            Iterable<RowExpression> candidates = filter(extractConjuncts(expression), isInequalityInferenceCandidate());
            for (RowExpression conjunct : candidates) {
                addInequalityInferenceCandidate(conjunct);
            }
            return this;
        }

        private InequalityInference.Builder addInequalityInferenceCandidate(RowExpression expression)
        {
            checkArgument(isInequalityInferenceCandidate().apply(expression), "RowExpression: " + expression + " is not an inequality inference candidate");
            inequalityExpressions.add(canonicalizeInequality(expression));
            return this;
        }

        // Convert all inequalities to LESS_THAN or LESS_THAN_OR_EQUAL
        RowExpression canonicalizeInequality(RowExpression expression)
        {
            if (isOperation(expression, GREATER_THAN, functionAndTypeManager) ||
                    isOperation(expression, GREATER_THAN_OR_EQUAL, functionAndTypeManager)) {
                CallExpression callExpression = (CallExpression) expression;
                OperatorType operatorType = functionAndTypeManager.getFunctionMetadata(callExpression.getFunctionHandle()).getOperatorType().get();
                operatorType = OperatorType.flip(operatorType);
                FunctionHandle functionHandle = functionAndTypeManager.resolveOperator(operatorType, swapPair(fromTypes(callExpression.getArguments().stream().map(RowExpression::getType).collect(toImmutableList()))));
                expression = new CallExpression(operatorType.getOperator(), functionHandle, BOOLEAN, swapPair(callExpression.getArguments()));
            }
            return expression;
        }

        private Predicate<RowExpression> isInequalityInferenceCandidate()
        {
            return expression -> (isOperation(expression, GREATER_THAN_OR_EQUAL, functionAndTypeManager) ||
                    isOperation(expression, GREATER_THAN, functionAndTypeManager) ||
                    isOperation(expression, LESS_THAN_OR_EQUAL, functionAndTypeManager) ||
                    isOperation(expression, LESS_THAN, functionAndTypeManager)) &&
                    determinismEvaluator.isDeterministic(expression) &&
                    !nullabilityAnalyzer.mayReturnNullOnNonNullInput(expression) &&
                    !getLeft(expression).equals(getRight(expression));
        }
    }
}
