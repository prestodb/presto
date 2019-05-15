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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.LogicalRowExpressions;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Extracts sort expression to be used for creating {@link com.facebook.presto.operator.SortedPositionLinks} from join filter expression.
 * Currently this class can extract sort and search expressions from filter function conjuncts of shape:
 * <p>
 * {@code A.a < f(B.x, B.y, B.z)} or {@code f(B.x, B.y, B.z) < A.a}
 * <p>
 * where {@code a} is the build side symbol reference and {@code x,y,z} are probe
 * side symbol references. Any of inequality operators ({@code <,<=,>,>=}) can be used.
 * Same build side symbol need to be used in all conjuncts.
 */
public final class SortExpressionExtractor
{
    /* TODO:
       This class could be extended to handle any expressions like:
       A.a * sin(A.b) / log(B.x) < cos(B.z)
       by transforming it to:
       f(A.a, A.b) < g(B.x, B.z)
       Where f(...) and g(...) would be some functions/expressions. That
       would allow us to perform binary search on arbitrary complex expressions
       by sorting position links according to the result of f(...) function.
     */
    private SortExpressionExtractor() {}

    public static Optional<SortExpressionContext> extractSortExpression(Set<Symbol> buildSymbols, RowExpression filter, FunctionManager functionManager)
    {
        List<RowExpression> filterConjuncts = LogicalRowExpressions.extractConjuncts(filter);
        SortExpressionVisitor visitor = new SortExpressionVisitor(buildSymbols, functionManager);

        DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionManager);
        List<SortExpressionContext> sortExpressionCandidates = filterConjuncts.stream()
                .filter(determinismEvaluator::isDeterministic)
                .map(conjunct -> conjunct.accept(visitor, null))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toMap(SortExpressionContext::getSortExpression, identity(), SortExpressionExtractor::merge))
                .values()
                .stream()
                .collect(toImmutableList());

        // For now heuristically pick sort expression which has most search expressions assigned to it.
        // TODO: make it cost based decision based on symbol statistics
        return sortExpressionCandidates.stream()
                .sorted(comparing(context -> -1 * context.getSearchExpressions().size()))
                .findFirst();
    }

    private static SortExpressionContext merge(SortExpressionContext left, SortExpressionContext right)
    {
        checkArgument(left.getSortExpression().equals(right.getSortExpression()));
        ImmutableList.Builder<RowExpression> searchExpressions = ImmutableList.builder();
        searchExpressions.addAll(left.getSearchExpressions());
        searchExpressions.addAll(right.getSearchExpressions());
        return new SortExpressionContext(left.getSortExpression(), searchExpressions.build());
    }

    private static class SortExpressionVisitor
            implements RowExpressionVisitor<Optional<SortExpressionContext>, Void>
    {
        private final Set<Symbol> buildSymbols;
        private final FunctionManager functionManager;

        public SortExpressionVisitor(Set<Symbol> buildSymbols, FunctionManager functionManager)
        {
            this.buildSymbols = buildSymbols;
            this.functionManager = functionManager;
        }

        @Override
        public Optional<SortExpressionContext> visitCall(CallExpression call, Void context)
        {
            FunctionMetadata functionMetadata = functionManager.getFunctionMetadata(call.getFunctionHandle());
            if (!functionMetadata.getOperatorType().map(OperatorType::isComparisonOperator).orElse(false)) {
                return Optional.empty();
            }

            switch (functionMetadata.getOperatorType().get()) {
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    RowExpression left = call.getArguments().get(0);
                    RowExpression right = call.getArguments().get(1);
                    Optional<VariableReferenceExpression> sortChannel = asBuildVariableReference(buildSymbols, right);
                    boolean hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, left);
                    if (!sortChannel.isPresent()) {
                        sortChannel = asBuildVariableReference(buildSymbols, left);
                        hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, right);
                    }
                    if (sortChannel.isPresent() && !hasBuildReferencesOnOtherSide) {
                        return sortChannel.map(variableReference -> new SortExpressionContext(variableReference, singletonList(call)));
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        }

        @Override
        public Optional<SortExpressionContext> visitInputReference(InputReferenceExpression input, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<SortExpressionContext> visitConstant(ConstantExpression literal, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<SortExpressionContext> visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<SortExpressionContext> visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<SortExpressionContext> visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return Optional.empty();
        }
    }

    private static Optional<VariableReferenceExpression> asBuildVariableReference(Set<Symbol> buildLayout, RowExpression expression)
    {
        // Currently only we support only symbol as sort expression on build side
        if (expression instanceof VariableReferenceExpression) {
            VariableReferenceExpression reference = (VariableReferenceExpression) expression;
            if (buildLayout.contains(new Symbol(reference.getName()))) {
                return Optional.of(reference);
            }
        }
        return Optional.empty();
    }

    private static boolean hasBuildSymbolReference(Set<Symbol> buildSymbols, RowExpression expression)
    {
        return expression.accept(new BuildSymbolReferenceFinder(buildSymbols), null);
    }

    private static class BuildSymbolReferenceFinder
            implements RowExpressionVisitor<Boolean, Void>
    {
        private final Set<String> buildSymbols;

        public BuildSymbolReferenceFinder(Set<Symbol> buildSymbols)
        {
            this.buildSymbols = requireNonNull(buildSymbols, "buildSymbols is null").stream()
                    .map(Symbol::getName)
                    .collect(toImmutableSet());
        }

        @Override
        public Boolean visitInputReference(InputReferenceExpression input, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitCall(CallExpression call, Void context)
        {
            for (RowExpression argument : call.getArguments()) {
                if (argument.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitConstant(ConstantExpression literal, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return lambda.getBody().accept(this, context);
        }

        @Override
        public Boolean visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return buildSymbols.contains(reference.getName());
        }

        @Override
        public Boolean visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            for (RowExpression argument : specialForm.getArguments()) {
                if (argument.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }
    }
}
