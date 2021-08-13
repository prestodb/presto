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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.AssignmentUtils;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.util.AstUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.Apply.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Replaces correlated ApplyNode with InPredicate expression with SemiJoin
 * <p>
 * Transforms:
 * <pre>
 * - Apply (output: a in B.b)
 *    - input: some plan A producing symbol a
 *    - subquery: some plan B producing symbol b, using symbols from A
 * </pre>
 * Into:
 * <pre>
 * - Project (output: CASE WHEN (countmatches > 0) THEN true WHEN (countnullmatches > 0) THEN null ELSE false END)
 *   - Aggregate (countmatches=count(*) where a, b not null; countnullmatches where a,b null but buildSideKnownNonNull is not null)
 *     grouping by (A'.*)
 *     - LeftJoin on (A and B correlation condition)
 *       - AssignUniqueId (A')
 *         - A
 * </pre>
 * <p>
 *
 * @see TransformCorrelatedScalarAggregationToJoin
 */
public class TransformCorrelatedInPredicateToJoin
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .with(nonEmpty(correlation()));

    private final StandardFunctionResolution functionResolution;

    public TransformCorrelatedInPredicateToJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager);
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode apply, Captures captures, Context context)
    {
        Assignments subqueryAssignments = apply.getSubqueryAssignments();
        if (subqueryAssignments.size() != 1) {
            return Result.empty();
        }
        Expression assignmentExpression = castToExpression(getOnlyElement(subqueryAssignments.getExpressions()));
        if (!(assignmentExpression instanceof InPredicate)) {
            return Result.empty();
        }

        InPredicate inPredicate = (InPredicate) assignmentExpression;
        VariableReferenceExpression inPredicateOutputVariable = getOnlyElement(subqueryAssignments.getVariables());

        return apply(apply, inPredicate, inPredicateOutputVariable, context.getLookup(), context.getIdAllocator(), context.getVariableAllocator());
    }

    private Result apply(
            ApplyNode apply,
            InPredicate inPredicate,
            VariableReferenceExpression inPredicateOutputVariable,
            Lookup lookup,
            PlanNodeIdAllocator idAllocator,
            PlanVariableAllocator variableAllocator)
    {
        Optional<Decorrelated> decorrelated = new DecorrelatingVisitor(lookup, apply.getCorrelation(), variableAllocator.getTypes())
                .decorrelate(apply.getSubquery());

        if (!decorrelated.isPresent()) {
            return Result.empty();
        }

        PlanNode projection = buildInPredicateEquivalent(
                apply,
                inPredicate,
                inPredicateOutputVariable,
                decorrelated.get(),
                idAllocator,
                variableAllocator);

        return Result.ofPlanNode(projection);
    }

    private PlanNode buildInPredicateEquivalent(
            ApplyNode apply,
            InPredicate inPredicate,
            VariableReferenceExpression inPredicateOutputVariable,
            Decorrelated decorrelated,
            PlanNodeIdAllocator idAllocator,
            PlanVariableAllocator variableAllocator)
    {
        Expression correlationCondition = and(decorrelated.getCorrelatedPredicates());
        PlanNode decorrelatedBuildSource = decorrelated.getDecorrelatedNode();

        AssignUniqueId probeSide = new AssignUniqueId(
                idAllocator.getNextId(),
                apply.getInput(),
                variableAllocator.newVariable("unique", BIGINT));

        VariableReferenceExpression buildSideKnownNonNull = variableAllocator.newVariable("buildSideKnownNonNull", BIGINT);
        ProjectNode buildSide = new ProjectNode(
                idAllocator.getNextId(),
                decorrelatedBuildSource,
                Assignments.builder()
                        .putAll(identitiesAsSymbolReferences(decorrelatedBuildSource.getOutputVariables()))
                        .put(buildSideKnownNonNull, castToRowExpression(bigint(0)))
                        .build());

        checkArgument(inPredicate.getValue() instanceof SymbolReference, "Unexpected expression: %s", inPredicate.getValue());
        SymbolReference probeSideSymbolReference = (SymbolReference) inPredicate.getValue();
        checkArgument(inPredicate.getValueList() instanceof SymbolReference, "Unexpected expression: %s", inPredicate.getValueList());
        SymbolReference buildSideSymbolReference = (SymbolReference) inPredicate.getValueList();

        Expression joinExpression = and(
                or(
                        new IsNullPredicate(probeSideSymbolReference),
                        new ComparisonExpression(ComparisonExpression.Operator.EQUAL, probeSideSymbolReference, buildSideSymbolReference),
                        new IsNullPredicate(buildSideSymbolReference)),
                correlationCondition);

        JoinNode leftOuterJoin = leftOuterJoin(idAllocator, probeSide, buildSide, joinExpression);

        VariableReferenceExpression countMatchesVariable = variableAllocator.newVariable("countMatches", BIGINT);
        VariableReferenceExpression countNullMatchesVariable = variableAllocator.newVariable("countNullMatches", BIGINT);

        Expression matchCondition = and(
                new IsNotNullPredicate(probeSideSymbolReference),
                new IsNotNullPredicate(buildSideSymbolReference));

        Expression nullMatchCondition = and(
                new IsNotNullPredicate(new SymbolReference(buildSideKnownNonNull.getName())),
                new NotExpression(matchCondition));

        AggregationNode aggregation = new AggregationNode(
                idAllocator.getNextId(),
                leftOuterJoin,
                ImmutableMap.<VariableReferenceExpression, AggregationNode.Aggregation>builder()
                        .put(countMatchesVariable, countWithFilter(matchCondition))
                        .put(countNullMatchesVariable, countWithFilter(nullMatchCondition))
                        .build(),
                singleGroupingSet(probeSide.getOutputVariables()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        // TODO since we care only about "some count > 0", we could have specialized node instead of leftOuterJoin that does the job without materializing join results
        SearchedCaseExpression inPredicateEquivalent = new SearchedCaseExpression(
                ImmutableList.of(
                        new WhenClause(isGreaterThan(countMatchesVariable, 0), booleanConstant(true)),
                        new WhenClause(isGreaterThan(countNullMatchesVariable, 0), booleanConstant(null))),
                Optional.of(booleanConstant(false)));
        return new ProjectNode(
                idAllocator.getNextId(),
                aggregation,
                Assignments.builder()
                        .putAll(identitiesAsSymbolReferences(apply.getInput().getOutputVariables()))
                        .put(inPredicateOutputVariable, castToRowExpression(inPredicateEquivalent))
                        .build());
    }

    private static JoinNode leftOuterJoin(PlanNodeIdAllocator idAllocator, AssignUniqueId probeSide, ProjectNode buildSide, Expression joinExpression)
    {
        return new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.LEFT,
                probeSide,
                buildSide,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(probeSide.getOutputVariables())
                        .addAll(buildSide.getOutputVariables())
                        .build(),
                Optional.of(castToRowExpression(joinExpression)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private AggregationNode.Aggregation countWithFilter(Expression condition)
    {
        return new AggregationNode.Aggregation(
                new CallExpression(
                        "count",
                        functionResolution.countFunction(),
                        BIGINT,
                        ImmutableList.of()),
                Optional.of(castToRowExpression(condition)),
                Optional.empty(),
                false,
                Optional.empty()); /* mask */
    }

    private static Expression isGreaterThan(VariableReferenceExpression variable, long value)
    {
        return new ComparisonExpression(
                ComparisonExpression.Operator.GREATER_THAN,
                new SymbolReference(variable.getName()),
                bigint(value));
    }

    private static Expression bigint(long value)
    {
        return new Cast(new LongLiteral(String.valueOf(value)), BIGINT.toString());
    }

    private static Expression booleanConstant(@Nullable Boolean value)
    {
        if (value == null) {
            return new Cast(new NullLiteral(), BOOLEAN.toString());
        }
        return new BooleanLiteral(value.toString());
    }

    private static class DecorrelatingVisitor
            extends InternalPlanVisitor<Optional<Decorrelated>, PlanNode>
    {
        private final Lookup lookup;
        private final Set<VariableReferenceExpression> correlation;
        private final TypeProvider types;

        public DecorrelatingVisitor(Lookup lookup, Iterable<VariableReferenceExpression> correlation, TypeProvider types)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.correlation = ImmutableSet.copyOf(requireNonNull(correlation, "correlation is null"));
            this.types = requireNonNull(types, "types is null");
        }

        public Optional<Decorrelated> decorrelate(PlanNode reference)
        {
            return lookup.resolve(reference).accept(this, reference);
        }

        @Override
        public Optional<Decorrelated> visitProject(ProjectNode node, PlanNode reference)
        {
            if (isCorrelatedShallowly(node)) {
                // TODO: handle correlated projection
                return Optional.empty();
            }

            Optional<Decorrelated> result = decorrelate(node.getSource());
            return result.map(decorrelated -> {
                Assignments.Builder assignments = Assignments.builder()
                        .putAll(node.getAssignments());

                // Pull up all symbols used by a filter (except correlation)
                decorrelated.getCorrelatedPredicates().stream()
                        .flatMap(AstUtils::preOrder)
                        .filter(SymbolReference.class::isInstance)
                        .map(SymbolReference.class::cast)
                        .map(symbolReference -> new VariableReferenceExpression(symbolReference.getName(), types.get(symbolReference)))
                        .filter(variable -> !correlation.contains(variable))
                        .map(AssignmentUtils::identityAsSymbolReference)
                        .forEach(assignments::put);

                return new Decorrelated(
                        decorrelated.getCorrelatedPredicates(),
                        new ProjectNode(
                                node.getId(), // FIXME should I reuse or not?
                                decorrelated.getDecorrelatedNode(),
                                assignments.build()));
            });
        }

        @Override
        public Optional<Decorrelated> visitFilter(FilterNode node, PlanNode reference)
        {
            Optional<Decorrelated> result = decorrelate(node.getSource());
            return result.map(decorrelated ->
                    new Decorrelated(
                            ImmutableList.<Expression>builder()
                                    .addAll(decorrelated.getCorrelatedPredicates())
                                    // No need to retain uncorrelated conditions, predicate push down will push them back
                                    .add(castToExpression(node.getPredicate()))
                                    .build(),
                            decorrelated.getDecorrelatedNode()));
        }

        @Override
        public Optional<Decorrelated> visitPlan(PlanNode node, PlanNode reference)
        {
            if (isCorrelatedRecursively(node)) {
                return Optional.empty();
            }
            else {
                return Optional.of(new Decorrelated(ImmutableList.of(), reference));
            }
        }

        private boolean isCorrelatedRecursively(PlanNode node)
        {
            if (isCorrelatedShallowly(node)) {
                return true;
            }
            return node.getSources().stream()
                    .map(lookup::resolve)
                    .anyMatch(this::isCorrelatedRecursively);
        }

        private boolean isCorrelatedShallowly(PlanNode node)
        {
            return VariablesExtractor.extractUniqueNonRecursive(node, types).stream().anyMatch(correlation::contains);
        }
    }

    private static class Decorrelated
    {
        private final List<Expression> correlatedPredicates;
        private final PlanNode decorrelatedNode;

        public Decorrelated(List<Expression> correlatedPredicates, PlanNode decorrelatedNode)
        {
            this.correlatedPredicates = ImmutableList.copyOf(requireNonNull(correlatedPredicates, "correlatedPredicates is null"));
            this.decorrelatedNode = requireNonNull(decorrelatedNode, "decorrelatedNode is null");
        }

        public List<Expression> getCorrelatedPredicates()
        {
            return correlatedPredicates;
        }

        public PlanNode getDecorrelatedNode()
        {
            return decorrelatedNode;
        }
    }
}
