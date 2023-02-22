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
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InSubqueryRowExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
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
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.SuccessorsFunction;
import com.google.common.graph.Traverser;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.Apply.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.searchedCaseExpression;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
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

    private final FunctionResolution functionResolution;

    public TransformCorrelatedInPredicateToJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
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
        RowExpression assignmentExpression = getOnlyElement(subqueryAssignments.getExpressions());
        if (!(assignmentExpression instanceof InSubqueryRowExpression)) {
            return Result.empty();
        }
        InSubqueryRowExpression inPredicate = (InSubqueryRowExpression) assignmentExpression;
        VariableReferenceExpression inPredicateOutputVariable = getOnlyElement(subqueryAssignments.getVariables());

        return apply(apply, inPredicate, inPredicateOutputVariable, context.getLookup(), context.getIdAllocator(), context.getVariableAllocator());
    }

    private Result apply(
            ApplyNode apply,
            InSubqueryRowExpression inPredicate,
            VariableReferenceExpression inPredicateOutputVariable,
            Lookup lookup,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator)
    {
        Optional<Decorrelated> decorrelated = new DecorrelatingVisitor(lookup, apply.getCorrelation(), TypeProvider.viewOf(variableAllocator.getVariables()))
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
            InSubqueryRowExpression inPredicate,
            VariableReferenceExpression inPredicateOutputVariable,
            Decorrelated decorrelated,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator)
    {
        RowExpression correlationCondition = and(decorrelated.getCorrelatedPredicates());
        PlanNode decorrelatedBuildSource = decorrelated.getDecorrelatedNode();

        AssignUniqueId probeSide = new AssignUniqueId(
                apply.getSourceLocation(),
                idAllocator.getNextId(),
                apply.getInput(),
                variableAllocator.newVariable("unique", BIGINT));

        VariableReferenceExpression buildSideKnownNonNull = variableAllocator.newVariable(inPredicateOutputVariable.getSourceLocation(), "buildSideKnownNonNull", BIGINT);
        ProjectNode buildSide = new ProjectNode(
                idAllocator.getNextId(),
                decorrelatedBuildSource,
                Assignments.builder()
                        .putAll(identityAssignments(decorrelatedBuildSource.getOutputVariables()))
                        .put(buildSideKnownNonNull, constant(0L, BIGINT))
                        .build());

        VariableReferenceExpression probeSideSymbolReference = inPredicate.getValue();
        VariableReferenceExpression buildSideSymbolReference = inPredicate.getSubquery();

        RowExpression isProbeSideNull = specialForm(probeSideSymbolReference.getSourceLocation(), IS_NULL, BOOLEAN, probeSideSymbolReference);
        RowExpression isBuildSideNull = specialForm(buildSideSymbolReference.getSourceLocation(), IS_NULL, BOOLEAN, buildSideSymbolReference);
        RowExpression comparison = call(
                ComparisonExpression.Operator.EQUAL.name(),
                functionResolution.comparisonFunction(ComparisonExpression.Operator.EQUAL, probeSideSymbolReference.getType(), buildSideSymbolReference.getType()),
                BOOLEAN,
                probeSideSymbolReference,
                buildSideSymbolReference);

        RowExpression joinExpression = and(
                or(isProbeSideNull, comparison, isBuildSideNull),
                correlationCondition);

        JoinNode leftOuterJoin = leftOuterJoin(idAllocator, probeSide, buildSide, joinExpression);

        VariableReferenceExpression countMatchesVariable = variableAllocator.newVariable(buildSideSymbolReference.getSourceLocation(), "countMatches", BIGINT);
        VariableReferenceExpression countNullMatchesVariable = variableAllocator.newVariable(buildSideSymbolReference.getSourceLocation(), "countNullMatches", BIGINT);

        RowExpression matchCondition = and(
                isNotNull(probeSideSymbolReference),
                isNotNull(buildSideSymbolReference));

        RowExpression nullMatchCondition = and(
                isNotNull(buildSideKnownNonNull),
                not(matchCondition));

        AggregationNode aggregation = new AggregationNode(
                apply.getSourceLocation(),
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
        RowExpression inPredicateEquivalent = searchedCaseExpression(
                ImmutableList.of(
                        specialForm(WHEN, BOOLEAN, isGreaterThan(countMatchesVariable, 0), TRUE_CONSTANT),
                        specialForm(WHEN, BOOLEAN, isGreaterThan(countNullMatchesVariable, 0), new ConstantExpression(null, BOOLEAN))),
                Optional.of(FALSE_CONSTANT));
        return new ProjectNode(
                idAllocator.getNextId(),
                aggregation,
                Assignments.builder()
                        .putAll(identityAssignments(apply.getInput().getOutputVariables()))
                        .put(inPredicateOutputVariable, inPredicateEquivalent)
                        .build());
    }

    private RowExpression isNotNull(RowExpression expression)
    {
        return not(specialForm(IS_NULL, BOOLEAN, ImmutableList.of(expression)));
    }

    private RowExpression not(RowExpression expression)
    {
        return call(
                expression.getSourceLocation(),
                "not",
                functionResolution.notFunction(),
                BOOLEAN,
                expression);
    }

    private static JoinNode leftOuterJoin(PlanNodeIdAllocator idAllocator, AssignUniqueId probeSide, ProjectNode buildSide, RowExpression joinExpression)
    {
        return new JoinNode(
                probeSide.getSourceLocation(),
                idAllocator.getNextId(),
                JoinNode.Type.LEFT,
                probeSide,
                buildSide,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(probeSide.getOutputVariables())
                        .addAll(buildSide.getOutputVariables())
                        .build(),
                Optional.of(joinExpression),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private AggregationNode.Aggregation countWithFilter(RowExpression condition)
    {
        return new AggregationNode.Aggregation(
                new CallExpression(
                        condition.getSourceLocation(),
                        "count",
                        functionResolution.countFunction(),
                        BIGINT,
                        ImmutableList.of()),
                Optional.of(condition),
                Optional.empty(),
                false,
                Optional.empty()); /* mask */
    }

    private RowExpression isGreaterThan(VariableReferenceExpression variable, long value)
    {
        return call(
                ComparisonExpression.Operator.GREATER_THAN.name(),
                functionResolution.comparisonFunction(ComparisonExpression.Operator.GREATER_THAN, BIGINT, BIGINT),
                BOOLEAN,
                variable,
                constant(value, BIGINT));
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
                        .flatMap(expression -> stream(
                                Traverser.forTree((SuccessorsFunction<RowExpression>) RowExpression::getChildren)
                                        .depthFirstPreOrder(expression)))
                        .filter(VariableReferenceExpression.class::isInstance)
                        .map(VariableReferenceExpression.class::cast)
                        .filter(variable -> !correlation.contains(variable))
                        .map(AssignmentUtils::identityAssignments)
                        .forEach(assignments::putAll);

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
                            ImmutableList.<RowExpression>builder()
                                    .addAll(decorrelated.getCorrelatedPredicates())
                                    // No need to retain uncorrelated conditions, predicate push down will push them back
                                    .add(node.getPredicate())
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
        private final List<RowExpression> correlatedPredicates;
        private final PlanNode decorrelatedNode;

        public Decorrelated(List<RowExpression> correlatedPredicates, PlanNode decorrelatedNode)
        {
            this.correlatedPredicates = ImmutableList.copyOf(requireNonNull(correlatedPredicates, "correlatedPredicates is null"));
            this.decorrelatedNode = requireNonNull(decorrelatedNode, "decorrelatedNode is null");
        }

        public List<RowExpression> getCorrelatedPredicates()
        {
            return correlatedPredicates;
        }

        public PlanNode getDecorrelatedNode()
        {
            return decorrelatedNode;
        }
    }
}
