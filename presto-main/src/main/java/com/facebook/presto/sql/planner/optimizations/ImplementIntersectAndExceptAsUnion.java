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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SetOperationNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.spi.plan.AggregationNode.Step;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils.fromListMultimap;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Converts INTERSECT and EXCEPT queries into UNION ALL..GROUP BY...WHERE
 * Eg:  SELECT a FROM foo INTERSECT SELECT x FROM bar
 * <p/>
 * =>
 * <p/>
 * SELECT a
 * FROM
 * (SELECT a,
 * COUNT(foo_marker) AS foo_cnt,
 * COUNT(bar_marker) AS bar_cnt
 * FROM
 * (
 * SELECT a, true as foo_marker, null as bar_marker FROM foo
 * UNION ALL
 * SELECT x, null as foo_marker, true as bar_marker FROM bar
 * ) T1
 * GROUP BY a) T2
 * WHERE foo_cnt >= 1 AND bar_cnt >= 1;
 * <p>
 * Eg:  SELECT a FROM foo EXCEPT SELECT x FROM bar
 * <p/>
 * =>
 * <p/>
 * SELECT a
 * FROM
 * (SELECT a,
 * COUNT(foo_marker) AS foo_cnt,
 * COUNT(bar_marker) AS bar_cnt
 * FROM
 * (
 * SELECT a, true as foo_marker, null as bar_marker FROM foo
 * UNION ALL
 * SELECT x, null as foo_marker, true as bar_marker FROM bar
 * ) T1
 * GROUP BY a) T2
 * WHERE foo_cnt >= 1 AND bar_cnt = 0;
 */
public class ImplementIntersectAndExceptAsUnion
        implements PlanOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public ImplementIntersectAndExceptAsUnion(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(session, functionAndTypeManager, idAllocator, variableAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private static final String MARKER = "marker";

        private final Session session;
        private final StandardFunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;

        private Rewriter(Session session, FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            requireNonNull(functionAndTypeManager, "functionManager is null");
            this.session = requireNonNull(session, "session is null");
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<Void> rewriteContext)
        {
            List<PlanNode> sources = node.getSources().stream()
                    .map(rewriteContext::rewrite)
                    .collect(toList());

            List<VariableReferenceExpression> markers = allocateVariables(sources.size(), MARKER, BOOLEAN);

            // identity projection for all the fields in each of the sources plus marker columns
            List<PlanNode> withMarkers = appendMarkers(markers, sources, node);

            // add a union over all the rewritten sources. The outputs of the union have the same name as the
            // original intersect node
            List<VariableReferenceExpression> outputs = node.getOutputVariables();
            UnionNode union = union(withMarkers, ImmutableList.copyOf(concat(outputs, markers)));

            // add count aggregations and filter rows where any of the counts is >= 1
            List<VariableReferenceExpression> aggregationOutputs = allocateVariables(markers.size(), "count", BIGINT);
            AggregationNode aggregation = computeCounts(union, node.getOutputVariables(), markers, aggregationOutputs);
            FilterNode filterNode = addFilterForIntersect(aggregation);

            return project(filterNode, outputs);
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, RewriteContext<Void> rewriteContext)
        {
            List<PlanNode> sources = node.getSources().stream()
                    .map(rewriteContext::rewrite)
                    .collect(toList());

            List<VariableReferenceExpression> markers = allocateVariables(sources.size(), MARKER, BOOLEAN);

            // identity projection for all the fields in each of the sources plus marker columns
            List<PlanNode> withMarkers = appendMarkers(markers, sources, node);

            // add a union over all the rewritten sources. The outputs of the union have the same name as the
            // original except node
            List<VariableReferenceExpression> outputs = node.getOutputVariables();
            UnionNode union = union(withMarkers, ImmutableList.copyOf(concat(outputs, markers)));

            // add count aggregations and filter rows where count for the first source is >= 1 and all others are 0
            List<VariableReferenceExpression> aggregationOutputs = allocateVariables(markers.size(), "count", BIGINT);
            AggregationNode aggregation = computeCounts(union, node.getOutputVariables(), markers, aggregationOutputs);
            FilterNode filterNode = addFilterForExcept(aggregation, aggregationOutputs.get(0), aggregationOutputs.subList(1, aggregationOutputs.size()));

            return project(filterNode, outputs);
        }

        private List<VariableReferenceExpression> allocateVariables(int count, String nameHint, Type type)
        {
            ImmutableList.Builder<VariableReferenceExpression> variablesBuilder = ImmutableList.builder();
            for (int i = 0; i < count; i++) {
                variablesBuilder.add(variableAllocator.newVariable(nameHint, type));
            }
            return variablesBuilder.build();
        }

        private List<PlanNode> appendMarkers(List<VariableReferenceExpression> markers, List<PlanNode> nodes, SetOperationNode node)
        {
            ImmutableList.Builder<PlanNode> result = ImmutableList.builder();
            for (int i = 0; i < nodes.size(); i++) {
                result.add(appendMarkers(nodes.get(i), i, markers, Maps.transformValues(node.sourceVariableMap(i), variable -> variable)));
            }
            return result.build();
        }

        private PlanNode appendMarkers(PlanNode source, int markerIndex, List<VariableReferenceExpression> markers, Map<VariableReferenceExpression, VariableReferenceExpression> projections)
        {
            Assignments.Builder assignments = Assignments.builder();
            // add existing intersect symbols to projection
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : projections.entrySet()) {
                VariableReferenceExpression variable = variableAllocator.newVariable(entry.getKey().getSourceLocation(), entry.getKey().getName(), entry.getKey().getType());
                assignments.put(variable, entry.getValue());
            }

            // add extra marker fields to the projection
            for (int i = 0; i < markers.size(); ++i) {
                RowExpression expression = (i == markerIndex) ? TRUE_CONSTANT : new ConstantExpression(null, BOOLEAN);
                assignments.put(variableAllocator.newVariable(markers.get(i).getSourceLocation(), markers.get(i).getName(), BOOLEAN), expression);
            }

            return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
        }

        private UnionNode union(List<PlanNode> nodes, List<VariableReferenceExpression> outputs)
        {
            ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs = ImmutableListMultimap.builder();
            for (PlanNode source : nodes) {
                for (int i = 0; i < source.getOutputVariables().size(); i++) {
                    outputsToInputs.put(outputs.get(i), source.getOutputVariables().get(i));
                }
            }

            ListMultimap<VariableReferenceExpression, VariableReferenceExpression> mapping = outputsToInputs.build();
            return new UnionNode(nodes.get(0).getSourceLocation(), idAllocator.getNextId(), nodes, ImmutableList.copyOf(mapping.keySet()), fromListMultimap(mapping));
        }

        private AggregationNode computeCounts(UnionNode sourceNode, List<VariableReferenceExpression> originalColumns, List<VariableReferenceExpression> markers, List<VariableReferenceExpression> aggregationOutputs)
        {
            ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();

            for (int i = 0; i < markers.size(); i++) {
                VariableReferenceExpression output = aggregationOutputs.get(i);
                aggregations.put(output, new Aggregation(
                        new CallExpression(
                                output.getSourceLocation(),
                                "count",
                                functionResolution.countFunction(markers.get(i).getType()),
                                BIGINT,
                                ImmutableList.of(markers.get(i))),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty()));
            }

            return new AggregationNode(
                    sourceNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    sourceNode,
                    aggregations.build(),
                    singleGroupingSet(originalColumns),
                    ImmutableList.of(),
                    Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());
        }

        private FilterNode addFilterForIntersect(AggregationNode aggregation)
        {
            ImmutableList<RowExpression> predicates = aggregation.getAggregations().keySet().stream()
                    .map(column -> comparisonExpression(functionResolution, GREATER_THAN_OR_EQUAL, column, new ConstantExpression(1L, BIGINT)))
                    .collect(toImmutableList());
            return new FilterNode(aggregation.getSourceLocation(), idAllocator.getNextId(), aggregation, and(predicates));
        }

        private FilterNode addFilterForExcept(AggregationNode aggregation, VariableReferenceExpression firstSource, List<VariableReferenceExpression> remainingSources)
        {
            ImmutableList.Builder<RowExpression> predicatesBuilder = ImmutableList.builder();
            predicatesBuilder.add(comparisonExpression(functionResolution, GREATER_THAN_OR_EQUAL, firstSource, new ConstantExpression(1L, BIGINT)));
            for (VariableReferenceExpression variable : remainingSources) {
                predicatesBuilder.add(comparisonExpression(functionResolution, EQUAL, variable, new ConstantExpression(0L, BIGINT)));
            }

            return new FilterNode(aggregation.getSourceLocation(), idAllocator.getNextId(), aggregation, and(predicatesBuilder.build()));
        }

        private ProjectNode project(PlanNode node, List<VariableReferenceExpression> columns)
        {
            return new ProjectNode(
                    idAllocator.getNextId(),
                    node,
                    identityAssignments(columns));
        }
    }
}
