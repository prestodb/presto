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

import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

// TODO: move this class to TransformCorrelatedScalarAggregationToJoin when old optimizer is gone
public class ScalarAggregationToJoinRewriter
{
    private final FunctionResolution functionResolution;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Lookup lookup;
    private final PlanNodeDecorrelator planNodeDecorrelator;

    public ScalarAggregationToJoinRewriter(FunctionAndTypeManager functionAndTypeManager, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        requireNonNull(functionAndTypeManager, "metadata is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                functionAndTypeManager);
        this.planNodeDecorrelator = new PlanNodeDecorrelator(idAllocator, variableAllocator, lookup, logicalRowExpressions);
    }

    public PlanNode rewriteScalarAggregation(LateralJoinNode lateralJoinNode, AggregationNode aggregation)
    {
        List<VariableReferenceExpression> correlation = lateralJoinNode.getCorrelation();
        Optional<DecorrelatedNode> source = planNodeDecorrelator.decorrelateFilters(lookup.resolve(aggregation.getSource()), correlation);
        if (!source.isPresent()) {
            return lateralJoinNode;
        }

        VariableReferenceExpression nonNull = variableAllocator.newVariable("non_null", BooleanType.BOOLEAN);
        Assignments scalarAggregationSourceAssignments = Assignments.builder()
                .putAll(identityAssignments(source.get().getNode().getOutputVariables()))
                .put(nonNull, TRUE_CONSTANT)
                .build();
        ProjectNode scalarAggregationSourceWithNonNullableVariable = new ProjectNode(
                idAllocator.getNextId(),
                source.get().getNode(),
                scalarAggregationSourceAssignments);

        return rewriteScalarAggregation(
                lateralJoinNode,
                aggregation,
                scalarAggregationSourceWithNonNullableVariable,
                source.get().getCorrelatedPredicates(),
                nonNull);
    }

    private PlanNode rewriteScalarAggregation(
            LateralJoinNode lateralJoinNode,
            AggregationNode scalarAggregation,
            PlanNode scalarAggregationSource,
            Optional<RowExpression> joinExpression,
            VariableReferenceExpression nonNull)
    {
        AssignUniqueId inputWithUniqueColumns = new AssignUniqueId(
                lateralJoinNode.getSourceLocation(),
                idAllocator.getNextId(),
                lateralJoinNode.getInput(),
                variableAllocator.newVariable(nonNull.getSourceLocation(), "unique", BIGINT));

        JoinNode leftOuterJoin = new JoinNode(
                scalarAggregation.getSourceLocation(),
                idAllocator.getNextId(),
                JoinNode.Type.LEFT,
                inputWithUniqueColumns,
                scalarAggregationSource,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(inputWithUniqueColumns.getOutputVariables())
                        .addAll(scalarAggregationSource.getOutputVariables())
                        .build(),
                joinExpression,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Optional<AggregationNode> aggregationNode = createAggregationNode(
                scalarAggregation,
                leftOuterJoin,
                nonNull);

        if (!aggregationNode.isPresent()) {
            return lateralJoinNode;
        }

        Optional<ProjectNode> subqueryProjection = searchFrom(lateralJoinNode.getSubquery(), lookup)
                .where(ProjectNode.class::isInstance)
                .recurseOnlyWhen(EnforceSingleRowNode.class::isInstance)
                .findFirst();

        List<VariableReferenceExpression> aggregationOutputVariables = getTruncatedAggregationVariables(lateralJoinNode, aggregationNode.get());

        if (subqueryProjection.isPresent()) {
            Assignments assignments = Assignments.builder()
                    .putAll(identityAssignments(aggregationOutputVariables))
                    .putAll(subqueryProjection.get().getAssignments())
                    .build();

            return new ProjectNode(
                    idAllocator.getNextId(),
                    aggregationNode.get(),
                    assignments);
        }
        else {
            return new ProjectNode(
                    idAllocator.getNextId(),
                    aggregationNode.get(),
                    identityAssignments(aggregationOutputVariables));
        }
    }

    private List<VariableReferenceExpression> getTruncatedAggregationVariables(LateralJoinNode lateralJoinNode, AggregationNode aggregationNode)
    {
        Set<VariableReferenceExpression> applyVariables = new HashSet<>(lateralJoinNode.getOutputVariables());
        return aggregationNode.getOutputVariables().stream()
                .filter(applyVariables::contains)
                .collect(toImmutableList());
    }

    private Optional<AggregationNode> createAggregationNode(
            AggregationNode scalarAggregation,
            JoinNode leftOuterJoin,
            VariableReferenceExpression nonNull)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : scalarAggregation.getAggregations().entrySet()) {
            VariableReferenceExpression variable = entry.getKey();
            if (functionResolution.isCountFunction(entry.getValue().getFunctionHandle())) {
                Type scalarAggregationSourceType = nonNull.getType();
                aggregations.put(variable, new Aggregation(
                        new CallExpression(
                                variable.getSourceLocation(),
                                "count",
                                functionResolution.countFunction(scalarAggregationSourceType),
                                BIGINT,
                                ImmutableList.of(nonNull)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        entry.getValue().getMask()));
            }
            else {
                aggregations.put(variable, entry.getValue());
            }
        }

        return Optional.of(new AggregationNode(
                scalarAggregation.getSourceLocation(),
                idAllocator.getNextId(),
                leftOuterJoin,
                aggregations.build(),
                singleGroupingSet(leftOuterJoin.getLeft().getOutputVariables()),
                ImmutableList.of(),
                scalarAggregation.getStep(),
                scalarAggregation.getHashVariable(),
                Optional.empty()));
    }
}
