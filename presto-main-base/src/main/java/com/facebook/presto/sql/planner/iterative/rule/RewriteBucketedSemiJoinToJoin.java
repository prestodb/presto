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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isRewriteBucketedSemiJoinToJoinEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.isDistinct;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static java.util.Objects.requireNonNull;

/**
 * When both sides of a semi-join are backed by tables bucketed on the semi-join key,
 * rewrite the SemiJoinNode to a colocated LEFT JOIN with a DISTINCT on the right side.
 * This avoids data shuffle since both sides are already co-partitioned by the join key.
 *
 * <p>Using LEFT JOIN (rather than INNER JOIN) preserves all source rows and correctly
 * computes the semi-join output boolean for both positive and anti-semi-join patterns.
 *
 * <pre>
 * - SemiJoin (l.key IN r.key) → semiJoinOutput
 *     - scan l (bucketed by key)
 *     - scan r (bucketed by key)
 * </pre>
 * into:
 * <pre>
 * - Project (semiJoinOutput := IF(l.key IS NULL, NULL, r.key IS NOT NULL), identity(l.*))
 *     - LeftJoin (l.key = r.key), output = l.* + r.key
 *         - scan l
 *         - Aggregate (DISTINCT r.key)
 *             - scan r
 * </pre>
 */
public class RewriteBucketedSemiJoinToJoin
        implements Rule<SemiJoinNode>
{
    private static final String BUCKETED_BY_PROPERTY = "bucketed_by";

    private final Metadata metadata;
    private final FunctionResolution functionResolution;

    public RewriteBucketedSemiJoinToJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return semiJoin();
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteBucketedSemiJoinToJoinEnabled(session);
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        VariableReferenceExpression sourceJoinVariable = node.getSourceJoinVariable();
        VariableReferenceExpression filteringSourceJoinVariable = node.getFilteringSourceJoinVariable();

        PlanNode resolvedSource = context.getLookup().resolve(node.getSource());
        Optional<TableScanInfo> sourceInfo = findTableScanAndResolveVariable(resolvedSource, sourceJoinVariable, context);
        if (!sourceInfo.isPresent()) {
            return Result.empty();
        }

        PlanNode resolvedFilteringSource = context.getLookup().resolve(node.getFilteringSource());
        Optional<TableScanInfo> filteringInfo = findTableScanAndResolveVariable(resolvedFilteringSource, filteringSourceJoinVariable, context);
        if (!filteringInfo.isPresent()) {
            return Result.empty();
        }

        Session session = context.getSession();
        if (!isBucketedByColumn(sourceInfo.get(), session)) {
            return Result.empty();
        }
        if (!isBucketedByColumn(filteringInfo.get(), session)) {
            return Result.empty();
        }

        PlanNode distinctFilteringSource;
        if (isOutputDistinct(resolvedFilteringSource, filteringSourceJoinVariable, context)) {
            distinctFilteringSource = node.getFilteringSource();
        }
        else {
            distinctFilteringSource = new AggregationNode(
                    node.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    resolvedFilteringSource,
                    ImmutableMap.of(),
                    singleGroupingSet(ImmutableList.of(filteringSourceJoinVariable)),
                    ImmutableList.of(),
                    SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        // LEFT JOIN preserves all source rows; filteringSourceJoinVariable is NULL for non-matches
        JoinNode leftJoin = new JoinNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.LEFT,
                node.getSource(),
                distinctFilteringSource,
                ImmutableList.of(new EquiJoinClause(sourceJoinVariable, filteringSourceJoinVariable)),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(node.getSource().getOutputVariables())
                        .add(filteringSourceJoinVariable)
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        // semiJoinOutput := IF(sourceJoinVariable IS NULL, NULL, filteringSourceJoinVariable IS NOT NULL)
        // This preserves SemiJoinNode NULL semantics: when probe key is NULL, output is NULL
        VariableReferenceExpression semiJoinOutput = node.getSemiJoinOutput();
        RowExpression isNotNull = new CallExpression(
                "not",
                functionResolution.notFunction(),
                BOOLEAN,
                ImmutableList.of(new SpecialFormExpression(IS_NULL, BOOLEAN, ImmutableList.of(filteringSourceJoinVariable))));
        RowExpression semiJoinExpression = new SpecialFormExpression(
                IF,
                BOOLEAN,
                ImmutableList.of(
                        new SpecialFormExpression(IS_NULL, BOOLEAN, ImmutableList.of(sourceJoinVariable)),
                        new ConstantExpression(null, BOOLEAN),
                        isNotNull));

        ImmutableList<VariableReferenceExpression> referencedOutputs = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(node.getSource().getOutputVariables())
                .add(semiJoinOutput)
                .build();

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                leftJoin,
                Assignments.builder()
                        .putAll(identityAssignments(node.getSource().getOutputVariables()))
                        .put(semiJoinOutput, semiJoinExpression)
                        .build()
                        .filter(referencedOutputs));

        return Result.ofPlanNode(projectNode);
    }

    private Optional<TableScanInfo> findTableScanAndResolveVariable(PlanNode node, VariableReferenceExpression variable, Context context)
    {
        if (node instanceof TableScanNode) {
            TableScanNode tableScan = (TableScanNode) node;
            ColumnHandle columnHandle = tableScan.getAssignments().get(variable);
            if (columnHandle == null) {
                return Optional.empty();
            }
            return Optional.of(new TableScanInfo(tableScan, columnHandle));
        }
        else if (node instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) node;
            RowExpression inputExpression = projectNode.getAssignments().get(variable);
            if (inputExpression instanceof VariableReferenceExpression) {
                return findTableScanAndResolveVariable(
                        context.getLookup().resolve(projectNode.getSource()),
                        (VariableReferenceExpression) inputExpression,
                        context);
            }
            return Optional.empty();
        }
        else if (node instanceof FilterNode) {
            return findTableScanAndResolveVariable(
                    context.getLookup().resolve(((FilterNode) node).getSource()),
                    variable,
                    context);
        }
        return Optional.empty();
    }

    private boolean isBucketedByColumn(TableScanInfo info, Session session)
    {
        Map<String, Object> properties = metadata.getTableMetadata(session, info.tableScan.getTable()).getMetadata().getProperties();
        Object bucketedByValue = properties.get(BUCKETED_BY_PROPERTY);
        if (!(bucketedByValue instanceof List)) {
            return false;
        }
        List<?> bucketColumns = (List<?>) bucketedByValue;
        if (bucketColumns.isEmpty()) {
            return false;
        }
        String columnName = metadata.getColumnMetadata(session, info.tableScan.getTable(), info.columnHandle).getName();
        return bucketColumns.stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .anyMatch(bucketColumn -> bucketColumn.equalsIgnoreCase(columnName));
    }

    private boolean isOutputDistinct(PlanNode node, VariableReferenceExpression output, Context context)
    {
        if (node instanceof AggregationNode) {
            AggregationNode aggregationNode = (AggregationNode) node;
            return isDistinct(aggregationNode) &&
                    aggregationNode.getGroupingKeys().size() == 1 &&
                    aggregationNode.getGroupingKeys().contains(output);
        }
        else if (node instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) node;
            RowExpression inputExpression = projectNode.getAssignments().get(output);
            if (inputExpression instanceof VariableReferenceExpression) {
                return isOutputDistinct(
                        context.getLookup().resolve(projectNode.getSource()),
                        (VariableReferenceExpression) inputExpression,
                        context);
            }
            return false;
        }
        else if (node instanceof FilterNode) {
            return isOutputDistinct(
                    context.getLookup().resolve(((FilterNode) node).getSource()),
                    output,
                    context);
        }
        return false;
    }

    private static class TableScanInfo
    {
        final TableScanNode tableScan;
        final ColumnHandle columnHandle;

        TableScanInfo(TableScanNode tableScan, ColumnHandle columnHandle)
        {
            this.tableScan = tableScan;
            this.columnHandle = columnHandle;
        }
    }
}
