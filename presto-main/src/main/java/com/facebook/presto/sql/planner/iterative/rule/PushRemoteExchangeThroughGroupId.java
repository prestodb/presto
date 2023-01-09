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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.getPartitioningProviderCatalog;
import static com.facebook.presto.SystemSessionProperties.shouldPushRemoteExchangeThroughGroupId;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.groupId;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Pushes RemoteExchange node down through GroupId node when GroupId node contains non-empty
 * set of common grouping columns.
 *
 * As an example this rule will change following plan
 * Aggregation [final]
 *   - RemoteExchange [repartition]
 *     - Aggregation [partial]
 *       - GroupId
 *         - TableScan
 * To
 * Aggregation
 *   - GroupId
 *     - RemoteExchange [repartition]
 *       - TableScan
 *
 * We can leverage this optimization rule to rewrite plan to be more efficient
 * if following conditions are true:
 *
 * 1. There are large number of grouping sets in query.
 * 2. Partial aggregation reduction ratio is not great.
 * 3. There is least one common grouping key among grouping sets.
 *
 * Note: This rule is disabled by default. Session property
 * PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID can be used to enable it.
 */
public final class PushRemoteExchangeThroughGroupId
        implements Rule<ExchangeNode>
{
    private final Metadata metadata;
    private static final Capture<GroupIdNode> GROUP_ID = newCapture();
    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .matching(exchange -> exchange.getScope().isRemote())
            .matching(exchange -> exchange.getType() == REPARTITION)
            .with(source().matching(
                    groupId()
                            .capturedAs(GROUP_ID)
                            .matching(groupId -> !groupId.getCommonGroupingColumns().isEmpty())));

    public PushRemoteExchangeThroughGroupId(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return shouldPushRemoteExchangeThroughGroupId(session);
    }

    @Override
    public Result apply(ExchangeNode node, Captures captures, Context context)
    {
        GroupIdNode groupIdNode = captures.get(GROUP_ID);

        List<VariableReferenceExpression> inputs = getOnlyElement(node.getInputs());
        inputs = removeVariable(inputs, groupIdNode.getGroupIdVariable());
        inputs = replaceAlias(inputs, groupIdNode.getGroupingColumns());

        PartitioningScheme partitioningScheme = node.getPartitioningScheme();
        List<VariableReferenceExpression> outputLayout = partitioningScheme.getOutputLayout();
        outputLayout = removeVariable(outputLayout, groupIdNode.getGroupIdVariable());
        outputLayout = replaceAlias(outputLayout, groupIdNode.getGroupingColumns());

        Set<VariableReferenceExpression> commonGroupingColumns = groupIdNode.getCommonGroupingColumns();
        List<VariableReferenceExpression> partitionColumns = replaceAlias(commonGroupingColumns, groupIdNode.getGroupingColumns());

        // Check that new |partitionColumns| must be subset of original partition columns.
        Map<VariableReferenceExpression, VariableReferenceExpression> groupingColumns = groupIdNode.getGroupingColumns();
        List<VariableReferenceExpression> originalPartitionColumns =
                partitioningScheme.getPartitioning().getVariableReferences()
                        .stream()
                        .map(expr -> groupingColumns.getOrDefault(expr, expr))
                        .collect(toImmutableList());
        if (!originalPartitionColumns.containsAll(partitionColumns)) {
            return Result.empty();
        }

        // Create new PartitioningHandle.
        PartitioningHandle partitioningHandle;
        if (GlobalSystemConnector.NAME.equals(getPartitioningProviderCatalog(context.getSession()))) {
            partitioningHandle = partitioningScheme.getPartitioning().getHandle();
        }
        else {
            partitioningHandle = createPartitioningHandle(context.getSession(), partitionColumns);
        }

        return Result.ofPlanNode(new GroupIdNode(
                node.getSourceLocation(),
                groupIdNode.getId(),
                new ExchangeNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getType(),
                        node.getScope(),
                        new PartitioningScheme(
                                Partitioning.create(partitioningHandle, partitionColumns),
                                outputLayout,
                                partitioningScheme.getHashColumn(),
                                partitioningScheme.isReplicateNullsAndAny(),
                                partitioningScheme.getBucketToPartition()),
                        ImmutableList.of(groupIdNode.getSource()),
                        ImmutableList.of(inputs),
                        node.isEnsureSourceOrdering(),
                        node.getOrderingScheme()),
                groupIdNode.getGroupingSets(),
                groupIdNode.getGroupingColumns(),
                groupIdNode.getAggregationArguments(),
                groupIdNode.getGroupIdVariable()));
    }

    private static List<VariableReferenceExpression> removeVariable(List<VariableReferenceExpression> variables, VariableReferenceExpression variableToRemove)
    {
        return variables.stream()
                .filter(variable -> !variableToRemove.equals(variable))
                .collect(toImmutableList());
    }

    private static List<VariableReferenceExpression> replaceAlias(Collection<VariableReferenceExpression> variables, Map<VariableReferenceExpression, VariableReferenceExpression> mapping)
    {
        return variables.stream()
                .map(variable -> mapping.containsKey(variable) ? mapping.get(variable) : variable)
                .collect(toImmutableList());
    }

    private PartitioningHandle createPartitioningHandle(Session session, Collection<VariableReferenceExpression> partitioningColumns)
    {
        List<Type> partitioningTypes = partitioningColumns.stream()
                .map(VariableReferenceExpression::getType)
                .collect(toImmutableList());
        return metadata.getPartitioningHandleForExchange(
                session,
                getPartitioningProviderCatalog(session),
                getHashPartitionCount(session),
                partitioningTypes);
    }
}
