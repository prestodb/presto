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
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPullConstantProjectionAboveExchange;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;

/**
 * Transforms:
 * <pre>
 *  Exchange(REMOTE)
 *    Project(a = col_a, c = CONSTANT)
 *      Source(col_a)
 * </pre>
 * to:
 * <pre>
 *  Project(a = a, c = CONSTANT)
 *    Exchange(REMOTE)
 *      Project(a = col_a)
 *        Source(col_a)
 * </pre>
 *
 * This avoids serializing and shuffling constant values across the network.
 * Constants used in partitioning, ordering, or hash columns are not pulled up.
 * For multi-source exchanges (UNION), only constants identical across ALL sources are pulled up.
 */
public class PullConstantProjectionAboveExchange
        implements Rule<ExchangeNode>
{
    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .matching(exchange -> exchange.getScope().isRemote());

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ExchangeNode exchange, Captures captures, Context context)
    {
        if (!isPullConstantProjectionAboveExchange(context.getSession())) {
            return Result.empty();
        }

        List<PlanNode> sources = exchange.getSources();
        List<List<VariableReferenceExpression>> inputs = exchange.getInputs();
        List<VariableReferenceExpression> outputLayout = exchange.getPartitioningScheme().getOutputLayout();

        // Resolve all sources through the lookup (they may be GroupReferences in the Memo)
        // and verify all sources are ProjectNodes
        ProjectNode[] resolvedSources = new ProjectNode[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            PlanNode resolved = context.getLookup().resolve(sources.get(i));
            if (!(resolved instanceof ProjectNode)) {
                return Result.empty();
            }
            resolvedSources[i] = (ProjectNode) resolved;
        }

        // Collect variables used in partitioning, hash, and ordering — these cannot be pulled up
        Set<VariableReferenceExpression> requiredByExchange = new HashSet<>();
        requiredByExchange.addAll(exchange.getPartitioningScheme().getPartitioning().getVariableReferences());
        exchange.getPartitioningScheme().getHashColumn().ifPresent(requiredByExchange::add);
        exchange.getOrderingScheme().ifPresent(ordering ->
                requiredByExchange.addAll(ordering.getOrderByVariables()));

        // For each output position, check if all sources produce the same constant
        // Map from output variable index to the constant expression
        Map<Integer, ConstantExpression> pullableConstants = new HashMap<>();

        for (int outputIdx = 0; outputIdx < outputLayout.size(); outputIdx++) {
            VariableReferenceExpression outputVar = outputLayout.get(outputIdx);

            // Skip if this variable is required by exchange mechanics
            if (requiredByExchange.contains(outputVar)) {
                continue;
            }

            ConstantExpression commonConstant = null;
            boolean allSourcesHaveSameConstant = true;

            for (int sourceIdx = 0; sourceIdx < resolvedSources.length; sourceIdx++) {
                ProjectNode projectNode = resolvedSources[sourceIdx];
                VariableReferenceExpression inputVar = inputs.get(sourceIdx).get(outputIdx);
                RowExpression assignment = projectNode.getAssignments().get(inputVar);

                if (assignment instanceof ConstantExpression) {
                    ConstantExpression constant = (ConstantExpression) assignment;
                    if (commonConstant == null) {
                        commonConstant = constant;
                    }
                    else if (!constantsAreEqual(commonConstant, constant)) {
                        allSourcesHaveSameConstant = false;
                        break;
                    }
                }
                else {
                    allSourcesHaveSameConstant = false;
                    break;
                }
            }

            if (allSourcesHaveSameConstant && commonConstant != null) {
                pullableConstants.put(outputIdx, commonConstant);
            }
        }

        if (pullableConstants.isEmpty()) {
            return Result.empty();
        }

        // Don't pull all columns — exchange must retain at least one output
        if (pullableConstants.size() >= outputLayout.size()) {
            return Result.empty();
        }

        // Build new sources with constant assignments removed
        ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
        ImmutableList.Builder<List<VariableReferenceExpression>> newInputs = ImmutableList.builder();

        for (int sourceIdx = 0; sourceIdx < resolvedSources.length; sourceIdx++) {
            ProjectNode projectNode = resolvedSources[sourceIdx];
            Set<VariableReferenceExpression> constantInputVars = new HashSet<>();
            for (int outputIdx : pullableConstants.keySet()) {
                constantInputVars.add(inputs.get(sourceIdx).get(outputIdx));
            }

            // Build new assignments without the pulled constants
            Assignments.Builder newAssignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().entrySet()) {
                if (!constantInputVars.contains(entry.getKey())) {
                    newAssignments.put(entry.getKey(), entry.getValue());
                }
            }

            // Build new input list without the pulled constant positions
            ImmutableList.Builder<VariableReferenceExpression> newInputList = ImmutableList.builder();
            for (int outputIdx = 0; outputIdx < outputLayout.size(); outputIdx++) {
                if (!pullableConstants.containsKey(outputIdx)) {
                    newInputList.add(inputs.get(sourceIdx).get(outputIdx));
                }
            }

            Assignments builtAssignments = newAssignments.build();
            if (builtAssignments.isEmpty()) {
                return Result.empty();
            }

            newSources.add(new ProjectNode(
                    projectNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    projectNode.getSources().get(0),
                    builtAssignments,
                    projectNode.getLocality()));
            newInputs.add(newInputList.build());
        }

        // Build new output layout without the pulled constants
        ImmutableList.Builder<VariableReferenceExpression> newOutputLayout = ImmutableList.builder();
        for (int outputIdx = 0; outputIdx < outputLayout.size(); outputIdx++) {
            if (!pullableConstants.containsKey(outputIdx)) {
                newOutputLayout.add(outputLayout.get(outputIdx));
            }
        }

        PartitioningScheme newPartitioningScheme = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                newOutputLayout.build(),
                exchange.getPartitioningScheme().getHashColumn(),
                exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                exchange.getPartitioningScheme().isScaleWriters(),
                exchange.getPartitioningScheme().getEncoding(),
                exchange.getPartitioningScheme().getBucketToPartition());

        ExchangeNode newExchange = new ExchangeNode(
                exchange.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                exchange.getType(),
                exchange.getScope(),
                newPartitioningScheme,
                newSources.build(),
                newInputs.build(),
                exchange.isEnsureSourceOrdering(),
                exchange.getOrderingScheme());

        // Build the projection above the exchange: identity for remaining columns + pulled constants
        Assignments.Builder aboveAssignments = Assignments.builder();
        for (int outputIdx = 0; outputIdx < outputLayout.size(); outputIdx++) {
            VariableReferenceExpression outputVar = outputLayout.get(outputIdx);
            if (pullableConstants.containsKey(outputIdx)) {
                aboveAssignments.put(outputVar, pullableConstants.get(outputIdx));
            }
            else {
                aboveAssignments.put(outputVar, outputVar);
            }
        }

        ProjectNode aboveProject = new ProjectNode(
                exchange.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                newExchange,
                aboveAssignments.build(),
                ProjectNode.Locality.LOCAL);

        return Result.ofPlanNode(aboveProject);
    }

    private static boolean constantsAreEqual(ConstantExpression a, ConstantExpression b)
    {
        if (!a.getType().equals(b.getType())) {
            return false;
        }
        if (a.isNull() && b.isNull()) {
            return true;
        }
        if (a.isNull() || b.isNull()) {
            return false;
        }
        return a.getValue().equals(b.getValue());
    }
}
