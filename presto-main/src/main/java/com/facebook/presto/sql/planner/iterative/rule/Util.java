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

import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

class Util
{
    private Util()
    {
    }

    /**
     * Prune the set of available inputs to those required by the given expressions.
     * <p>
     * If all inputs are used, return Optional.empty() to indicate that no pruning is necessary.
     */
    public static Optional<Set<VariableReferenceExpression>> pruneInputs(
            Collection<VariableReferenceExpression> availableInputs,
            Collection<RowExpression> expressions,
            TypeProvider types)
    {
        Set<VariableReferenceExpression> availableInputsSet = ImmutableSet.copyOf(availableInputs);
        Set<VariableReferenceExpression> prunedInputs = Sets.filter(
                availableInputsSet,
                VariablesExtractor.extractUnique(expressions)::contains);

        if (prunedInputs.size() == availableInputsSet.size()) {
            return Optional.empty();
        }

        return Optional.of(prunedInputs);
    }

    /**
     * Transforms a plan like P->C->X to C->P->X
     */
    public static PlanNode transpose(PlanNode parent, PlanNode child)
    {
        return child.replaceChildren(ImmutableList.of(
                parent.replaceChildren(
                        child.getSources())));
    }

    /**
     * @return If the node has outputs not in permittedOutputs, returns an identity projection containing only those node outputs also in permittedOutputs.
     */
    public static Optional<PlanNode> restrictOutputs(PlanNodeIdAllocator idAllocator, PlanNode node, Set<VariableReferenceExpression> permittedOutputs)
    {
        List<VariableReferenceExpression> restrictedOutputs = node.getOutputVariables().stream()
                .filter(permittedOutputs::contains)
                .collect(toImmutableList());

        if (restrictedOutputs.size() == node.getOutputVariables().size()) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectNode(
                        node.getSourceLocation(),
                        idAllocator.getNextId(),
                        node,
                        identityAssignments(restrictedOutputs),
                        LOCAL));
    }

    /**
     * @return The original node, with identity projections possibly inserted between node and each child, limiting the columns to those permitted.
     * Returns a present Optional iff at least one child was rewritten.
     */
    @SafeVarargs
    public static Optional<PlanNode> restrictChildOutputs(PlanNodeIdAllocator idAllocator, PlanNode node, Set<VariableReferenceExpression>... permittedChildOutputsArgs)
    {
        List<Set<VariableReferenceExpression>> permittedChildOutputs = ImmutableList.copyOf(permittedChildOutputsArgs);

        checkArgument(
                (node.getSources().size() == permittedChildOutputs.size()),
                "Mismatched child (%d) and permitted outputs (%d) sizes",
                node.getSources().size(),
                permittedChildOutputs.size());

        ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
        boolean rewroteChildren = false;

        for (int i = 0; i < node.getSources().size(); ++i) {
            PlanNode oldChild = node.getSources().get(i);
            Optional<PlanNode> newChild = restrictOutputs(idAllocator, oldChild, permittedChildOutputs.get(i));
            rewroteChildren |= newChild.isPresent();
            newChildrenBuilder.add(newChild.orElse(oldChild));
        }

        if (!rewroteChildren) {
            return Optional.empty();
        }
        return Optional.of(node.replaceChildren(newChildrenBuilder.build()));
    }

    public static OrderingScheme pruneOrderingColumns(OrderingScheme nodeOrderingScheme, LogicalProperties sourceLogicalProperties)
    {
        requireNonNull(nodeOrderingScheme, "nodeOrderingScheme is null");
        requireNonNull(sourceLogicalProperties, "nodeOrderingScheme is null");

        List<VariableReferenceExpression> orderingVariables = nodeOrderingScheme.getOrderBy().stream().map(Ordering::getVariable).collect(toImmutableList());
        int sizeSmallestDistinctPrefix = sizeSmallestDistinctPrefix(sourceLogicalProperties, orderingVariables);
        if (sizeSmallestDistinctPrefix == 0) {
            return nodeOrderingScheme;
        }
        List<Ordering> keyPrefix = nodeOrderingScheme.getOrderBy().subList(0, sizeSmallestDistinctPrefix);
        return new OrderingScheme(keyPrefix);
    }

    private static int sizeSmallestDistinctPrefix(LogicalProperties logicalProperties, List<VariableReferenceExpression> candidateVariables)
    {
        HashSet<VariableReferenceExpression> possibleKeySet = new HashSet<>();
        for (int prefixSize = 1; prefixSize < candidateVariables.size(); prefixSize++) {
            possibleKeySet.add(candidateVariables.get(prefixSize - 1));
            if (logicalProperties.isDistinct(possibleKeySet)) {
                return prefixSize;
            }
        }
        return 0;
    }
}
