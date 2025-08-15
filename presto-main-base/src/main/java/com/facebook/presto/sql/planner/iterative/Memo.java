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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.plan.LogicalProperties;
import com.facebook.presto.spi.plan.LogicalPropertiesProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Stores a plan in a form that's efficient to mutate locally (i.e. without
 * having to do full ancestor tree rewrites due to plan nodes being immutable).
 * <p>
 * Each node in a plan is placed in a group, and it's children are replaced with
 * symbolic references to the corresponding groups.
 * <p>
 * For example, a plan like:
 * <pre>
 *    A -> B -> C -> D
 *           \> E -> F
 * </pre>
 * would be stored as:
 * <pre>
 * root: G0
 *
 * G0 : { A -> G1 }
 * G1 : { B -> [G2, G3] }
 * G2 : { C -> G4 }
 * G3 : { E -> G5 }
 * G4 : { D }
 * G5 : { F }
 * </pre>
 * Groups are reference-counted, and groups that become unreachable from the root
 * due to mutations in a subtree get garbage-collected.
 */
public class Memo
{
    private static final int ROOT_GROUP_REF = 0;

    private final PlanNodeIdAllocator idAllocator;
    private final int rootGroup;

    private final Map<Integer, Group> groups = new HashMap<>();
    private final Optional<LogicalPropertiesProvider> logicalPropertiesProvider;

    private int nextGroupId = ROOT_GROUP_REF + 1;

    public Memo(PlanNodeIdAllocator idAllocator, PlanNode plan)
    {
        this(idAllocator, plan, Optional.empty());
    }

    public Memo(PlanNodeIdAllocator idAllocator, PlanNode plan, Optional<LogicalPropertiesProvider> logicalPropertiesProvider)
    {
        this.idAllocator = idAllocator;
        this.logicalPropertiesProvider = logicalPropertiesProvider;
        rootGroup = insertRecursive(plan);
        groups.get(rootGroup).incomingReferences.add(ROOT_GROUP_REF);
    }

    public int getRootGroup()
    {
        return rootGroup;
    }

    private Group getGroup(int group)
    {
        checkArgument(groups.containsKey(group), "Invalid group: %s", group);
        return groups.get(group);
    }

    public Optional<LogicalProperties> getLogicalProperties(int group)
    {
        checkArgument(groups.containsKey(group), "Invalid group: %s", group);
        return groups.get(group).logicalProperties;
    }

    public PlanNode getNode(int group)
    {
        return getGroup(group).membership;
    }

    public PlanNode resolve(GroupReference groupReference)
    {
        return getNode(groupReference.getGroupId());
    }

    public PlanNode extract()
    {
        return extract(getNode(rootGroup));
    }

    private PlanNode extract(PlanNode node)
    {
        return resolveGroupReferences(node, Lookup.from(planNode -> Stream.of(this.resolve(planNode))));
    }

    public PlanNode replace(int group, PlanNode node, String reason)
    {
        PlanNode old = getGroup(group).membership;

        checkArgument(new HashSet<>(old.getOutputVariables()).equals(new HashSet<>(node.getOutputVariables())),
                "%s: transformed expression doesn't produce same outputs: %s vs %s for node: %s",
                reason,
                old.getOutputVariables(),
                node.getOutputVariables(),
                node);

        if (node instanceof GroupReference) {
            node = getNode(((GroupReference) node).getGroupId());
        }
        else {
            node = insertChildrenAndRewrite(node);
        }

        incrementReferenceCounts(node, group);
        getGroup(group).membership = node;

        if (logicalPropertiesProvider.isPresent()) {
            // for now, we replace existing group logical properties with those computed for the new node
            // as we cannot ensure equivalence for all plans in a group until we support functional dependencies
            // once we can ensure equivalence we can simply reuse the previously computed properties for all plans in the group
            LogicalProperties newLogicalProperties = node.computeLogicalProperties(logicalPropertiesProvider.get());
            getGroup(group).logicalProperties = Optional.of(newLogicalProperties);
        }
        decrementReferenceCounts(old, group);
        evictStatisticsAndCost(group);

        return node;
    }

    public void assignStatsEquivalentPlanNode(GroupReference reference, Optional<PlanNode> statsEquivalentPlanNode)
    {
        getGroup(reference.getGroupId()).assignStatsEquivalentPlanNode(statsEquivalentPlanNode);
    }

    private void evictStatisticsAndCost(int group)
    {
        getGroup(group).stats = null;
        getGroup(group).cost = null;
        for (int parentGroup : getGroup(group).incomingReferences.elementSet()) {
            if (parentGroup != ROOT_GROUP_REF) {
                evictStatisticsAndCost(parentGroup);
            }
        }
    }

    public Optional<PlanNodeStatsEstimate> getStats(int group)
    {
        return Optional.ofNullable(getGroup(group).stats);
    }

    public void storeStats(int groupId, PlanNodeStatsEstimate stats)
    {
        Group group = getGroup(groupId);
        if (group.stats != null) {
            evictStatisticsAndCost(groupId); // cost is derived from stats, also needs eviction
        }
        group.stats = requireNonNull(stats, "stats is null");
    }

    public Optional<PlanCostEstimate> getCost(int group)
    {
        return Optional.ofNullable(getGroup(group).cost);
    }

    public void storeCost(int group, PlanCostEstimate cost)
    {
        getGroup(group).cost = requireNonNull(cost, "cost is null");
    }

    private void incrementReferenceCounts(PlanNode fromNode, int fromGroup)
    {
        Set<Integer> references = getAllReferences(fromNode);

        for (int group : references) {
            groups.get(group).incomingReferences.add(fromGroup);
        }
    }

    private void decrementReferenceCounts(PlanNode fromNode, int fromGroup)
    {
        Set<Integer> references = getAllReferences(fromNode);

        for (int group : references) {
            Group childGroup = groups.get(group);
            checkState(childGroup.incomingReferences.remove(fromGroup), "Reference to remove not found");

            if (childGroup.incomingReferences.isEmpty()) {
                deleteGroup(group);
            }
        }
    }

    private Set<Integer> getAllReferences(PlanNode node)
    {
        return node.getSources().stream()
                .map(GroupReference.class::cast)
                .map(GroupReference::getGroupId)
                .collect(Collectors.toSet());
    }

    private void deleteGroup(int group)
    {
        checkArgument(getGroup(group).incomingReferences.isEmpty(), "Cannot delete group that has incoming references");
        PlanNode deletedNode = groups.remove(group).membership;
        decrementReferenceCounts(deletedNode, group);
    }

    private PlanNode insertChildrenAndRewrite(PlanNode node)
    {
        return node.replaceChildren(
                node.getSources().stream()
                        .map(child -> {
                            int childId = insertRecursive(child);
                            return new GroupReference(
                                    node.getSourceLocation(),
                                    idAllocator.getNextId(),
                                    childId,
                                    child.getOutputVariables(),
                                    groups.get(childId).logicalProperties);
                        })
                        .collect(Collectors.toList()));
    }

    private int insertRecursive(PlanNode node)
    {
        if (node instanceof GroupReference) {
            return ((GroupReference) node).getGroupId();
        }

        int group = nextGroupId();
        PlanNode rewritten = insertChildrenAndRewrite(node);

        groups.put(group, new Group(rewritten, logicalPropertiesProvider.map(rewritten::computeLogicalProperties)));
        incrementReferenceCounts(rewritten, group);

        return group;
    }

    private int nextGroupId()
    {
        return nextGroupId++;
    }

    public int getGroupCount()
    {
        return groups.size();
    }

    private static final class Group
    {
        private final Multiset<Integer> incomingReferences = HashMultiset.create();
        private PlanNode membership;
        private Optional<LogicalProperties> logicalProperties;
        @Nullable
        private PlanNodeStatsEstimate stats;
        @Nullable
        private PlanCostEstimate cost;

        private Group(PlanNode member, Optional<LogicalProperties> logicalProperties)
        {
            this.membership = requireNonNull(member, "member is null");
            this.logicalProperties = logicalProperties;
        }

        private void assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
        {
            membership = membership.assignStatsEquivalentPlanNode(statsEquivalentPlanNode);
        }
    }
}
