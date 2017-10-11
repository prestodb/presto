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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

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
    private final PlanNodeIdAllocator idAllocator;
    private final int rootGroup;

    private final Int2ObjectOpenHashMap<PlanNode> membership = new Int2ObjectOpenHashMap<>();
    private final Int2IntOpenHashMap referenceCounts = new Int2IntOpenHashMap();

    private int nextGroupId;

    public Memo(PlanNodeIdAllocator idAllocator, PlanNode plan)
    {
        this.idAllocator = idAllocator;
        rootGroup = insertRecursive(plan);
        referenceCounts.put(rootGroup, 1);
    }

    public int getRootGroup()
    {
        return rootGroup;
    }

    public PlanNode getNode(int group)
    {
        checkArgument(membership.containsKey(group), "Invalid group: %s", group);
        return membership.get(group);
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
        PlanNode old = membership.get(group);

        checkArgument(new HashSet<>(old.getOutputSymbols()).equals(new HashSet<>(node.getOutputSymbols())),
                "%s: transformed expression doesn't produce same outputs: %s vs %s",
                reason,
                old.getOutputSymbols(),
                node.getOutputSymbols());

        if (node instanceof GroupReference) {
            node = getNode(((GroupReference) node).getGroupId());
        }
        else {
            node = insertChildrenAndRewrite(node);
        }

        incrementReferenceCounts(node);
        membership.put(group, node);
        decrementReferenceCounts(old);

        return node;
    }

    private void incrementReferenceCounts(PlanNode node)
    {
        Set<Integer> references = getAllReferences(node);

        for (int group : references) {
            referenceCounts.compute(group, (g, count) -> count + 1);
        }
    }

    private void decrementReferenceCounts(PlanNode node)
    {
        Set<Integer> references = getAllReferences(node);

        for (int group : references) {
            int newCount = referenceCounts.compute(group, (g, count) -> count - 1);
            checkState(newCount >= 0, "Reference count became negative");

            if (newCount == 0) {
                PlanNode child = membership.get(group);
                deleteGroup(group);
                decrementReferenceCounts(child);
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
        membership.remove(group);
        referenceCounts.remove(group);
    }

    private PlanNode insertChildrenAndRewrite(PlanNode node)
    {
        return node.replaceChildren(
                node.getSources().stream()
                        .map(child -> new GroupReference(
                                idAllocator.getNextId(),
                                insertRecursive(child),
                                child.getOutputSymbols()))
                        .collect(Collectors.toList()));
    }

    private int insertRecursive(PlanNode node)
    {
        if (node instanceof GroupReference) {
            return ((GroupReference) node).getGroupId();
        }

        int group = nextGroupId();
        PlanNode rewritten = insertChildrenAndRewrite(node);

        membership.put(group, rewritten);
        referenceCounts.put(group, 0);
        incrementReferenceCounts(rewritten);

        return group;
    }

    private int nextGroupId()
    {
        return nextGroupId++;
    }

    public int getGroupCount()
    {
        return membership.size();
    }
}
