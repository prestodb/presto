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

import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.sql.planner.optimizations.Predicates.alwaysTrue;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;

public class PlanNodeUtils
{
    private PlanNodeUtils() {}

    public static <T extends PlanNode> Optional<T> findFirstPlanNodeMatching(PlanNode node, Predicate<PlanNode> predicate)
    {
        return findFirstPlanNodeMatching(node, predicate, alwaysTrue());
    }

    public static <T extends PlanNode> Optional<T> findFirstPlanNodeMatching(
            PlanNode node,
            Predicate<PlanNode> predicate,
            Predicate<PlanNode> recursionPredicate)
    {
        if (predicate.test(node)) {
            return Optional.of((T) node);
        }
        if (recursionPredicate.test(node)) {
            for (PlanNode source : node.getSources()) {
                Optional<T> found = findFirstPlanNodeMatching(source, predicate, recursionPredicate);
                if (found.isPresent()) {
                    return found;
                }
            }
        }
        return Optional.empty();
    }

    public static PlanNode removeFirstPlanNodesMatching(PlanNode node, Predicate<PlanNode> predicate)
    {
        return removeFirstPlanNodesMatching(node, predicate, alwaysTrue());
    }

    public static PlanNode removeFirstPlanNodesMatching(
            PlanNode node,
            Predicate<PlanNode> predicate,
            Predicate<PlanNode> recursionPredicate)
    {
        if (predicate.test(node)) {
            checkArgument(
                    node.getSources().size() == 1,
                    "Unable to remove plan node as it contains many or none children");
            return node.getSources().get(0);
        }
        if (recursionPredicate.test(node)) {
            List<PlanNode> sources = node.getSources().stream()
                    .map(source -> removeFirstPlanNodesMatching(source, predicate, recursionPredicate))
                    .collect(toImmutableList());
            return replaceChildren(node, sources);
        }
        return node;
    }

    public static PlanNode replaceAllPlanNodesMatching(
            PlanNode planNode,
            PlanNode nodeToReplace,
            Predicate<PlanNode> predicate)
    {
        return replaceAllPlanNodesMatching(planNode, nodeToReplace, predicate, x -> true);
    }

    public static PlanNode replaceAllPlanNodesMatching(
            PlanNode node,
            PlanNode nodeToReplace,
            Predicate<PlanNode> predicate,
            Predicate<PlanNode> recursionPredicate)
    {
        if (predicate.test(node)) {
            return nodeToReplace;
        }
        if (recursionPredicate.test(node)) {
            List<PlanNode> sources = node.getSources().stream()
                    .map(source -> replaceAllPlanNodesMatching(source, nodeToReplace, predicate, recursionPredicate))
                    .collect(toImmutableList());
            return replaceChildren(node, sources);
        }
        return node;
    }
}
