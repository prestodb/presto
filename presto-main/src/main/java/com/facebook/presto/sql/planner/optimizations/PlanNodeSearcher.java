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
import static java.util.Objects.requireNonNull;

public class PlanNodeSearcher
{
    public static PlanNodeSearcher searchFrom(PlanNode node)
    {
        return new PlanNodeSearcher(node);
    }

    private final PlanNode node;
    private Predicate<PlanNode> where = alwaysTrue();
    private Predicate<PlanNode> skipOnly = alwaysTrue();

    public PlanNodeSearcher(PlanNode node)
    {
        this.node = requireNonNull(node, "node is null");
    }

    public PlanNodeSearcher where(Predicate<PlanNode> where)
    {
        this.where = requireNonNull(where, "where is null");
        return this;
    }

    public PlanNodeSearcher skipOnlyWhen(Predicate<PlanNode> skipOnly)
    {
        this.skipOnly = requireNonNull(skipOnly, "skipOnly is null");
        return this;
    }

    public <T extends PlanNode> Optional<T> findFirst()
    {
        return findFirstRecursive(node);
    }

    private <T extends PlanNode> Optional<T> findFirstRecursive(PlanNode node)
    {
        if (where.test(node)) {
            return Optional.of((T) node);
        }
        if (skipOnly.test(node)) {
            for (PlanNode source : node.getSources()) {
                Optional<T> found = findFirstRecursive(source);
                if (found.isPresent()) {
                    return found;
                }
            }
        }
        return Optional.empty();
    }

    public PlanNode removeFirst()
    {
        return removeFirstRecursive(node);
    }

    public PlanNode removeFirstRecursive(PlanNode node)
    {
        if (where.test(node)) {
            checkArgument(
                    node.getSources().size() == 1,
                    "Unable to remove plan node as it contains many or none children");
            return node.getSources().get(0);
        }
        if (skipOnly.test(node)) {
            List<PlanNode> sources = node.getSources().stream()
                    .map(source -> removeFirstRecursive(source))
                    .collect(toImmutableList());
            return replaceChildren(node, sources);
        }
        return node;
    }

    public PlanNode replaceAll(PlanNode newPlanNode)
    {
        return replaceAllRecursive(node, newPlanNode);
    }

    public PlanNode replaceAllRecursive(PlanNode node, PlanNode nodeToReplace)
    {
        if (where.test(node)) {
            return nodeToReplace;
        }
        if (skipOnly.test(node)) {
            List<PlanNode> sources = node.getSources().stream()
                    .map(source -> replaceAllRecursive(source, nodeToReplace))
                    .collect(toImmutableList());
            return replaceChildren(node, sources);
        }
        return node;
    }
}
