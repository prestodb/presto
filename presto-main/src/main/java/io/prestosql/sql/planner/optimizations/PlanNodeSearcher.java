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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.planner.iterative.Lookup.noLookup;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;

public class PlanNodeSearcher
{
    public static PlanNodeSearcher searchFrom(PlanNode node)
    {
        return searchFrom(node, noLookup());
    }

    /**
     * Use it in optimizer {@link io.prestosql.sql.planner.iterative.Rule} only if you truly do not have a better option
     * <p>
     * TODO: replace it with a support for plan (physical) properties in rules pattern matching
     */
    public static PlanNodeSearcher searchFrom(PlanNode node, Lookup lookup)
    {
        return new PlanNodeSearcher(node, lookup);
    }

    private final PlanNode node;
    private final Lookup lookup;
    private Predicate<PlanNode> where = alwaysTrue();
    private Predicate<PlanNode> recurseOnlyWhen = alwaysTrue();

    private PlanNodeSearcher(PlanNode node, Lookup lookup)
    {
        this.node = requireNonNull(node, "node is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    public PlanNodeSearcher where(Predicate<PlanNode> where)
    {
        this.where = requireNonNull(where, "where is null");
        return this;
    }

    public PlanNodeSearcher recurseOnlyWhen(Predicate<PlanNode> skipOnly)
    {
        this.recurseOnlyWhen = requireNonNull(skipOnly, "recurseOnlyWhen is null");
        return this;
    }

    public <T extends PlanNode> Optional<T> findFirst()
    {
        return findFirstRecursive(node);
    }

    private <T extends PlanNode> Optional<T> findFirstRecursive(PlanNode node)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            return Optional.of((T) node);
        }
        if (recurseOnlyWhen.test(node)) {
            for (PlanNode source : node.getSources()) {
                Optional<T> found = findFirstRecursive(source);
                if (found.isPresent()) {
                    return found;
                }
            }
        }
        return Optional.empty();
    }

    public <T extends PlanNode> Optional<T> findSingle()
    {
        List<T> all = findAll();
        switch (all.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(all.get(0));
            default:
                throw new IllegalStateException("Multiple nodes found");
        }
    }

    public <T extends PlanNode> List<T> findAll()
    {
        ImmutableList.Builder<T> nodes = ImmutableList.builder();
        findAllRecursive(node, nodes);
        return nodes.build();
    }

    public <T extends PlanNode> T findOnlyElement()
    {
        return getOnlyElement(findAll());
    }

    public <T extends PlanNode> T findOnlyElement(T defaultValue)
    {
        List<T> all = findAll();
        if (all.size() == 0) {
            return defaultValue;
        }
        return getOnlyElement(all);
    }

    private <T extends PlanNode> void findAllRecursive(PlanNode node, ImmutableList.Builder<T> nodes)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            nodes.add((T) node);
        }
        if (recurseOnlyWhen.test(node)) {
            for (PlanNode source : node.getSources()) {
                findAllRecursive(source, nodes);
            }
        }
    }

    public PlanNode removeAll()
    {
        return removeAllRecursive(node);
    }

    private PlanNode removeAllRecursive(PlanNode node)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            checkArgument(
                    node.getSources().size() == 1,
                    "Unable to remove plan node as it contains 0 or more than 1 children");
            return node.getSources().get(0);
        }
        if (recurseOnlyWhen.test(node)) {
            List<PlanNode> sources = node.getSources().stream()
                    .map(this::removeAllRecursive)
                    .collect(toImmutableList());
            return replaceChildren(node, sources);
        }
        return node;
    }

    public PlanNode removeFirst()
    {
        return removeFirstRecursive(node);
    }

    private PlanNode removeFirstRecursive(PlanNode node)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            checkArgument(
                    node.getSources().size() == 1,
                    "Unable to remove plan node as it contains 0 or more than 1 children");
            return node.getSources().get(0);
        }
        if (recurseOnlyWhen.test(node)) {
            List<PlanNode> sources = node.getSources();
            if (sources.isEmpty()) {
                return node;
            }
            else if (sources.size() == 1) {
                return replaceChildren(node, ImmutableList.of(removeFirstRecursive(sources.get(0))));
            }
            else {
                throw new IllegalArgumentException("Unable to remove first node when a node has multiple children, use removeAll instead");
            }
        }
        return node;
    }

    public PlanNode replaceAll(PlanNode newPlanNode)
    {
        return replaceAllRecursive(node, newPlanNode);
    }

    private PlanNode replaceAllRecursive(PlanNode node, PlanNode nodeToReplace)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            return nodeToReplace;
        }
        if (recurseOnlyWhen.test(node)) {
            List<PlanNode> sources = node.getSources().stream()
                    .map(source -> replaceAllRecursive(source, nodeToReplace))
                    .collect(toImmutableList());
            return replaceChildren(node, sources);
        }
        return node;
    }

    public PlanNode replaceFirst(PlanNode newPlanNode)
    {
        return replaceFirstRecursive(node, newPlanNode);
    }

    private PlanNode replaceFirstRecursive(PlanNode node, PlanNode nodeToReplace)
    {
        node = lookup.resolve(node);

        if (where.test(node)) {
            return nodeToReplace;
        }
        List<PlanNode> sources = node.getSources();
        if (sources.isEmpty()) {
            return node;
        }
        else if (sources.size() == 1) {
            return replaceChildren(node, ImmutableList.of(replaceFirstRecursive(node, sources.get(0))));
        }
        else {
            throw new IllegalArgumentException("Unable to replace first node when a node has multiple children, use replaceAll instead");
        }
    }

    public boolean matches()
    {
        return findFirst().isPresent();
    }

    public int count()
    {
        return findAll().size();
    }
}
