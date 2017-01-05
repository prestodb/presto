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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;

public final class GroupingSets
        extends GroupingElement
{
    private final List<List<QualifiedName>> sets;

    public GroupingSets(List<List<QualifiedName>> groupingSetList)
    {
        this(Optional.empty(), groupingSetList);
    }

    public GroupingSets(NodeLocation location, List<List<QualifiedName>> sets)
    {
        this(Optional.of(location), sets);
    }

    private GroupingSets(Optional<NodeLocation> location, List<List<QualifiedName>> sets)
    {
        super(location);
        requireNonNull(sets, "sets is null");
        checkArgument(!sets.isEmpty(), "grouping sets cannot be empty");
        this.sets = ImmutableList.copyOf(sets.stream().map(ImmutableList::copyOf).collect(Collectors.toList()));
    }

    public List<List<QualifiedName>> getSets()
    {
        return sets;
    }

    @Override
    public List<Set<Expression>> enumerateGroupingSets()
    {
        return sets.stream()
                .map(groupingSet -> groupingSet.stream()
                        .map(QualifiedNameReference::new)
                        .collect(Collectors.<Expression>toSet()))
                .collect(collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupingSets(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupingSets groupingSets = (GroupingSets) o;
        return Objects.equals(sets, groupingSets.sets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sets);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sets", sets)
                .toString();
    }
}
