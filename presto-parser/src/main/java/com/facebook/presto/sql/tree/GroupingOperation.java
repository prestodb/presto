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
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;

public class GroupingOperation
        extends Expression
{
    private final List<QualifiedName> groupingColumns;

    public GroupingOperation(Optional<NodeLocation> location, List<QualifiedName> groupingColumns)
    {
        super(location);
        requireNonNull(groupingColumns);
        checkArgument(!groupingColumns.isEmpty(), "grouping operation columns cannot be empty");
        this.groupingColumns = ImmutableList.copyOf(groupingColumns);
    }

    public List<QualifiedName> getColumns()
    {
        return groupingColumns;
    }

    public List<Expression> getGroupingColumns()
    {
        return groupingColumns.stream()
                .map(DereferenceExpression::from)
                .collect(collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupingOperation(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
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
        GroupingOperation other = (GroupingOperation) o;
        return Objects.equals(groupingColumns, other.groupingColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(groupingColumns);
    }
}
