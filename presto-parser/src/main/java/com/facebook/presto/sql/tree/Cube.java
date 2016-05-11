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
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class Cube
        extends GroupingElement
{
    private final List<QualifiedName> columns;

    public Cube(List<QualifiedName> columns)
    {
        this(Optional.empty(), columns);
    }

    public Cube(NodeLocation location, List<QualifiedName> columns)
    {
        this(Optional.of(location), columns);
    }

    private Cube(Optional<NodeLocation> location, List<QualifiedName> columns)
    {
        super(location);
        requireNonNull(columns, "columns is null");
        this.columns = columns;
    }

    public List<QualifiedName> getColumns()
    {
        return columns;
    }

    @Override
    public List<Set<Expression>> enumerateGroupingSets()
    {
        return ImmutableList.copyOf(Sets.powerSet(columns.stream()
                .map(QualifiedNameReference::new)
                .collect(toSet())));
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCube(this, context);
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
        Cube cube = (Cube) o;
        return Objects.equals(columns, cube.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .toString();
    }
}
