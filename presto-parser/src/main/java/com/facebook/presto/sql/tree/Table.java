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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Table
        extends QueryBody
{
    private final QualifiedName name;
    private final boolean isPlaceholderForRLS;

    public Table(QualifiedName name)
    {
        this(Optional.empty(), name, false);
    }

    public Table(QualifiedName name, boolean isPlaceholderForRLS)
    {
        this(Optional.empty(), name, isPlaceholderForRLS);
    }

    public Table(NodeLocation location, QualifiedName name)
    {
        this(Optional.of(location), name, false);
    }

    private Table(Optional<NodeLocation> location, QualifiedName name, boolean isPlaceholderForRLS)
    {
        super(location);
        this.name = name;
        this.isPlaceholderForRLS = isPlaceholderForRLS;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public boolean isPlaceholderForRLS()
    {
        return isPlaceholderForRLS;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTable(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(name)
                .toString();
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

        Table table = (Table) o;
        return (Objects.equals(name, table.name) && Objects.equals(isPlaceholderForRLS, table.isPlaceholderForRLS));
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
