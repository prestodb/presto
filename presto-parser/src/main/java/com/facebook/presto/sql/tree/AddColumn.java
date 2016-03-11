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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class AddColumn
        extends DataDefinitionStatement
{
    private final QualifiedName name;
    private final ColumnDefinition column;

    public AddColumn(QualifiedName name, ColumnDefinition column)
    {
        this(Optional.empty(), name, column);
    }

    public AddColumn(NodeLocation location, QualifiedName name, ColumnDefinition column)
    {
        this(Optional.of(location), name, column);
    }

    private AddColumn(Optional<NodeLocation> location, QualifiedName name, ColumnDefinition column)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.column = requireNonNull(column, "column is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public ColumnDefinition getColumn()
    {
        return column;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddColumn(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, column);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AddColumn o = (AddColumn) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(column, o.column);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("column", column)
                .toString();
    }
}
