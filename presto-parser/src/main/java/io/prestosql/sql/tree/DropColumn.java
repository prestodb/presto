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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DropColumn
        extends Statement
{
    private final QualifiedName table;
    private final Identifier column;

    public DropColumn(QualifiedName table, Identifier column)
    {
        this(Optional.empty(), table, column);
    }

    public DropColumn(NodeLocation location, QualifiedName table, Identifier column)
    {
        this(Optional.of(location), table, column);
    }

    private DropColumn(Optional<NodeLocation> location, QualifiedName table, Identifier column)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.column = requireNonNull(column, "column is null");
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Identifier getColumn()
    {
        return column;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropColumn(this, context);
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
        DropColumn that = (DropColumn) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(column, that.column);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, column);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("column", column)
                .toString();
    }
}
