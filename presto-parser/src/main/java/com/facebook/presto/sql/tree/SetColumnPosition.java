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

import com.facebook.presto.spi.analyzer.UpdateInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * {@code ALTER TABLE ... ALTER COLUMN <column> FIRST | AFTER <column>}, which moves an existing column
 * within the table's column order. The position is required here, unlike the optional clause of
 * {@link AddColumn}, because moving a column to where it already is would be the whole statement.
 */
public class SetColumnPosition
        extends Statement
{
    private final QualifiedName table;
    private final Identifier column;
    private final ColumnPosition position;
    private final boolean tableExists;

    public SetColumnPosition(QualifiedName table, Identifier column, ColumnPosition position, boolean tableExists)
    {
        this(Optional.empty(), table, column, position, tableExists);
    }

    public SetColumnPosition(NodeLocation location, QualifiedName table, Identifier column, ColumnPosition position, boolean tableExists)
    {
        this(Optional.of(location), table, column, position, tableExists);
    }

    private SetColumnPosition(Optional<NodeLocation> location, QualifiedName table, Identifier column, ColumnPosition position, boolean tableExists)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.column = requireNonNull(column, "column is null");
        this.position = requireNonNull(position, "position is null");
        this.tableExists = tableExists;
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Identifier getColumn()
    {
        return column;
    }

    public ColumnPosition getPosition()
    {
        return position;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetColumnPosition(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(column);
        if (position instanceof ColumnPosition.After) {
            nodes.add(((ColumnPosition.After) position).getColumn());
        }
        return nodes.build();
    }

    @Override
    public UpdateInfo getUpdateInfo()
    {
        return new UpdateInfo("SET COLUMN POSITION", table.toString());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, column, position, tableExists);
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
        SetColumnPosition o = (SetColumnPosition) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(column, o.column) &&
                Objects.equals(position, o.position) &&
                Objects.equals(tableExists, o.tableExists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("column", column)
                .add("position", position)
                .add("tableExists", tableExists)
                .toString();
    }
}
