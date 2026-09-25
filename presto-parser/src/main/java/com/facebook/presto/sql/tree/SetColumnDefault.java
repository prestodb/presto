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

public class SetColumnDefault
        extends Statement
{
    private final QualifiedName table;
    private final Identifier column;
    private final Expression defaultExpression;
    private final boolean tableExists;

    public SetColumnDefault(QualifiedName table, Identifier column, Expression defaultExpression, boolean tableExists)
    {
        this(Optional.empty(), table, column, defaultExpression, tableExists);
    }

    public SetColumnDefault(NodeLocation location, QualifiedName table, Identifier column, Expression defaultExpression, boolean tableExists)
    {
        this(Optional.of(location), table, column, defaultExpression, tableExists);
    }

    private SetColumnDefault(Optional<NodeLocation> location, QualifiedName table, Identifier column, Expression defaultExpression, boolean tableExists)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.column = requireNonNull(column, "column is null");
        this.defaultExpression = requireNonNull(defaultExpression, "defaultExpression is null");
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

    public Expression getDefaultExpression()
    {
        return defaultExpression;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetColumnDefault(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(defaultExpression);
    }

    @Override
    public UpdateInfo getUpdateInfo()
    {
        return new UpdateInfo("SET COLUMN DEFAULT", table.getSuffix());
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
        SetColumnDefault that = (SetColumnDefault) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(column, that.column) &&
                Objects.equals(defaultExpression, that.defaultExpression) &&
                Objects.equals(tableExists, that.tableExists);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, column, defaultExpression, tableExists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("column", column)
                .add("defaultExpression", defaultExpression)
                .add("tableExists", tableExists)
                .toString();
    }
}
