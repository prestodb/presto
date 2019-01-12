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

public class ShowGrants
        extends Statement
{
    private final boolean table;
    private final Optional<QualifiedName> tableName;

    public ShowGrants(boolean table, Optional<QualifiedName> tableName)
    {
        this(Optional.empty(), table, tableName);
    }

    public ShowGrants(NodeLocation location, boolean table, Optional<QualifiedName> tableName)
    {
        this(Optional.of(location), table, tableName);
    }

    public ShowGrants(Optional<NodeLocation> location, boolean table, Optional<QualifiedName> tableName)
    {
        super(location);
        requireNonNull(tableName, "tableName is null");

        this.table = table;
        this.tableName = tableName;
    }

    public boolean getTable()
    {
        return table;
    }

    public Optional<QualifiedName> getTableName()
    {
        return tableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowGrants(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tableName);
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
        ShowGrants o = (ShowGrants) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(tableName, o.tableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("tableName", tableName)
                .toString();
    }
}
