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

public class Delete
        extends Statement
{
    private final Table table;
    private final Optional<Expression> where;
    private final boolean triggeredByRefresh;

    public Delete(Table table, Optional<Expression> where)
    {
        this(Optional.empty(), table, where, false);
    }

    public Delete(Table table, Optional<Expression> where, boolean triggeredByRefresh)
    {
        this(Optional.empty(), table, where, triggeredByRefresh);
    }

    public Delete(NodeLocation location, Table table, Optional<Expression> where)
    {
        this(Optional.of(location), table, where, false);
    }

    private Delete(Optional<NodeLocation> location, Table table, Optional<Expression> where, boolean triggeredByRefresh)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.where = requireNonNull(where, "where is null");
        this.triggeredByRefresh = triggeredByRefresh;
    }

    public Table getTable()
    {
        return table;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public boolean isTriggeredByRefresh()
    {
        return triggeredByRefresh;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDelete(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, where, triggeredByRefresh);
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
        Delete o = (Delete) obj;
        return Objects.equals(table, o.table) &&
                Objects.equals(where, o.where) &&
                Objects.equals(triggeredByRefresh, o.triggeredByRefresh);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table.getName())
                .add("where", where)
                .add("triggeredByRefresh", triggeredByRefresh)
                .toString();
    }
}
