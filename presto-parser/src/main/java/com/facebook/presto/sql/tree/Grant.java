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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Grant
        extends Statement
{
    private final PrestoPrivilege prestoPrivilege;
    private final boolean table;
    private final QualifiedName tableName;
    private final PrestoIdentity prestoIdentity;
    private final boolean option;

    public Grant(PrestoPrivilege prestoPrivilege, boolean table, QualifiedName tableName, PrestoIdentity prestoIdentity, boolean option)
    {
        this.prestoPrivilege = requireNonNull(prestoPrivilege, "privilege is null");
        this.table = table;
        this.tableName = requireNonNull(tableName, "table name is null");
        this.prestoIdentity = requireNonNull(prestoIdentity, "user/role is null");
        this.option = option;
    }

    public PrestoPrivilege getPrestoPrivilege()
    {
        return prestoPrivilege;
    }

    public boolean isTable()
    {
        return table;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public PrestoIdentity getPrestoIdentity()
    {
        return prestoIdentity;
    }

    public boolean isOption()
    {
        return option;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGrant(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(prestoPrivilege, table, tableName, prestoIdentity, option);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Grant o = (Grant) obj;
        return Objects.equals(prestoPrivilege, o.prestoPrivilege) &&
                Objects.equals(table, o.table) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(prestoIdentity, o.prestoIdentity) &&
                Objects.equals(option, o.option);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("prestoPrivilege", prestoPrivilege)
                .add("table", table)
                .add("tableName", tableName)
                .add("prestoIdentity", prestoIdentity)
                .add("option", option)
                .toString();
    }
}
