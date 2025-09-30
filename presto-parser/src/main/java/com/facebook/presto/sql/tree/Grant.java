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
import static java.util.Objects.requireNonNull;

public class Grant
        extends Statement
{
    private final Optional<List<String>> privileges; // missing means ALL PRIVILEGES
    private final boolean table;
    private final QualifiedName tableName;
    private final PrincipalSpecification grantee;
    private final boolean withGrantOption;

    public Grant(Optional<List<String>> privileges, boolean table, QualifiedName tableName, PrincipalSpecification grantee, boolean withGrantOption)
    {
        this(Optional.empty(), privileges, table, tableName, grantee, withGrantOption);
    }

    public Grant(NodeLocation location, Optional<List<String>> privileges, boolean table, QualifiedName tableName, PrincipalSpecification grantee, boolean withGrantOption)
    {
        this(Optional.of(location), privileges, table, tableName, grantee, withGrantOption);
    }

    private Grant(Optional<NodeLocation> location, Optional<List<String>> privileges, boolean table, QualifiedName tableName, PrincipalSpecification grantee, boolean withGrantOption)
    {
        super(location);
        requireNonNull(privileges, "privileges is null");
        this.privileges = privileges.map(ImmutableList::copyOf);
        this.table = table;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.grantee = requireNonNull(grantee, "grantee is null");
        this.withGrantOption = withGrantOption;
    }

    public Optional<List<String>> getPrivileges()
    {
        return privileges;
    }

    public boolean isTable()
    {
        return table;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public PrincipalSpecification getGrantee()
    {
        return grantee;
    }

    public boolean isWithGrantOption()
    {
        return withGrantOption;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGrant(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(privileges, table, tableName, grantee, withGrantOption);
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
        return Objects.equals(privileges, o.privileges) &&
                Objects.equals(table, o.table) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(grantee, o.grantee) &&
                Objects.equals(withGrantOption, o.withGrantOption);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("privileges", privileges)
                .add("table", table)
                .add("tableName", tableName)
                .add("grantee", grantee)
                .add("withGrantOption", withGrantOption)
                .toString();
    }
}
