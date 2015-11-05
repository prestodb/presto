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
    private final List<PrivilegeNode> privilegeNodes;
    private final boolean table;
    private final QualifiedName tableName;
    private final IdentityNode identityNode;
    private final boolean withGrantOption;

    public Grant(List<PrivilegeNode> privilegeNodes, boolean table, QualifiedName tableName, IdentityNode identityNode, boolean withGrantOption)
    {
        this(Optional.empty(), privilegeNodes, table, tableName, identityNode, withGrantOption);
    }

    public Grant(NodeLocation location, List<PrivilegeNode> privilegeNodes, boolean table, QualifiedName tableName, IdentityNode identityNode, boolean withGrantOption)
    {
        this(Optional.of(location), privilegeNodes, table, tableName, identityNode, withGrantOption);
    }
    private Grant(Optional<NodeLocation> location, List<PrivilegeNode> privilegeNodes, boolean table, QualifiedName tableName, IdentityNode identityNode, boolean withGrantOption)
    {
        super(location);
        this.privilegeNodes = ImmutableList.copyOf(requireNonNull(privilegeNodes, "privilegeNodes is null"));
        this.table = table;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.identityNode = requireNonNull(identityNode, "identityNode is null");
        this.withGrantOption = withGrantOption;
    }

    public List<PrivilegeNode> getPrivilegeNodes()
    {
        return privilegeNodes;
    }

    public boolean isTable()
    {
        return table;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public IdentityNode getIdentityNode()
    {
        return identityNode;
    }

    public boolean isOption()
    {
        return withGrantOption;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGrant(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(privilegeNodes, table, tableName, identityNode, withGrantOption);
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
        return Objects.equals(privilegeNodes, o.privilegeNodes) &&
                Objects.equals(table, o.table) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(identityNode, o.identityNode) &&
                Objects.equals(withGrantOption, o.withGrantOption);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("privilegeNodes", privilegeNodes)
                .add("table", table)
                .add("tableName", tableName)
                .add("identityNode", identityNode)
                .add("withGrantOption", withGrantOption)
                .toString();
    }
}
