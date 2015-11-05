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

import com.facebook.presto.spi.security.Privilege;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PrivilegeNode
        extends Node
{
    private final Privilege privilege;

    public PrivilegeNode(Privilege privilege)
    {
        this(Optional.empty(), privilege);
    }

    public PrivilegeNode(NodeLocation location, Privilege privilege)
    {
        this(Optional.of(location), privilege);
    }

    private PrivilegeNode(Optional<NodeLocation> location, Privilege privilege)
    {
        super(location);
        this.privilege = requireNonNull(privilege, "privilege is null");
    }

    public Privilege getPrivilege()
    {
        return privilege;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPrivilegeNode(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(privilege);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || (getClass() != obj.getClass())) {
            return false;
        }
        PrivilegeNode o = (PrivilegeNode) obj;
        return Objects.equals(privilege, o.privilege);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("privilege", privilege)
                .toString();
    }
}
