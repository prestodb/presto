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

public class SetRole
        extends Statement
{
    public enum Type
    {
        ROLE, ALL, NONE
    }

    private final Type type;
    private final Optional<String> role;
    private final Optional<String> catalog;

    public SetRole(Type type, Optional<String> role, Optional<String> catalog)
    {
        this(Optional.empty(), type, role, catalog);
    }

    public SetRole(NodeLocation location, Type type, Optional<String> role, Optional<String> catalog)
    {
        this(Optional.of(location), type, role, catalog);
    }

    private SetRole(Optional<NodeLocation> location, Type type, Optional<String> role, Optional<String> catalog)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.role = requireNonNull(role, "role is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    public Type getType()
    {
        return type;
    }

    public Optional<String> getRole()
    {
        return role;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetRole(this, context);
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
        SetRole setRole = (SetRole) o;
        return type == setRole.type &&
                Objects.equals(role, setRole.role) &&
                Objects.equals(catalog, setRole.catalog);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, role, catalog);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("role", role)
                .add("catalog", catalog)
                .toString();
    }
}
