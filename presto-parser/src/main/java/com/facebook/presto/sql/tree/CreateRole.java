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

public class CreateRole
        extends Statement
{
    private final Identifier name;
    private final Optional<GrantorSpecification> grantor;

    public CreateRole(Identifier name, Optional<GrantorSpecification> grantor)
    {
        this(Optional.empty(), name, grantor);
    }

    public CreateRole(NodeLocation location, Identifier name, Optional<GrantorSpecification> grantor)
    {
        this(Optional.of(location), name, grantor);
    }

    private CreateRole(Optional<NodeLocation> location, Identifier name, Optional<GrantorSpecification> grantor)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.grantor = requireNonNull(grantor, "grantor is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public Optional<GrantorSpecification> getGrantor()
    {
        return grantor;
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
        CreateRole that = (CreateRole) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(grantor, that.grantor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, grantor);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("grantor", grantor)
                .toString();
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateRole(this, context);
    }
}
