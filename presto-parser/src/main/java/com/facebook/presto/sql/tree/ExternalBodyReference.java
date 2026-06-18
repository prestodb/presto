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

import static com.google.common.base.MoreObjects.ToStringHelper;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ExternalBodyReference
        extends RoutineBody
{
    private final Optional<Identifier> identifier;

    public ExternalBodyReference()
    {
        this(Optional.empty(), Optional.empty());
    }

    public ExternalBodyReference(Identifier identifier)
    {
        this(Optional.empty(), Optional.of(identifier));
    }

    private ExternalBodyReference(Optional<NodeLocation> location, Optional<Identifier> identifier)
    {
        super(location);
        this.identifier = requireNonNull(identifier, "identifier is null");
    }

    public Optional<Identifier> getIdentifier()
    {
        return identifier;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExternalBodyReference(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        if (identifier.isPresent()) {
            return ImmutableList.of(identifier.get());
        }
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identifier);
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
        ExternalBodyReference o = (ExternalBodyReference) obj;
        return Objects.equals(identifier, o.identifier);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this);
        identifier.ifPresent(value -> helper.add("identifier", value));
        return helper.toString();
    }
}
