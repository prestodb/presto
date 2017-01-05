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

public class ShowCreate
        extends Statement
{
    public enum Type {
        TABLE,
        VIEW
    }

    private final Type type;
    private final QualifiedName name;

    public ShowCreate(Type type, QualifiedName name)
    {
        this(Optional.empty(), type, name);
    }

    public ShowCreate(NodeLocation location, Type type, QualifiedName name)
    {
        this(Optional.of(location), type, name);
    }

    private ShowCreate(Optional<NodeLocation> location, Type type, QualifiedName name)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.name = requireNonNull(name, "name is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowCreate(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name);
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
        ShowCreate o = (ShowCreate) obj;
        return Objects.equals(name, o.name) && Objects.equals(type, o.type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .toString();
    }
}
