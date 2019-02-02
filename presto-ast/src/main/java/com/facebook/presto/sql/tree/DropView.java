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

public class DropView
        extends Statement
{
    private final QualifiedName name;
    private final boolean exists;

    public DropView(QualifiedName name, boolean exists)
    {
        this(Optional.empty(), name, exists);
    }

    public DropView(NodeLocation location, QualifiedName name, boolean exists)
    {
        this(Optional.of(location), name, exists);
    }

    private DropView(Optional<NodeLocation> location, QualifiedName name, boolean exists)
    {
        super(location);
        this.name = name;
        this.exists = exists;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public boolean isExists()
    {
        return exists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropView(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, exists);
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
        DropView o = (DropView) obj;
        return Objects.equals(name, o.name)
                && (exists == o.exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("exists", exists)
                .toString();
    }
}
