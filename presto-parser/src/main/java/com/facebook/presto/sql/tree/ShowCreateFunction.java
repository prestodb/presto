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

public class ShowCreateFunction
        extends Statement
{
    private final QualifiedName name;
    private final Optional<List<String>> parameterTypes;

    public ShowCreateFunction(QualifiedName name, Optional<List<String>> parameterTypes)
    {
        this(Optional.empty(), name, parameterTypes);
    }

    public ShowCreateFunction(NodeLocation location, QualifiedName name, Optional<List<String>> parameterTypes)
    {
        this(Optional.of(location), name, parameterTypes);
    }

    private ShowCreateFunction(Optional<NodeLocation> location, QualifiedName name, Optional<List<String>> parameterTypes)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Optional<List<String>> getParameterTypes()
    {
        return parameterTypes;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowCreateFunction(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameterTypes);
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
        ShowCreateFunction o = (ShowCreateFunction) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(parameterTypes, o.parameterTypes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameterTypes", parameterTypes)
                .toString();
    }
}
