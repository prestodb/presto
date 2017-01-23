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

public class Execute
        extends Statement
{
    private final String name;
    private final List<Expression> parameters;

    public Execute(NodeLocation location, String name, List<Expression> parameters)
    {
        this(Optional.of(location), name, parameters);
    }

    public Execute(String name, List<Expression> parameters)
    {
        this(Optional.empty(), name, parameters);
    }

    private Execute(Optional<NodeLocation> location, String name, List<Expression> parameters)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.parameters = requireNonNull(ImmutableList.copyOf(parameters), "parameters is null");
    }

    public String getName()
    {
        return name;
    }

    public List<Expression> getParameters()
    {
        return parameters;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExecute(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return parameters;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameters);
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
        Execute o = (Execute) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(parameters, o.parameters);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameters", parameters)
                .toString();
    }
}
