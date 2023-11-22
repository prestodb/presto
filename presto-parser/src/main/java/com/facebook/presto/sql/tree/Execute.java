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
    private final Identifier name;
    private final List<Expression> parameters;
    private final boolean batchExecution;

    public Execute(NodeLocation location, Identifier name, List<Expression> parameters, boolean batchExecution)
    {
        this(Optional.of(location), name, parameters, batchExecution);
    }

    public Execute(Identifier name, List<Expression> parameters, boolean batchExecution)
    {
        this(Optional.empty(), name, parameters, batchExecution);
    }

    private Execute(Optional<NodeLocation> location, Identifier name, List<Expression> parameters, boolean batchExecution)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.parameters = requireNonNull(ImmutableList.copyOf(parameters), "parameters is null");
        this.batchExecution = batchExecution;
    }

    public Identifier getName()
    {
        return name;
    }

    public List<Expression> getParameters()
    {
        return parameters;
    }

    public boolean isBatchExecution()
    {
        return batchExecution;
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
        return Objects.hash(name, parameters, batchExecution);
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
                Objects.equals(parameters, o.parameters) &&
                batchExecution == o.batchExecution;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameters", parameters)
                .add("batchExecution", batchExecution)
                .toString();
    }
}
