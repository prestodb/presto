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

import static java.util.Objects.requireNonNull;

public class GenericDataType
        extends Expression
{
    private final Identifier name;
    private final List<DataTypeParameter> arguments;

    public GenericDataType(NodeLocation location, Identifier name, List<DataTypeParameter> arguments)
    {
        super(Optional.of(location));
        this.name = requireNonNull(name, "name is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public List<DataTypeParameter> getArguments()
    {
        return arguments;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(name)
                .addAll(arguments)
                .build();
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGenericDataType(this, context);
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
        GenericDataType that = (GenericDataType) o;
        return name.equals(that.name) &&
                arguments.equals(that.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, arguments);
    }
}
