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

public class FunctionReference
        extends Expression
{
    private final QualifiedName name;
    private final List<Expression> arguments;

    public FunctionReference(QualifiedName name, List<Expression> arguments)
    {
        super(Optional.empty());
        this.name = requireNonNull(name, "name is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return arguments;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFunctionReference(this, context);
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
        FunctionReference that = (FunctionReference) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, arguments);
    }
}
