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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FunctionCall
        extends Expression
{
    private final QualifiedName name;
    private final Optional<Window> window;
    private final Optional<Expression> filter;
    private final boolean distinct;
    private final List<Expression> arguments;

    public FunctionCall(QualifiedName name, List<Expression> arguments)
    {
        this(Optional.empty(), name, Optional.empty(),  Optional.empty(), false, arguments);
    }

    public FunctionCall(NodeLocation location, QualifiedName name, List<Expression> arguments)
    {
        this(Optional.of(location), name, Optional.empty(), Optional.empty(), false, arguments);
    }

    public FunctionCall(QualifiedName name, boolean distinct, List<Expression> arguments)
    {
        this(Optional.empty(), name, Optional.empty(), Optional.empty(), distinct, arguments);
    }

    public FunctionCall(NodeLocation location, QualifiedName name, boolean distinct, List<Expression> arguments)
    {
        this(Optional.of(location), name, Optional.empty(), Optional.empty(), distinct, arguments);
    }

    public FunctionCall(QualifiedName name, Optional<Window> window, boolean distinct, List<Expression> arguments)
    {
        this(Optional.empty(), name, window, Optional.empty(), distinct, arguments);
    }

    public FunctionCall(QualifiedName name, Optional<Window> window, Optional<Expression> filter, boolean distinct, List<Expression> arguments)
    {
        this(Optional.empty(), name, window, filter, distinct, arguments);
    }

    public FunctionCall(NodeLocation location, QualifiedName name, Optional<Window> window, Optional<Expression> filter, boolean distinct, List<Expression> arguments)
    {
        this(Optional.of(location), name, window, filter, distinct, arguments);
    }

    private FunctionCall(Optional<NodeLocation> location, QualifiedName name, Optional<Window> window, Optional<Expression> filter, boolean distinct, List<Expression> arguments)
    {
        super(location);
        requireNonNull(name, "name is null");
        requireNonNull(window, "window is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(arguments, "arguments is null");

        this.name = name;
        this.window = window;
        this.distinct = distinct;
        this.arguments = arguments;
        this.filter = filter;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Optional<Window> getWindow()
    {
        return window;
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    public Optional<Expression> getFilter()
    {
        return filter;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFunctionCall(this, context);
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
        FunctionCall o = (FunctionCall) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(window, o.window) &&
                Objects.equals(filter, o.filter) &&
                Objects.equals(distinct, o.distinct) &&
                Objects.equals(arguments, o.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, distinct, window, filter, arguments);
    }
}
