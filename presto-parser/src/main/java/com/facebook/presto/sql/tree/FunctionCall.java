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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import java.util.List;

public class FunctionCall
        extends Expression
{
    private final QualifiedName name;
    private final Optional<Window> window;
    private final boolean distinct;
    private final List<Expression> arguments;

    public FunctionCall(QualifiedName name, List<Expression> arguments)
    {
        this(name, null, false, arguments);
    }

    public FunctionCall(QualifiedName name, Window window, boolean distinct, List<Expression> arguments)
    {
        this.name = name;
        this.window = Optional.fromNullable(window);
        this.distinct = distinct;
        this.arguments = arguments;
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
        return Objects.equal(name, o.name) &&
                Objects.equal(window, o.window) &&
                Objects.equal(distinct, o.distinct) &&
                Objects.equal(arguments, o.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, distinct, window, arguments);
    }

    public static Function<FunctionCall, List<Expression>> argumentsGetter()
    {
        return new Function<FunctionCall, List<Expression>>()
        {
            @Override
            public List<Expression> apply(FunctionCall input)
            {
                return input.getArguments();
            }
        };
    }

    public static Predicate<FunctionCall> distinctPredicate()
    {
        return new Predicate<FunctionCall>()
        {
            @Override
            public boolean apply(FunctionCall input)
            {
                return input.isDistinct();
            }
        };
    }
}
