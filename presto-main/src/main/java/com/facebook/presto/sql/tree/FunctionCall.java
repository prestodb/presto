package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("window", window)
                .add("distinct", distinct)
                .add("arguments", arguments)
                .toString();
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
}
