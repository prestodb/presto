package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import java.util.List;

public class FunctionCall
        extends Expression
{
    private final QualifiedName name;
    private final boolean distinct;
    private final List<Expression> arguments;

    public FunctionCall(QualifiedName name, List<Expression> arguments)
    {
        this(name, false, arguments);
    }

    public FunctionCall(QualifiedName name, boolean distinct, List<Expression> arguments)
    {
        this.name = name;
        this.distinct = distinct;
        this.arguments = arguments;
    }

    public QualifiedName getName()
    {
        return name;
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
                .add("distinct", distinct)
                .add("arguments", arguments)
                .toString();
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

        FunctionCall that = (FunctionCall) o;

        if (distinct != that.distinct) {
            return false;
        }
        if (!arguments.equals(that.arguments)) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name.hashCode();
        result = 31 * result + (distinct ? 1 : 0);
        result = 31 * result + arguments.hashCode();
        return result;
    }

}
