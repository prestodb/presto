package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class FunctionCall
        extends Expression
{
    private final QualifiedName name;
    private final boolean distinct;
    private final List<Expression> arguments;

    public FunctionCall(String name)
    {
        this(name, ImmutableList.<Expression>of());
    }

    public FunctionCall(String name, List<Expression> arguments)
    {
        this(new QualifiedName(name), arguments);
    }

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
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("distinct", distinct)
                .add("arguments", arguments)
                .toString();
    }
}
