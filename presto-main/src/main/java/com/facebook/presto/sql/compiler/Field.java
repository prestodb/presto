package com.facebook.presto.sql.compiler;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;

public class Field
{
    private final QualifiedName name;
    private final TupleInfo.Type type;

    public Field(QualifiedName name, TupleInfo.Type type)
    {
        this.name = name;
        this.type = type;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public TupleInfo.Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return name + ":" + type;
    }

    public static Function<Field, QualifiedName> nameGetter()
    {
        return new Function<Field, QualifiedName>()
        {
            @Override
            public QualifiedName apply(Field input)
            {
                return input.getName();
            }
        };
    }
}
