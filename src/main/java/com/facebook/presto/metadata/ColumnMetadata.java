package com.facebook.presto.metadata;


import com.facebook.presto.TupleInfo;

public class ColumnMetadata
{
    private final String name;
    private final TupleInfo.Type type;

    public ColumnMetadata(TupleInfo.Type type, String name)
    {
        this.type = type;
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public TupleInfo.Type getType()
    {
        return type;
    }
}
