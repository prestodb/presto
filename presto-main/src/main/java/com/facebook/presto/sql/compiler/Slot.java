package com.facebook.presto.sql.compiler;

public class Slot
{
    private final int id;
    private final Type type;

    public Slot(int id, Type type)
    {
        this.id = id;
        this.type = type;
    }

    public String getName()
    {
        return "$" + id;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
