package com.facebook.presto.sql.compiler;

public class Slot
{
    private final int id;

    public Slot(int id)
    {
        this.id = id;
    }

    public String getName()
    {
        return "$" + id;
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
