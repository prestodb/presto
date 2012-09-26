package com.facebook.presto.sql.tree;

public class TableCorrelation
{
    private final String name;

    public TableCorrelation(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }
}
