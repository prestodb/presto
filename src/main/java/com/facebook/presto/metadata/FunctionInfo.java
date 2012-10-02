package com.facebook.presto.metadata;

public class FunctionInfo
{
    private boolean isAggregate;

    public FunctionInfo(boolean aggregate)
    {
        isAggregate = aggregate;
    }

    public boolean isAggregate()
    {
        return isAggregate;
    }
}
