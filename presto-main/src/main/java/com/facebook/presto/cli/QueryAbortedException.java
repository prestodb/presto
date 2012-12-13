package com.facebook.presto.cli;

public class QueryAbortedException
    extends RuntimeException
{
    public QueryAbortedException(Throwable cause)
    {
        super(cause);
    }
}
