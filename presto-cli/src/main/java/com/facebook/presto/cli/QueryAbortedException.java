package com.facebook.presto.cli;

import java.io.IOException;

public class QueryAbortedException
        extends IOException
{
    public QueryAbortedException(Throwable cause)
    {
        super(cause);
    }
}
