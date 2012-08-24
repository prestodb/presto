package com.facebook.presto;

import java.io.IOException;

public class RuntimeIOException extends RuntimeException
{
    public RuntimeIOException(IOException cause)
    {
        super(cause);
    }
}
