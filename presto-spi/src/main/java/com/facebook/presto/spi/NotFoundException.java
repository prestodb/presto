package com.facebook.presto.spi;

public abstract class NotFoundException
        extends RuntimeException
{
    protected NotFoundException()
    {
    }

    protected NotFoundException(String message)
    {
        super(message);
    }

    protected NotFoundException(String message, Throwable cause)
    {
        super(message, cause);
    }

    protected NotFoundException(Throwable cause)
    {
        super(cause);
    }

    protected NotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
