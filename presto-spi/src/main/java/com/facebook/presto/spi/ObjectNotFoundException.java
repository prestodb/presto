package com.facebook.presto.spi;

public class ObjectNotFoundException
        extends Exception
{
    public ObjectNotFoundException(String message)
    {
        super(message);
    }
}
