package com.facebook.presto.client;

import static com.google.common.base.Preconditions.checkNotNull;

public class Failure
        extends RuntimeException
{
    private final String type;

    Failure(String type, String message, Failure cause)
    {
        super(message, cause, true, true);
        this.type = checkNotNull(type, "type is null");
    }

    public String getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        String message = getMessage();
        if (message != null) {
            return type + ": " + message;
        }
        return type;
    }
}
