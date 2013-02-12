/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.thrift.spi;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

@ThriftStruct
public class PrestoImporterException extends Exception
{
    @ThriftConstructor
    public PrestoImporterException(String message)
    {
        super(message);
    }

    public PrestoImporterException(String format, Object... args)
    {
        this(String.format(format, args));
    }

    public PrestoImporterException(Throwable cause, String message)
    {
        super(message, cause);
    }

    public PrestoImporterException(Throwable cause, String format, Object... args)
    {
        this(cause, String.format(format, args));
    }

    @Override
    @ThriftField(1)
    public String getMessage()
    {
        Throwable cause = getCause();

        if (cause != null) {
            return super.getMessage() + ": " + cause.getMessage();
        }

        return super.getMessage();
    }
}
