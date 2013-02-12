package com.facebook.prism.namespaceservice;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.concurrent.Immutable;

@Immutable
@ThriftStruct
public class PrismRepositoryError
        extends Exception
{
    private final String message;
    private final PrismErrorCode code;

    @ThriftConstructor
    public PrismRepositoryError(String message, PrismErrorCode code)
    {
        this.message = message;
        this.code = code;
    }

    @ThriftField(1)
    public String getMessage()
    {
        return message;
    }

    @ThriftField(2)
    public PrismErrorCode getCode()
    {
        return code;
    }
}
