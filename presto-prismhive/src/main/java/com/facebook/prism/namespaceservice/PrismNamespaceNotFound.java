package com.facebook.prism.namespaceservice;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.concurrent.Immutable;

@Immutable
@ThriftStruct
public class PrismNamespaceNotFound
        extends Exception
{
    private final String message;

    @ThriftConstructor
    public PrismNamespaceNotFound(String message)
    {
        this.message = message;
    }

    @ThriftField(1)
    public String getMessage()
    {
        return message;
    }
}
