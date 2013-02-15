package com.facebook.prism.namespaceservice;

import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

import java.io.Closeable;
import java.util.List;

@ThriftService("prism")
public interface PrismServiceClient
        extends Closeable
{
    @ThriftMethod
    List<String> getAllNamespaceNames()
            throws PrismRepositoryError;

    @ThriftMethod(exception = {@ThriftException(type = PrismNamespaceNotFound.class, id = 1), @ThriftException(type = PrismRepositoryError.class, id = 2)})
    PrismNamespace getNamespace(String name)
            throws PrismNamespaceNotFound, PrismRepositoryError;

    @Override
    public void close();
}
