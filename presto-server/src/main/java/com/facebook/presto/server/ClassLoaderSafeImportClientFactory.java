/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ImportClientFactory;
import com.google.common.base.Preconditions;

@SuppressWarnings("UnusedDeclaration")
public class ClassLoaderSafeImportClientFactory
        implements ImportClientFactory
{
    private final ImportClientFactory delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeImportClientFactory(ImportClientFactory delegate, ClassLoader classLoader)
    {
        Preconditions.checkNotNull(delegate, "delegate is null");
        Preconditions.checkNotNull(classLoader, "classLoader is null");
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public boolean hasCatalog(String catalogName)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.hasCatalog(catalogName);
        }
    }

    @Override
    public ImportClient createClient(String catalogName)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            ImportClient client = delegate.createClient(catalogName);
            if (client == null) {
                return null;
            }
            return new ClassLoaderSafeImportClient(client, classLoader);
        }
    }
}
