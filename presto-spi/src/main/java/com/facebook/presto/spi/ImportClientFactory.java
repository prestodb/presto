/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.spi;

public interface ImportClientFactory
{
    boolean hasCatalog(String catalogName);

    /**
     * Creates an import client for the specified catalogName, if the catalog is supported; null otherwise
     */
    ImportClient createClient(String catalogName);
}
