/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.spi;

public interface ImportClientFactory
{
    /**
     * Creates an import client for the specified sourceName, if the source is supported; null otherwise
     */
    ImportClient createClient(String sourceName);
}
