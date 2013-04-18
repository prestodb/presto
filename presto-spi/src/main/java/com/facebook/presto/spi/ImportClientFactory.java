/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.spi;

public interface ImportClientFactory
{
    ImportClient createClient(String clientId);
}
