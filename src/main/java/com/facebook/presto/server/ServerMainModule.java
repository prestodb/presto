/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class ServerMainModule implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(StaticQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(UncompressedBlockMapper.class).in(Scopes.SINGLETON);
        binder.bind(UncompressedBlocksMapper.class).in(Scopes.SINGLETON);
    }
}
