/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import javax.ws.rs.core.MediaType;

public final class PrestoMediaTypes
{
    public static final String PRESTO_BLOCK = "application/X-presto-block";
    public static final MediaType PRESTO_BLOCK_TYPE = new MediaType("application", "X-presto-block");

    public static final String PRESTO_BLOCKS = "application/X-presto-blocks";
    public static final MediaType PRESTO_BLOCKS_TYPE = new MediaType("application", "X-presto-blocks");


    private PrestoMediaTypes()
    {
    }
}
