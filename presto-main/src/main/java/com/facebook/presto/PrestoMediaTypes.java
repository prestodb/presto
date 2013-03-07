/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.google.common.net.MediaType;

public final class PrestoMediaTypes
{
    public static final String PRESTO_PAGES = "application/X-presto-pages";
    public static final MediaType PRESTO_PAGES_TYPE = MediaType.create("application", "X-presto-pages");


    private PrestoMediaTypes()
    {
    }
}
