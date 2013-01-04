/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

public final class PrestoHeaders
{
    public static final String PRESTO_USER = "X-Presto-User";
    public static final String PRESTO_CATALOG = "X-Presto-Catalog";
    public static final String PRESTO_SCHEMA = "X-Presto-Schema";

    private PrestoHeaders()
    {
    }
}
