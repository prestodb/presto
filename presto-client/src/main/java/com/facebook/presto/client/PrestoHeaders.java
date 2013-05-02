/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.client;

public final class PrestoHeaders
{
    public static final String PRESTO_USER = "X-Presto-User";
    public static final String PRESTO_CATALOG = "X-Presto-Catalog";
    public static final String PRESTO_SCHEMA = "X-Presto-Schema";

    public static final String PRESTO_CURRENT_STATE = "X-Presto-Current-State";
    public static final String PRESTO_MAX_WAIT = "X-Presto-Max-Wait";

    public PrestoHeaders()
    {
    }
}
