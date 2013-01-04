package com.facebook.presto.sql.analyzer;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class Session
{
    public static final String DEFAULT_CATALOG = "default";
    public static final String DEFAULT_SCHEMA = "default";

    private final String user;
    private final String catalog;
    private final String schema;

    public Session(@Nullable String user, String catalog, String schema)
    {
        this.user = user;
        this.catalog = checkNotNull(catalog, "catalog is null");
        this.schema = checkNotNull(schema, "schema is null");
    }

    @Nullable
    public String getUser()
    {
        return user;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }
}
