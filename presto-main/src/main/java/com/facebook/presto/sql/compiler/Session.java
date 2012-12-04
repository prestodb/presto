package com.facebook.presto.sql.compiler;

import static com.google.common.base.Preconditions.checkNotNull;

public class Session
{
    public static final String DEFAULT_CATALOG = "default";
    public static final String DEFAULT_SCHEMA = "default";

    private String currentCatalog;
    private String currentSchema;

    public Session(String currentCatalog, String currentSchema)
    {
        this.currentCatalog = checkNotNull(currentCatalog, "currentCatalog is null");
        this.currentSchema = checkNotNull(currentSchema, "currentSchema is null");
    }

    public Session()
    {
        this(DEFAULT_CATALOG, DEFAULT_SCHEMA);
    }

    public void using(String catalogName, String schemaName)
    {
        currentCatalog = catalogName;
        currentSchema = schemaName;
    }

    public String getCurrentCatalog()
    {
        return currentCatalog;
    }

    public String getCurrentSchema()
    {
        return currentSchema;
    }
}
