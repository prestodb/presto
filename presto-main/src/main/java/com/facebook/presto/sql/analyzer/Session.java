package com.facebook.presto.sql.analyzer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class Session
{
    public static final String DEFAULT_CATALOG = "default";
    public static final String DEFAULT_SCHEMA = "default";

    private final String user;
    private final String catalog;
    private final String schema;
    private final long startTime;

    public Session(@Nullable String user, String catalog, String schema)
    {
        this(user, catalog, schema, System.currentTimeMillis());
    }

    @JsonCreator
    public Session(
            @JsonProperty("user") @Nullable String user,
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("startTime") long startTime)
    {
        this.user = user;
        this.catalog = checkNotNull(catalog, "catalog is null");
        this.schema = checkNotNull(schema, "schema is null");
        this.startTime = startTime;
    }

    @JsonProperty
    @Nullable
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public long getStartTime()
    {
        return startTime;
    }
}
