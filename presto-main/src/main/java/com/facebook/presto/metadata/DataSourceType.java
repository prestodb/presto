package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public enum DataSourceType
{
    TPCH("tpch"), // only used in tests.
    NATIVE("native"),
    INTERNAL("internal"),
    IMPORT("import"),
    REMOTE("remote"),
    WRITING("writing");

    private static final Map<String, DataSourceType> NAME_MAP;
    static {
        ImmutableMap.Builder<String, DataSourceType> builder = ImmutableMap.builder();
        for (DataSourceType dataSourceType : DataSourceType.values()) {
            builder.put(dataSourceType.getName(), dataSourceType);
        }
        NAME_MAP = builder.build();
    }

    private final String name;

    private DataSourceType(String name)
    {
        this.name = checkNotNull(name, "name is null");
    }

    @JsonValue
    public String getName()
    {
        return name;
    }

    @JsonCreator
    public static DataSourceType fromName(String name)
    {
        checkNotNull(name, "name is null");
        DataSourceType dataSourceType = NAME_MAP.get(name);
        checkArgument(dataSourceType != null, "Invalid dataSourceType name: %s", name);
        return dataSourceType;
    }
}
