package com.facebook.prism.namespaceservice;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

import javax.annotation.concurrent.Immutable;

@Immutable
@ThriftStruct
public class PrismNamespace
{
    private final String name;
    private final String regionName;
    private final String hiveMetastore;
    private final String hiveDatabaseName;

    @ThriftConstructor
    public PrismNamespace(String name, String regionName, String hiveMetastore, String hiveDatabaseName)
    {
        this.name = name;
        this.regionName = regionName;
        this.hiveMetastore = hiveMetastore;
        this.hiveDatabaseName = hiveDatabaseName;
    }

    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @ThriftField(3)
    public String getRegionName()
    {
        return regionName;
    }

    @ThriftField(5)
    public String getHiveMetastore()
    {
        return hiveMetastore;
    }

    @ThriftField(33)
    public String getHiveDatabaseName()
    {
        return hiveDatabaseName;
    }
}
