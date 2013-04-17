package com.facebook.presto.storage;

import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class MockStorageManager
        implements StorageManager
{
    private final ConcurrentMap<NativeTableHandle, QualifiedTableName> tables = new ConcurrentHashMap<>();

    @Override
    public void insertSourceTable(NativeTableHandle tableHandle, QualifiedTableName sourceTableName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(sourceTableName, "sourceTableName is null");

        tables.putIfAbsent(tableHandle, sourceTableName);
    }

    @Override
    public QualifiedTableName getSourceTable(NativeTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        return tables.get(tableHandle);
    }

    @Override
    public void dropSourceTable(NativeTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        tables.remove(tableHandle);
    }
}
