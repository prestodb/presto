/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

@SuppressWarnings("UnusedDeclaration")
public class ClassLoaderSafeImportClient
        implements ImportClient
{
    private final ImportClient delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeImportClient(ImportClient delegate, ClassLoader classLoader)
    {
        Preconditions.checkNotNull(delegate, "delegate is null");
        Preconditions.checkNotNull(classLoader, "classLoader is null");
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public List<String> listSchemaNames()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.listSchemaNames();
        }
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTables(schemaNameOrNull);
        }
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableHandle(tableName);
        }
    }

    @Override
    public SchemaTableMetadata getTableMetadata(TableHandle table)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableMetadata(table);
        }
    }

    @Override
    public SchemaTableName getTableName(TableHandle tableHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableName(tableHandle);
        }
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnHandle(tableHandle, columnName);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnHandles(tableHandle);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnMetadata(tableHandle, columnHandle);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTableColumns(prefix);
        }
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(SchemaTablePrefix prefix)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.listTablePartitionValues(prefix);
        }
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitions(table, bindings);
        }
    }

    @Override
    public Iterable<PartitionChunk> getPartitionChunks(List<Partition> partitions, List<ColumnHandle> columns)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitionChunks(partitions, columns);
        }
    }

    @Override
    public RecordCursor getRecords(PartitionChunk partitionChunk)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getRecords(partitionChunk);
        }
    }

    @Override
    public byte[] serializePartitionChunk(PartitionChunk partitionChunk)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.serializePartitionChunk(partitionChunk);
        }
    }

    @Override
    public PartitionChunk deserializePartitionChunk(byte[] bytes)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.deserializePartitionChunk(bytes);
        }
    }
}
